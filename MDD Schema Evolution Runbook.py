# Databricks notebook source
# DBTITLE 1,Schema Evolution Runbook — docs
# MAGIC %md
# MAGIC # MDD Schema Evolution Runbook
# MAGIC
# MAGIC One on-demand, widget-driven schema-change job. **The YAML in the config volume is the single source of truth** — this runbook edits YAML first, syncs table schemas to it, then re-runs the config loader. The regular hourly pipeline needs no restart: it reads configs fresh each run.
# MAGIC
# MAGIC **Actions — strict execute / review / route policy**
# MAGIC
# MAGIC * **REVIEW** — diff YAML vs actual schemas (bronze/quarantine/silver/SCD2-history) for one or all sources. Zero risk. Use as the UAT promotion gate (expect "no drift").
# MAGIC * **ADD** — golden path, fully automated + idempotent: YAML update → ALTER every existing target → config loader (validation + PII mask/tag catchup) → verification
# MAGIC * **ADD_RULES** — appends data-quality rules (not_null / regex / range) to a source's YAML `quality_rules`: append-only (never removes or modifies existing rules), idempotent, enforced on rows from the next pipeline run
# MAGIC * **ALTER_WIDEN** — guarded: varchar/decimal widening + safe upcasts only; validates existing data fits; narrowing/cross-type → refused, routed to rebuild
# MAGIC * **RENAME** — review-first: requires explicit renames mapping; refuses if transformation exprs reference the column; needs column mapping enabled or routes to rebuild
# MAGIC * **DROP** — report-and-route only: impact report + exact rebuild command. Never executes.
# MAGIC
# MAGIC **column_spec examples**
# MAGIC
# MAGIC * ADD: `{"name": "department", "type": "string", "length": 80, "comment": "Employee department"}` — optional `"pii_type": "name|email|phone|street_address|financial"`, `"excluded": true` (bronze/quarantine only)
# MAGIC * ADD_RULES: `{"rules": [{"name": "manager_not_null", "rule": "not_null", "column": "manager"}]}` — rule = `not_null` | `regex` (+`pattern`) | `range` (+`min`,`max`); column must be a YAML config column or a silver transformation target
# MAGIC * ALTER_WIDEN: `{"name": "full_name", "new_type": "VARCHAR(200)"}` or `{"name": "project_cost", "new_type": "DECIMAL(14,2)"}`
# MAGIC * RENAME: `{"renames": {"old_col": "new_col"}}`
# MAGIC * DROP: `{"name": "old_col"}` (empty spec + no source_name = report orphaned columns everywhere)
# MAGIC
# MAGIC **Safety rails**
# MAGIC
# MAGIC * `dry_run=true` (default) prints the full plan and changes nothing; `confirm=true` required to execute.
# MAGIC * Business/merge keys are untouchable; names must be valid identifiers; collisions with transformation targets rejected.
# MAGIC * Executing actions abort if the hourly pipeline ("MDD Pipeline Orchestration Job") is mid-run.
# MAGIC * Idempotent: re-running with the same spec is a no-op.
# MAGIC * Rebuild routing for unsafe changes: "MDD On-Demand Runbook Job" with `source_name=<src>, mode=rebuild, confirm=true` (audit logs survive; raw files must remain in landing).
# MAGIC
# MAGIC **Dev → UAT → Prod**: jobs are discovered BY NAME (never hardcoded IDs); widget defaults are env-neutral; ship this notebook + its job + final YAMLs together in the bundle. After ADD, existing rows keep NULL until the source re-sends values (or a rebuild backfills). DQ rules for a new column are added by editing YAML `quality_rules` + re-running the config loader.

# COMMAND ----------

# DBTITLE 1,Widgets
# Widgets — interactive use, or via the "MDD Schema Evolution Job" (task parameters map 1:1 to widgets).
try:
    dbutils.widgets.text("source_name", "", "source_name (REVIEW: empty = all sources)")
    dbutils.widgets.dropdown("action", "REVIEW", ["REVIEW", "ADD", "ADD_RULES", "ALTER_WIDEN", "RENAME", "DROP"], "action")
    dbutils.widgets.text("column_spec", "", "column_spec (JSON — see header)")
    dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "dry_run")
    dbutils.widgets.dropdown("confirm", "false", ["false", "true"], "confirm")
    WIDGETS_OK = True
except Exception:
    WIDGETS_OK = False
    print("widget context unavailable (chat/verification run) — defaults/overrides apply")

# COMMAND ----------

# DBTITLE 1,Helpers + params
import yaml, json, re, time
from databricks.sdk import WorkspaceClient

# Catalog: bundle variable (schema-evolution job passes catalog_name; default mdd_platform)
dbutils.widgets.text("catalog_name", "mdd_platform", "MDD catalog (bundle passes ${var.catalog_name})")
CAT = dbutils.widgets.get("catalog_name").strip() or "mdd_platform"
CFG_VOL = f"/Volumes/{CAT}/config/config_files"
LOADER_NB = "/MDD_DataPlatform/MDD Config Loader"
MASK_FN_TYPES = {"name", "email", "phone", "street_address", "financial"}  # mirrors config loader MASK_FN
TECH_COLS = {"_rescued_data", "_source_file", "_file_mtime", "_ingest_ts", "_batch_id",
             "_row_hash", "_ingest_date", "_merged_ts", "_failed_rules", "_quarantine_ts",
             "_effective_from", "_effective_to", "_is_current"}

OVERRIDE = {}  # chat/verification runs: e.g. {"source_name": "src_009_employee_billing", "action": "ADD", ...}

def P(name, default):
    if name in OVERRIDE:
        return OVERRIDE[name]
    try:
        v = dbutils.widgets.get(name)
        return v if v not in (None, "") else default
    except Exception:
        return default

SRC = P("source_name", "").strip()
ACTION = P("action", "REVIEW").upper()
DRY = str(P("dry_run", "true")).lower() == "true"
CONFIRM = str(P("confirm", "false")).lower() == "true"
try:
    SPEC = json.loads(P("column_spec", "") or "{}")
except json.JSONDecodeError as e:
    raise ValueError(f"column_spec is not valid JSON: {e}")
print(f"action={ACTION}  source_name={SRC or '(all)'}  dry_run={DRY}  confirm={CONFIRM}")
if not DRY and not CONFIRM:
    raise ValueError("Refusing to execute: dry_run=false requires confirm=true (review the dry-run plan first).")

# ---- framework-mirroring helpers (single source of truth = YAML files) ----------------
def col_sql_type(c):
    t = (c.get("type") or "STRING").upper()
    return f"VARCHAR({int(c['length'])})" if (t == "STRING" and c.get("length")) else t

def active_sources():
    return sorted(f.name[:-5] for f in dbutils.fs.ls(CFG_VOL) if f.name.endswith(".yaml"))

def load_cfg(src):
    return yaml.safe_load(dbutils.fs.head(f"{CFG_VOL}/{src}.yaml"))

def layer_tables(cfg):
    """Existing framework target tables for this source (same conventions as the framework/loader)."""
    b, s, q = cfg.get("bronze") or {}, cfg.get("silver") or {}, cfg.get("quarantine") or {}
    strategy = (s.get("merge_strategy") or cfg.get("merge_strategy") or "scd1").lower()
    silver_t = s.get("target_table", f"{CAT}.silver.{cfg['source_name']}")
    cand = {"bronze": b.get("target_table", f"{CAT}.bronze.{cfg['source_name']}"),
            "quarantine": q.get("target_table", f"{CAT}.quarantine.{cfg['source_name']}"),
            "silver": silver_t}
    if strategy in ("scd2", "scd1+scd2"):
        cand["history"] = (s.get("scd2") or {}).get("target_table") or f"{silver_t}_history"
    return {l: t for l, t in cand.items() if spark.catalog.tableExists(t)}

def excluded_cols(cfg):
    return set((cfg.get("silver") or {}).get("exclude_columns") or [])

def transform_targets(cfg):
    return {t["column"]: (t.get("type") or "STRING").upper() for t in ((cfg.get("silver") or {}).get("transformations") or [])}

def expected_cols(cfg, layer):
    """Business columns the layer should carry per YAML (tech columns excluded).
    Framework convention: bronze + quarantine store business columns as raw STRING
    (schema-on-read); types are enforced only in silver/SCD2-history via col_cast."""
    if layer in ("bronze", "quarantine"):
        return {c["name"].lower(): "STRING" for c in cfg["columns"]}
    ex = excluded_cols(cfg)
    exp = {c["name"].lower(): col_sql_type(c) for c in cfg["columns"] if c["name"] not in ex}
    exp.update({k.lower(): v for k, v in transform_targets(cfg).items()})
    return exp

def actual_cols(table):
    """True column types via DESCRIBE — information_schema reports VARCHAR as STRING/length 0
    in this workspace, so it cannot be trusted for type diffs."""
    out = {}
    for r in spark.sql(f"DESCRIBE TABLE {table}").collect():
        n, dt = r["col_name"], r["data_type"]
        if n.startswith("#"):
            break  # partition/clustering metadata sections
        if not n or dt is None:
            continue
        out[n.lower()] = str(dt).upper()
    return out

def widen_verdict(old, new):
    """WIDEN_OK = safe in-place ALTER; REBUILD = route to rebuild; UNCHANGED = no-op."""
    o, n = old.upper(), new.upper()
    if o == n:
        return "UNCHANGED"
    vm, vn = re.fullmatch(r"VARCHAR\((\d+)\)", o), re.fullmatch(r"VARCHAR\((\d+)\)", n)
    if vm and vn and int(vn[1]) > int(vm[1]):
        return "WIDEN_OK"
    if vm and n == "STRING":
        return "WIDEN_OK"
    dm, dn = re.fullmatch(r"DECIMAL\((\d+),(\d+)\)", o), re.fullmatch(r"DECIMAL\((\d+),(\d+)\)", n)
    if dm and dn and int(dn[1]) > int(dm[1]) and int(dn[2]) == int(dm[2]):
        return "WIDEN_OK"
    if (o, n) in (("INT", "BIGINT"), ("FLOAT", "DOUBLE")):
        return "WIDEN_OK"
    return "REBUILD"

def find_job_id(name_sub):
    for j in WorkspaceClient().jobs.list():
        if name_sub.lower() in (j.settings.name or "").lower():
            return j.job_id
    return None

def hourly_pipeline_busy():
    """Guard: never ALTER while the hourly pipeline is mid-run."""
    jid = find_job_id("MDD Pipeline Orchestration")
    if not jid:
        print("WARN: hourly pipeline job not found by name — proceeding without idle check")
        return None
    for r in WorkspaceClient().jobs.list_runs(job_id=jid, active_only=True):
        return f"MDD Pipeline Orchestration Job run {r.run_id} ({getattr(r.state.life_cycle_state, 'name', r.state.life_cycle_state)})"
    return None

def run_config_loader():
    """Re-validate updated YAML, MERGE control table, apply PII masks/tags (catchup)."""
    try:
        dbutils.notebook.run(LOADER_NB, 1200, {"catalog_name": CAT})
        return "config loader completed (notebook.run)"
    except Exception as e:
        jid = find_job_id("MDD Config Loader Job")
        if not jid:
            raise RuntimeError(f"config loader unreachable: {str(e)[:200]}")
        w = WorkspaceClient()
        run = w.jobs.run_now(jid)
        print(f"config loader via job run {run.run_id}")
        for _ in range(90):
            time.sleep(10)
            rr = w.jobs.get_run(run.run_id)
            st = getattr(rr.state.life_cycle_state, "name", str(rr.state.life_cycle_state))
            if st not in ("PENDING", "RUNNING", "QUEUED", "BLOCKED"):
                break
        res = getattr(rr.state, "result_state", None)
        res = getattr(res, "name", str(res)) if res is not None else "?"
        if res != "SUCCESS":
            raise RuntimeError(f"config loader job failed: {res}")
        return f"config loader completed (job run {run.run_id})"

def write_cfg(src, cfg):
    dbutils.fs.put(f"{CFG_VOL}/{src}.yaml", yaml.safe_dump(cfg, sort_keys=False), overwrite=True)

def check_source(src):
    if src not in active_sources():
        raise ValueError(f"unknown source '{src}'. active: {active_sources()}")
    return load_cfg(src)

# COMMAND ----------

# DBTITLE 1,REVIEW — drift report
# REVIEW — zero-risk diff: YAML (source of truth) vs actual table schemas.
if ACTION == "REVIEW":
    targets = active_sources() if not SRC else [SRC]
    if SRC:
        check_source(SRC)
    drift = 0
    for src in targets:
        cfg = check_source(src)
        for layer, tbl in sorted(layer_tables(cfg).items()):
            exp, act = expected_cols(cfg, layer), actual_cols(tbl)
            missing = {k: v for k, v in exp.items() if k not in act}
            extra = {k: v for k, v in act.items() if k not in exp and k not in TECH_COLS}
            changed = {k: (act[k], exp[k]) for k in set(exp) & set(act) if act[k] != exp[k]}
            if missing or extra or changed:
                drift += 1
                print(f"\n### {src} / {layer}  ({tbl})")
                if missing:
                    print("  missing (YAML has, table lacks)  -> ADD:", missing)
                if extra:
                    print("  orphaned (table has, YAML lacks) -> DROP/rebuild:", extra)
                for k, (a, e) in changed.items():
                    print(f"  type drift '{k}': table={a} yaml={e} -> {widen_verdict(a, e)}")
    print(f"\nREVIEW complete: {len(targets)} source(s), {drift} layer(s) with drift"
          + (" — all clean." if drift == 0 else ""))
    if drift == 0:
        print("No action needed. For changes: ADD / ALTER_WIDEN / RENAME with dry_run=true shows the exact plan.")

# COMMAND ----------

# DBTITLE 1,ADD — golden path
# ADD — golden path: YAML update -> ALTER every existing target -> config loader -> verify. Idempotent.
if ACTION == "ADD":
    if not SRC:
        raise ValueError("ADD requires source_name")
    cfg = check_source(SRC)
    spec = {"name": None, "type": "string", "length": None, "comment": None,
            "pii_type": None, "excluded": False}
    spec.update(SPEC)
    name = str(spec["name"] or "")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"invalid column name {name!r}")
    if name.lower() in TECH_COLS:
        raise ValueError(f"{name} collides with a framework tech column")
    if name.lower() in {k.lower() for k in transform_targets(cfg)}:
        raise ValueError(f"{name} collides with a transformation target")
    key_cols = set(cfg.get("business_keys") or []) | set((cfg.get("silver") or {}).get("merge_keys") or [])
    if name in key_cols:
        raise ValueError("refusing: column collides with a business/merge key name")
    if spec.get("pii_type") and spec["pii_type"] not in MASK_FN_TYPES:
        raise ValueError(f"pii_type must be one of {sorted(MASK_FN_TYPES)}")
    existing_yaml = {c["name"].lower() for c in cfg["columns"]}
    if name.lower() in existing_yaml:
        print(f"idempotent: '{name}' already in YAML for {SRC} — syncing table drift only")

    col_entry = {"name": name, "type": str(spec["type"] or "string").lower()}
    if spec.get("length"):
        col_entry["length"] = int(spec["length"])
    if spec.get("comment"):
        col_entry["comment"] = spec["comment"]
    sql_t = col_sql_type(col_entry)
    excluded = bool(spec.get("excluded"))

    plan, skipped = [], []
    for layer, tbl in sorted(layer_tables(cfg).items()):
        if layer in ("silver", "history") and excluded:
            skipped.append(f"{layer} (excluded from silver)")
            continue
        if name.lower() in actual_cols(tbl):
            skipped.append(f"{layer} (already has column)")
            continue
        cmt = (" COMMENT '" + str(spec["comment"]).replace("'", "''") + "'") if spec.get("comment") else ""
        # framework convention: bronze/quarantine land raw STRING; types enforced in silver/history
        layer_t = "STRING" if layer in ("bronze", "quarantine") else sql_t
        plan.append((layer, tbl, f"ALTER TABLE {tbl} ADD COLUMN `{name}` {layer_t}{cmt}"))

    print(f"ADD plan for {SRC} → column '{name}' {sql_t}" + ("  [excluded from silver]" if excluded else ""))
    for layer, tbl, stmt in plan:
        print(f"  [{layer:10s}] {stmt}")
    for s in skipped:
        print(f"  [skip      ] {s}")
    note = f"YAML: append column entry {col_entry}"
    if spec.get("pii_type"):
        note += f"; pii_columns['{name}'] = '{spec['pii_type']}' (masks/tags applied by loader)"
    if excluded:
        note += "; silver.exclude_columns += name"
    print("  " + note)
    print("  then: config loader (validate + control-table MERGE + PII mask/tag catchup)")

    if DRY:
        print("\nDRY RUN — nothing changed. Re-run with dry_run=false & confirm=true to execute.")
    else:
        busy = hourly_pipeline_busy()
        if busy:
            raise RuntimeError(f"ABORT — hourly pipeline mid-run: {busy}. Re-run when idle (hourly at :00, done ~:15).")
        if name.lower() not in existing_yaml:
            cfg["columns"].append(col_entry)
        if spec.get("pii_type"):
            cfg.setdefault("pii_columns", {})[name] = spec["pii_type"]
        if excluded:
            (cfg.get("silver") or {}).setdefault("exclude_columns", []).append(name)
        write_cfg(SRC, cfg)
        print("YAML updated.")
        for layer, tbl, stmt in plan:
            spark.sql(stmt)
            print(f"  executed [{layer}] {stmt}")
        print(run_config_loader())
        ok = all(name.lower() in actual_cols(t) for _, t, _ in plan) if plan else True
        tagged = True
        silver_tbl = layer_tables(cfg).get("silver")
        if spec.get("pii_type") and silver_tbl and not excluded:
            sch, tbl = silver_tbl.split(".")[1], silver_tbl.split(".")[2]
            tagged = spark.sql(f"""SELECT count(*) FROM {CAT}.information_schema.column_tags
                WHERE table_schema='{sch}' AND table_name='{tbl}' AND column_name='{name}'""").collect()[0][0] > 0
        print(f"VERIFY: column present in all planned tables: {ok}; PII tagged: {tagged if spec.get('pii_type') else 'n/a'}")
        if not (ok and tagged):
            raise RuntimeError("verification failed — inspect the tables above")
        print(f"\nDONE. '{name}' live in bronze/quarantine/silver for {SRC}.")
        print("Note: existing rows keep NULL until re-sent; add quality_rules in YAML if needed + re-run loader.")

# COMMAND ----------

# DBTITLE 1,ADD_RULES — append quality rules
# ADD_RULES — append data-quality rules to a source's YAML quality_rules. Append-only:
# never removes or modifies existing rules. Idempotent: already-present rule names skip silently.
if ACTION == "ADD_RULES":
    if not SRC:
        raise ValueError("ADD_RULES requires source_name")
    rules_in = SPEC.get("rules") or []
    if not isinstance(rules_in, list) or not rules_in:
        raise ValueError("ADD_RULES requires spec {'rules': [{'name','rule','column', ...}]}")
    cfg = check_source(SRC)
    # rules evaluate on the post-dedup, post-transformation df: config columns AND transformation targets are legal
    valid_cols = {c["name"].lower() for c in cfg["columns"]} | {k.lower() for k in transform_targets(cfg)}
    existing = {r.get("name") for r in cfg.get("quality_rules") or []}
    plan, skipped, seen_new = [], [], set()
    for r in rules_in:
        name, rule, col = r.get("name"), str(r.get("rule") or "").lower(), r.get("column")
        if not name or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(name)):
            raise ValueError(f"invalid rule name {name!r}")
        if str(name) in existing:
            skipped.append(f"{name} (already present — idempotent skip)")
            continue
        if str(name) in seen_new:
            raise ValueError(f"duplicate rule name in spec: {name}")
        if rule not in ("not_null", "regex", "range"):
            raise ValueError(f"rule '{name}': rule must be not_null | regex | range (got {rule!r})")
        if not col or str(col).lower() not in valid_cols:
            raise ValueError(f"rule '{name}': unknown column {col!r} (must be a config column or a silver transformation target)")
        if rule == "regex" and not r.get("pattern"):
            raise ValueError(f"rule '{name}': regex rules require 'pattern'")
        if rule == "range" and (r.get("min") is None or r.get("max") is None):
            raise ValueError(f"rule '{name}': range rules require 'min' and 'max'")
        entry = {"name": str(name), "rule": rule, "column": str(col)}
        if rule == "regex":
            entry["pattern"] = str(r["pattern"])
        if rule == "range":
            entry["min"], entry["max"] = r["min"], r["max"]
        plan.append(entry)
        seen_new.add(str(name))

    print(f"ADD_RULES plan for {SRC}: append {len(plan)} rule(s), skip {len(skipped)}")
    for e in plan:
        print(f"  + {e}")
    for s in skipped:
        print("  skip:", s)
    print("  then: config loader (validate + control-table MERGE) — rules enforced from the next pipeline run")

    if DRY:
        print("\nDRY RUN — nothing changed. Re-run with dry_run=false & confirm=true to execute.")
    elif plan:
        busy = hourly_pipeline_busy()
        if busy:
            raise RuntimeError(f"ABORT — hourly pipeline mid-run: {busy}. Re-run when idle.")
        cfg.setdefault("quality_rules", []).extend(plan)  # append-only — existing rules untouched
        write_cfg(SRC, cfg)
        print("YAML updated.")
        print(run_config_loader())
        stored = json.loads(spark.sql(
            f"SELECT quality_rules FROM {CAT}.config.pipeline_configs WHERE source_name='{SRC}'"
        ).collect()[0]["quality_rules"])
        stored_names = {r.get("name") for r in stored}
        yaml_now = {r.get("name") for r in load_cfg(SRC).get("quality_rules") or []}
        ok_ctl = all(e["name"] in stored_names for e in plan)
        ok_yaml = all(e["name"] in yaml_now for e in plan)
        print(f"VERIFY: control table has new rules: {ok_ctl}; YAML has new rules: {ok_yaml}")
        if not (ok_ctl and ok_yaml):
            raise RuntimeError("verification failed — new rules missing from YAML/control table")
        print(f"\nDONE. {len(plan)} rule(s) live for {SRC}.")
        print("Note: rules evaluate on new incremental rows only; existing rows are not retroactively quarantined.")
    else:
        print("Nothing to append (all requested rules already present).")

# COMMAND ----------

# DBTITLE 1,ALTER_WIDEN — guarded widening
# ALTER_WIDEN — guarded widening only (varchar/decimal widen + safe upcasts). Narrow/cross-type -> routed to rebuild.
if ACTION == "ALTER_WIDEN":
    if not SRC or not SPEC.get("name") or not SPEC.get("new_type"):
        raise ValueError("ALTER_WIDEN requires source_name + spec {'name','new_type'}")
    cfg = check_source(SRC)
    name = str(SPEC["name"])
    new_sql = str(SPEC["new_type"]).upper().strip()
    if name.lower() in {k.lower() for k in transform_targets(cfg)}:
        raise ValueError(f"'{name}' is transformation-derived: widen it by editing its transformation type in YAML + re-run loader (silver/history only), or rebuild.")
    col = next((c for c in cfg["columns"] if c["name"].lower() == name.lower()), None)
    if not col:
        raise ValueError(f"'{name}' is not a config column of {SRC}")
    old_sql = col_sql_type(col)
    verdict = widen_verdict(old_sql, new_sql)
    print(f"ALTER_WIDEN {SRC}.{name}: {old_sql} -> {new_sql}  verdict={verdict}")
    if verdict == "UNCHANGED":
        print("Nothing to do.")
    elif verdict == "REBUILD":
        print("\nRefused: narrowing/cross-type changes are routed to rebuild (lossless from raw files).")
        print(f"  1) update the YAML type for '{name}'")
        print(f"  2) rebuild via 'MDD On-Demand Runbook Job': source_name={SRC}, mode=rebuild, confirm=true")
    else:
        # data-fits guard (decimal widening) + per-table plan — widening applies to the TYPED layers
        # (silver/history); bronze/quarantine land raw STRING by design, so their YAML type is
        # informational only (silver col_cast) and there is nothing to ALTER there.
        plan, notes = [], []
        if new_sql.startswith("DECIMAL"):
            dn = re.fullmatch(r"DECIMAL\((\d+),(\d+)\)", new_sql)
            p2, s2 = int(dn[1]), int(dn[2])
            for layer, tbl in sorted(layer_tables(cfg).items()):
                if layer not in ("silver", "history"):
                    continue
                if layer == "history" and name in excluded_cols(cfg):
                    continue
                if name.lower() not in actual_cols(tbl):
                    continue
                n_bad = spark.sql(f"""SELECT count(*) FROM {tbl}
                    WHERE abs(cast(`{name}` as DOUBLE)) >= power(10.0, {p2 - s2})""").collect()[0][0]
                notes.append(f"{layer}: {n_bad} row(s) exceed DECIMAL({p2},{s2}) range" if n_bad else f"{layer}: data fits")
        for layer, tbl in sorted(layer_tables(cfg).items()):
            if layer not in ("silver", "history"):
                print(f"  [skip] {layer}: raw STRING layer — type not enforced (nothing to ALTER)")
                continue
            act = actual_cols(tbl)
            if name.lower() not in act:
                print(f"  [skip] {layer}: column not present")
                continue
            cur = act[name.lower()]
            if cur == new_sql:
                print(f"  [skip] {layer}: already {new_sql}")
                continue
            if widen_verdict(cur, new_sql) != "WIDEN_OK":
                raise RuntimeError(f"ABORT — {layer} table type {cur} cannot widen to {new_sql}: route to rebuild")
            plan.append((layer, tbl, f"ALTER TABLE {tbl} ALTER COLUMN `{name}` TYPE {new_sql}"))
        for n in notes:
            print("  [data     ]", n)
        if any("exceed" in n for n in notes):
            raise RuntimeError("ABORT — existing data does not fit the new type; route to rebuild (values would overflow).")
        for layer, tbl, stmt in plan:
            print(f"  [{layer:10s}] {stmt}")
        print(f"  YAML: '{name}' type -> {new_sql}")
        if DRY:
            print("\nDRY RUN — nothing changed. Re-run with dry_run=false & confirm=true to execute.")
        elif plan:
            busy = hourly_pipeline_busy()
            if busy:
                raise RuntimeError(f"ABORT — hourly pipeline mid-run: {busy}. Re-run when idle.")
            m = re.fullmatch(r"VARCHAR\((\d+)\)", new_sql)
            if m:
                col["type"], col["length"] = "string", int(m[1])
            elif new_sql == "STRING":
                col["type"] = "string"
                col.pop("length", None)
            else:
                col["type"] = new_sql.lower()
                col.pop("length", None)
            write_cfg(SRC, cfg)
            print("YAML updated.")
            for layer, tbl, stmt in plan:
                spark.sql(stmt)
                print(f"  executed [{layer}] {stmt}")
            print(run_config_loader())
            ok = all(actual_cols(t).get(name.lower()) == new_sql for _, t, _ in plan)
            print(f"VERIFY: widened everywhere: {ok}")
            if not ok:
                raise RuntimeError("verification failed — inspect the tables above")
            print("DONE.")
        else:
            print("Nothing to execute (all targets already widened).")

# COMMAND ----------

# DBTITLE 1,RENAME — review-first
# RENAME — review-first: explicit mapping only; refuses when transformation exprs reference the column.
# Needs Delta column mapping (mode=name) on each target, else routes to rebuild. Note: a rename changes
# the _row_hash inputs — re-touched keys re-update on the next incremental run (expected, harmless).
if ACTION == "RENAME":
    renames = {str(k): str(v) for k, v in (SPEC.get("renames") or {}).items()}
    if not renames:
        raise ValueError("RENAME requires spec {'renames': {'old':'new'}}")
    if not SRC:
        raise ValueError("RENAME requires source_name")
    cfg = check_source(SRC)
    key_cols = set(cfg.get("business_keys") or []) | set((cfg.get("silver") or {}).get("merge_keys") or [])
    tf_exprs = {t["column"]: t.get("expr", "") for t in ((cfg.get("silver") or {}).get("transformations") or [])}
    yaml_changes = []
    for old, new in renames.items():
        col = next((c for c in cfg["columns"] if c["name"].lower() == old.lower()), None)
        if not col:
            raise ValueError(f"'{old}' is not a config column of {SRC}")
        if old in key_cols:
            raise ValueError(f"refusing: '{old}' is a business/merge key")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", new):
            raise ValueError(f"invalid new name {new!r}")
        if (any(c["name"].lower() == new.lower() for c in cfg["columns"]) or new.lower() in TECH_COLS
                or new.lower() in {k.lower() for k in tf_exprs}):
            raise ValueError(f"new name '{new}' collides with an existing column/tech/transformation target")
        refs = [t for t, e in tf_exprs.items() if re.search(rf"\b{re.escape(old)}\b", e)]
        if refs:
            raise ValueError(f"refusing: transformation(s) {refs} reference '{old}' — update their exprs in YAML first, then retry (or rebuild)")
        yaml_changes.append(f"columns: '{old}' -> '{new}'")
        if old in (cfg.get("pii_columns") or {}):
            yaml_changes.append(f"pii_columns key '{old}' -> '{new}'")
        for r in cfg.get("quality_rules") or []:
            if (r.get("column") or "").lower() == old.lower():
                yaml_changes.append(f"rule '{r.get('name')}' column -> '{new}'")
        if old in ((cfg.get("silver") or {}).get("exclude_columns") or []):
            yaml_changes.append(f"silver.exclude_columns '{old}' -> '{new}'")

    alter_plan, blocked = [], []
    for old, new in renames.items():
        for layer, tbl in sorted(layer_tables(cfg).items()):
            if layer in ("silver", "history") and old in excluded_cols(cfg):
                continue
            if old.lower() not in actual_cols(tbl):
                continue
            rows = spark.sql(f"SHOW TBLPROPERTIES {tbl} ('delta.columnMapping.mode')").collect()
            cm = rows[0]["value"] if rows else ""
            if cm != "name":
                blocked.append(f"{tbl}: columnMapping.mode={cm or 'unset'} — RENAME COLUMN requires 'name'")
            else:
                alter_plan.append((layer, tbl, f"ALTER TABLE {tbl} RENAME COLUMN `{old}` TO `{new}`"))

    for c in yaml_changes:
        print("  YAML:", c)
    for layer, tbl, stmt in alter_plan:
        print(f"  [{layer:10s}] {stmt}")
    for b in blocked:
        print("  BLOCKED:", b)
    print("  note: rename changes _row_hash inputs — re-touched keys re-update next run (expected)")
    if DRY:
        print("\nDRY RUN — nothing changed.")
    elif blocked:
        print("\nRouted: enable column mapping (ALTER TABLE ... SET TBLPROPERTIES ('delta.columnMapping.mode'='name')) or rebuild:")
        print(f"  'MDD On-Demand Runbook Job': source_name={SRC}, mode=rebuild, confirm=true (update renames in YAML first)")
        raise RuntimeError("rename blocked on column mapping — see above")
    elif alter_plan:
        busy = hourly_pipeline_busy()
        if busy:
            raise RuntimeError(f"ABORT — hourly pipeline mid-run: {busy}. Re-run when idle.")
        for layer, tbl, stmt in alter_plan:  # tables first, then YAML (order keeps loader validation green)
            spark.sql(stmt)
            print("  executed:", stmt)
        for old, new in renames.items():
            for c in cfg["columns"]:
                if c["name"].lower() == old.lower():
                    c["name"] = new
            if old in (cfg.get("pii_columns") or {}):
                cfg["pii_columns"][new] = cfg["pii_columns"].pop(old)
            for r in cfg.get("quality_rules") or []:
                if (r.get("column") or "").lower() == old.lower():
                    r["column"] = new
            exl = (cfg.get("silver") or {}).get("exclude_columns") or []
            (cfg.get("silver") or {}) ["exclude_columns"] = [new if x == old else x for x in exl]
        write_cfg(SRC, cfg)
        print("YAML updated.")
        print(run_config_loader())
        for old, new in renames.items():
            for layer, tbl in sorted(layer_tables(cfg).items()):
                if layer in ("silver", "history") and new in excluded_cols(cfg):
                    continue
                act = actual_cols(tbl)
                if old.lower() in act or new.lower() in act:
                    print(f"  VERIFY {layer}: '{old}' gone: {old.lower() not in act}, '{new}' present: {new.lower() in act}")
        print("RENAME done.")
    else:
        print("Nothing to execute.")

# COMMAND ----------

# DBTITLE 1,DROP — report and route
# DROP — report-and-route only. NEVER executes a drop: removal is a rebuild decision.
if ACTION == "DROP":
    if not SRC:
        print("DROP (all sources): orphaned business columns (table has, YAML lacks):")
        found = False
        for s in active_sources():
            cfg = check_source(s)
            for layer, tbl in sorted(layer_tables(cfg).items()):
                exp, act = expected_cols(cfg, layer), actual_cols(tbl)
                orph = [k for k in act if k not in exp and k not in TECH_COLS]
                if orph:
                    found = True
                    print(f"  {s}/{layer}: {orph}")
        if not found:
            print("  none — no orphaned columns anywhere.")
    else:
        cfg = check_source(SRC)
        name = str(SPEC.get("name") or "")
        if not name:
            raise ValueError("DROP with source_name requires spec {'name': ...}")
        locations = {l: t for l, t in layer_tables(cfg).items() if name.lower() in actual_cols(t)}
        rules = [r.get("name") for r in cfg.get("quality_rules") or [] if (r.get("column") or "").lower() == name.lower()]
        in_yaml = any(c["name"].lower() == name.lower() for c in cfg["columns"])
        pii = name in (cfg.get("pii_columns") or {})
        print(f"DROP report for {SRC}.{name}:")
        print(f"  in YAML columns: {in_yaml}")
        print(f"  physically present in: {locations or 'nowhere'}")
        if rules:
            print(f"  DQ rules reference it: {rules} — remove/retarget them in YAML first")
        if pii:
            print("  PII column: its masks/tags would be orphaned until rebuild")
        print("  _row_hash formula changes (all config columns are hash inputs) — re-touched keys re-update")
        print("\nRouted (never auto-executed):")
        print(f"  1) remove the column + its rules/pii entry from YAML, re-run the config loader")
        print(f"  2) rebuild to make it fully vanish: 'MDD On-Demand Runbook Job' — source_name={SRC}, mode=rebuild, confirm=true")
        print("     (drops bronze/quarantine/silver/history + checkpoint + watermark; audit logs survive; raw files must remain in landing)")
print("\nRunbook finished.")

# COMMAND ----------

