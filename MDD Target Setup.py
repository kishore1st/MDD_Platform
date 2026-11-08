# Databricks notebook source
# DBTITLE 1,Documentation — what setup does
# MAGIC %md
# MAGIC # MDD Target Setup — one-shot environment bootstrap
# MAGIC
# MAGIC Runs **in the TARGET workspace** after `databricks bundle deploy -t prod`. Creates everything the framework, dashboard, and alerts need:
# MAGIC
# MAGIC | Step | What it does |
# MAGIC |---|---|
# MAGIC | 1 | Catalog (**`catalog_name` parameter** — bundle variable, defaults to `mdd_platform`) + schemas (audit, bronze, config, landing, quarantine, silver, ops) + managed volumes (config_files, checkpoints, source_files) — all IF NOT EXISTS, fully idempotent |
# MAGIC | 2 | Copies the 7 source YAML configs from the bundle (`bundle_configs/`) into the config volume |
# MAGIC | 3 | Runs the MDD Config Loader job once → control table `config.pipeline_configs` populated, PII masks deferred until tables exist |
# MAGIC | 4 | Runs the MDD Pipeline Orchestration job once → all bronze/silver/quarantine/audit tables created (empty) |
# MAGIC | 5 | Applies the captured `ops` views (from `bundle_assets/ops_views_capture.json`) and the 3 metric views, substituting the source-workspace **job IDs AND catalog name** with the target's; then patches **every deployed MDD notebook and the Ops Dashboard file** the same way (job IDs + catalog) |
# MAGIC
# MAGIC **Prerequisites**: bundle deployed (jobs + notebooks exist at /Workspace/MDD_DataPlatform). Deploying user needs CREATE CATALOG rights.
# MAGIC **After this**: run the MDD Target Smoke Test job. Views that depend on tables which only appear later (e.g. quarantine tables with zero violations) are retried by the smoke test.

# COMMAND ----------

# DBTITLE 1,Catalog skeleton
# Cell 1 — catalog skeleton (idempotent). Catalog comes from the bundle's
# catalog_name variable (the setup job passes it as a task parameter); defaults
# to mdd_platform for interactive runs in the source workspace.
dbutils.widgets.text("catalog_name", "mdd_platform", "MDD catalog to create/populate")
CAT = dbutils.widgets.get("catalog_name").strip()
SCHEMAS = ["audit", "bronze", "config", "landing", "quarantine", "silver", "ops"]
VOLUMES = [("config", "config_files"), ("config", "checkpoints"), ("landing", "source_files")]

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CAT}")
for s in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{s}")
for sch, vol in VOLUMES:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CAT}.{sch}.{vol}")

print("Catalog skeleton ready:")
for r in spark.sql(f"SHOW SCHEMAS IN {CAT}").collect():
    print("  schema:", r[0])
for r in spark.sql(f"SHOW VOLUMES IN {CAT}.config").collect():
    print("  volume:", r[0])
for r in spark.sql(f"SHOW VOLUMES IN {CAT}.landing").collect():
    print("  volume:", r[0])

# COMMAND ----------

# DBTITLE 1,Helpers
# Cell 2 — helpers: job lookup, run-and-wait, workspace-file -> volume copy
import io, json, time, posixpath
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
BUNDLE_WS = "/Workspace/MDD_DataPlatform/files"

def job_id_by_name(name):
    for j in w.jobs.list():
        if j.settings and j.settings.name == name:
            return j.job_id
    return None

def run_job_and_wait(name, timeout_minutes=25):
    jid = job_id_by_name(name)
    if jid is None:
        raise ValueError(f"Job not found in this workspace: {name}. Was the bundle deployed first?")
    run = w.jobs.run_now(int(jid))
    deadline = time.time() + timeout_minutes * 60
    while True:
        r = w.jobs.get_run(run.run_id)
        state = str(r.state.life_cycle_state)
        if state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            result = str(r.state.result_state)
            print(f"{name}: {state} / {result}")
            if result != "SUCCESS":
                raise RuntimeError(f"Job {name} run {run.run_id} finished with {result}: "
                                   f"{r.state.state_message}")
            return run
        if time.time() > deadline:
            raise TimeoutError(f"Job {name} run {run.run_id} did not finish within {timeout_minutes} min")
        time.sleep(15)

def ws_read_text(path):
    """Read a workspace file as text (SDK download)."""
    buf = io.BytesIO()
    resp = w.workspace.download(path)
    data = resp.content if hasattr(resp, "content") else resp.read()
    return data.decode("utf-8")

def copy_ws_dir_to_volume(ws_dir, volume_dir, text_fn=None):
    """Copy every file under a workspace directory into a UC volume directory.
    text_fn (optional) transforms file contents before writing (e.g. catalog substitution)."""
    copied = []
    for obj in w.workspace.list(ws_dir):
        if (obj.path or "").endswith((".yaml", ".yml", ".csv", ".json")):
            rel = obj.path[len(ws_dir.rstrip('/')) + 1:]
            text = ws_read_text(obj.path)
            if text_fn:
                text = text_fn(text)
            dst = f"{volume_dir.rstrip('/')}/{rel}"
            parent = posixpath.dirname(dst)
            try:
                dbutils.fs.ls(parent)
            except Exception:
                dbutils.fs.mkdirs(parent)
            dbutils.fs.put(dst, text, overwrite=True)
            copied.append((rel, len(text)))
    return copied

print("helpers ready")

# COMMAND ----------

# DBTITLE 1,Load configs + run config loader
# Cell 3 — load the YAML configs into the config volume, then run the Config Loader job.
# YAML contents carry the source catalog (mdd_platform) in source_path/target_table —
# substitute the target catalog into every YAML before it lands in the config volume.
def subst_catalog(text):
    return text if CAT == "mdd_platform" else text.replace("mdd_platform", CAT)

copied = copy_ws_dir_to_volume(f"{BUNDLE_WS}/bundle_configs",
                                f"/Volumes/{CAT}/config/config_files", text_fn=subst_catalog)
print(f"Copied {len(copied)} config files:")
for rel, n in copied:
    print(f"  {rel} ({n} bytes)")
if not copied:
    raise RuntimeError("No config YAMLs found in bundle_configs — was the bundle fully exported/deployed?")

run_job_and_wait("MDD Config Loader Job")

active = spark.sql(f"SELECT source_name, active FROM {CAT}.config.pipeline_configs ORDER BY 1")
active.show(50, truncate=False)
n_active = spark.sql(f"SELECT count(*) FROM {CAT}.config.pipeline_configs WHERE active").first()[0]
print(f"Active sources in control table: {n_active}")
assert n_active > 0, "Control table is empty — config loader did not load anything."

# COMMAND ----------

# DBTITLE 1,First framework run
# Cell 4 — first framework run: creates all bronze/silver/quarantine/audit tables (empty)
run_job_and_wait("MDD Pipeline Orchestration Job", timeout_minutes=30)

for sch in ("audit", "bronze", "silver", "quarantine"):
    rows = [r["tableName"] for r in spark.sql(f"SHOW TABLES IN {CAT}.{sch}").collect()]
    print(f"{sch}: {len(rows)} tables -> {rows}")

# COMMAND ----------

# DBTITLE 1,Apply ops views
# Cell 5 — apply captured ops views, substituting orchestration job IDs.
# Source-workspace job IDs embedded in DDL (real orchestration job + the stale one in the
# costing notebooks) are replaced with THIS workspace's orchestration job id.
OLD_JOB_IDS = ["736782493336258", "433265226360788"]
target_job_id = job_id_by_name("MDD Pipeline Orchestration Job")
assert target_job_id, "Orchestration job not found"
target_job_id = str(target_job_id)
print(f"Target orchestration job id: {target_job_id}")

def substitute(sql_text):
    """Repoint source-workspace references at THIS workspace: orchestration job ids
    (current + stale) and the source catalog name (mdd_platform -> target catalog)."""
    for old in OLD_JOB_IDS:
        sql_text = sql_text.replace(old, target_job_id)
    if CAT != "mdd_platform":
        sql_text = sql_text.replace("mdd_platform", CAT)
    return sql_text

capture = json.loads(ws_read_text(f"{BUNDLE_WS}/bundle_assets/ops_views_capture.json"))
print(f"Capture file loaded: {len(capture.get('views', {}))} views")

# Apply v_* views with a dependency retry loop (views can reference each other and tables
# that may not exist yet — up to 4 passes, leftovers reported and retried by the smoke test).
pending = {k: substitute(v) for k, v in capture["views"].items()}
applied, deferred = [], {}
for attempt in range(4):
    if not pending:
        break
    progress = False
    for name, ddl in list(pending.items()):
        try:
            spark.sql(ddl)
            applied.append(name)
            del pending[name]
            progress = True
        except Exception as e:
            deferred[name] = str(e)[:180]
    if not progress:
        break
print(f"Applied {len(applied)} views: {applied}")
if pending:
    print(f"DEFERRED ({len(pending)}) — the smoke test retries these after sample data lands:")
    for n, e in pending.items():
        print(f"  {n}: {e}")

# COMMAND ----------

# DBTITLE 1,Metric views + notebook patches + PII catch-up
# Cell 6 — apply metric views from bundle_assets/mv_*.yaml, patch ALL notebooks +
# dashboard (job ids + catalog), PII catch-up
mv_files = [o.path for o in w.workspace.list(f"{BUNDLE_WS}/bundle_assets")
            if o.path.endswith(".yaml") and "/mv_" in o.path]
mv_files.sort()
print(f"Metric view YAMLs found: {[p.split('/')[-1] for p in mv_files]}")
for path in mv_files:
    name = path.split("/")[-1].replace(".yaml", "")
    yaml_text = substitute(ws_read_text(path))  # repoint job ids + catalog inside the metric YAML
    spark.sql(f"CREATE OR REPLACE VIEW {CAT}.ops.{name} WITH METRICS LANGUAGE YAML AS\n$$\n{yaml_text}\n$$")
    print(f"  created metric view: {CAT}.ops.{name}")

# Patch EVERY deployed MDD notebook (and the Ops Dashboard source file) so source-workspace
# references — orchestration job ids AND the source catalog (mdd_platform) — point at THIS
# workspace. Covers all runtime notebooks (framework, loader, builders, runbooks, maintenance,
# costing, smoke test, cleanup, this setup notebook), the dashboard's dataset SQL, and the
# architecture doc; only files that actually contain a substituted reference are rewritten.
PATCH_TARGETS = sorted(
    o.path for o in w.workspace.list(BUNDLE_WS)
    if "DIRECTORY" not in str(getattr(o, "object_type", "") or "").upper())
for path in PATCH_TARGETS:
    try:
        is_notebook = not path.endswith((".json", ".yml", ".yaml", ".csv", ".md"))
        buf = io.BytesIO()
        if is_notebook:
            w.workspace.download(path, format="SOURCE", output=buf)
        else:
            w.workspace.download(path, output=buf)
        src = buf.getvalue().decode("utf-8")
        new_src = substitute(src)
        if new_src != src:
            if is_notebook:
                w.workspace.upload(path, new_src.encode("utf-8"), format="SOURCE", overwrite=True)
            else:
                w.workspace.upload(path, new_src.encode("utf-8"), overwrite=True)
            print(f"  patched (job id / catalog): {path}")
        else:
            print(f"  unchanged: {path}")
    except Exception as e:
        print(f"  WARN could not patch {path}: {str(e)[:150]}")

# Re-run the config loader now that silver tables exist — applies deferred PII masks/tags.
run_job_and_wait("MDD Config Loader Job")
print("\nSETUP COMPLETE. Next: run the 'MDD Target Smoke Test' job.")

# COMMAND ----------

