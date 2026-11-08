# Databricks notebook source
# DBTITLE 1,MDD Config Loader
# MAGIC %md
# MAGIC # MDD Config Loader
# MAGIC
# MAGIC **Trigger:** file arrival in `/Volumes/mdd_platform/config/config_files/` (job-level file-arrival trigger, 60s debounce; also runs via Run Now) — NOT on every pipeline run.
# MAGIC > Note: the file-arrival trigger is **UNPAUSED** (enabled 2026-11-08) — new/changed YAML configs dropped into the volume auto-load within ~2 min. Explicit runs (Run Now, and the schema-evolution runbook's synchronous call) also always work.
# MAGIC
# MAGIC Responsibilities:
# MAGIC 1. **Load/refresh YAML configs** into `mdd_platform.config.pipeline_configs` (idempotent MERGE; reloads never flip the `active` flag).
# MAGIC 2. **PII governance** — create mask functions, apply tags+masks on silver targets (incl. SCD2 history) **and quarantine tables**, tags on bronze raw columns. Deferred PII (targets not yet created) is caught up on the next loader run.
# MAGIC 3. **Validate transformation definitions** in YAML before they reach the pipeline.

# COMMAND ----------

# DBTITLE 1,Load configs + apply PII governance
import yaml, json, datetime
from pyspark.sql import functions as F

# Catalog: bundle variable — the loader job passes catalog_name as a task parameter
# (default mdd_platform keeps interactive runs in the source workspace working).
dbutils.widgets.text("catalog_name", "mdd_platform", "MDD catalog (bundle passes ${var.catalog_name})")
CAT = dbutils.widgets.get("catalog_name").strip() or "mdd_platform"
CFG_VOL = f"/Volumes/{CAT}/config/config_files"

def _local(p): return p.replace("dbfs:", "").replace("file:", "")
def _q(s): return "'" + str(s).replace("'", "''") + "'"

def ensure_framework_tables():
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {CAT}.config.pipeline_configs (
      source_name STRING, config STRING, business_keys STRING, pii_columns STRING,
      quality_rules STRING, file_path STRING, active BOOLEAN, last_loaded_ts TIMESTAMP)""")

import re as _re
# --- Lightweight safety gate for silver.transformations ---------------------------------
# Expressions may only use these functions, plain column references, arithmetic and literals
# (row-deterministic; no subqueries, no DDL, no string interpolation tricks). Fail the loader
# loudly on anything else - configs stay metadata, never arbitrary SQL execution.
TFN_ALLOW = {"cast", "concat", "trim", "ltrim", "rtrim", "upper", "lower", "initcap", "length",
             "substring", "substr", "split", "replace", "regexp_replace", "regexp_extract",
             "regexp_extract_all", "round", "bround", "ceil", "floor", "abs", "coalesce",
             "lpad", "rpad", "reverse", "if", "nullif", "nanvl", "power", "mod", "sqrt", "exp", "ln", "log"}
TFN_KEYWORDS = {"as", "case", "when", "then", "else", "end", "and", "or", "not", "in", "like",
                "between", "is", "null", "true", "false", "interval", "div", "decimal", "int",
                "integer", "bigint", "smallint", "tinyint", "double", "float", "string", "varchar",
                "char", "timestamp", "date", "boolean", "numeric"}

def validate_transformations(cfg):
    """Validates the silver section: transformations (expr whitelist) + exclude_columns."""
    src = cfg.get("source_name")
    s_sec = cfg.get("silver") or {}
    t_list = s_sec.get("transformations") or []
    known = {c["name"] for c in cfg["columns"]}
    excluded = set(s_sec.get("exclude_columns") or [])
    if excluded:
        if excluded - known:
            raise ValueError(f"{src}: silver.exclude_columns references unknown columns {sorted(excluded - known)}")
        key_cols = (set(cfg.get("business_keys") or []) | set(s_sec.get("business_keys") or [])
                    | set(s_sec.get("merge_keys") or []))
        if excluded & key_cols:
            raise ValueError(f"{src}: exclude_columns cannot contain business/merge keys {sorted(excluded & key_cols)}")
        if excluded & {t.get("column") for t in t_list}:
            raise ValueError(f"{src}: exclude_columns cannot contain transformation targets {sorted(excluded & {t.get('column') for t in t_list})}")
        print(f"{src}: {len(excluded)} column(s) excluded from silver (kept in bronze/quarantine/_row_hash)")
    if not t_list:
        return
    seen = set()
    for t in t_list:
        col, typ, expr = t.get("column"), str(t.get("type") or "string"), str(t.get("expr") or "")
        if not col or not _re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", col):
            raise ValueError(f"{src}: bad transformation column name {col!r}")
        if not _re.fullmatch(r"[A-Za-z0-9_(), ]+", typ):
            raise ValueError(f"{src}: transformation {col}: suspicious type {typ!r}")
        if col in known or col in seen:
            raise ValueError(f"{src}: transformation target {col} collides with an existing column")
        seen.add(col)
        if ";" in expr or "--" in expr or "`" in expr or _re.search(r"\bselect\b", expr, _re.I):
            raise ValueError(f"{src}: transformation {col}: forbidden tokens in expr")
        fns = {m.lower() for m in _re.findall(r"([A-Za-z_][A-Za-z0-9_]*)\s*\(", expr)}
        bad = fns - TFN_ALLOW - TFN_KEYWORDS  # type specs like decimal(8,2) aren't function calls
        if bad:
            raise ValueError(f"{src}: transformation {col}: functions not allowed {sorted(bad)}")
        bare = _re.sub(r"'[^']*'", "''", expr)  # ignore string literals before identifier scan
        idents = set(_re.findall(r"[A-Za-z_][A-Za-z0-9_]*", bare))
        unknown = {i for i in idents if i not in known and i.lower() not in TFN_ALLOW
                   and i.lower() not in TFN_KEYWORDS}
        if unknown:
            raise ValueError(f"{src}: transformation {col}: unknown columns/identifiers {sorted(unknown)}")
    print(f"{src}: {len(t_list)} transformation(s) validated (whitelist OK)")

def load_configs():
    """Read all YAML configs from the volume and MERGE into the control table (idempotent)."""
    rows = []
    for f in dbutils.fs.ls(CFG_VOL):
        if f.name.endswith(".yaml"):
            cfg = yaml.safe_load(open(_local(f.path)).read())
            validate_transformations(cfg)
            rows.append((cfg["source_name"], json.dumps(cfg), json.dumps(cfg["business_keys"]),
                         json.dumps(cfg["pii_columns"]), json.dumps(cfg["quality_rules"]),
                         f.path, True, datetime.datetime.now()))
    df = spark.createDataFrame(rows,
        "source_name string, config string, business_keys string, pii_columns string, quality_rules string, file_path string, active boolean, last_loaded_ts timestamp")
    df.createOrReplaceTempView("cfg_updates")
    spark.sql(f"""MERGE INTO {CAT}.config.pipeline_configs t USING cfg_updates s
        ON t.source_name = s.source_name
        WHEN MATCHED THEN UPDATE SET t.config = s.config, t.business_keys = s.business_keys,
          t.pii_columns = s.pii_columns, t.quality_rules = s.quality_rules,
          t.file_path = s.file_path, t.last_loaded_ts = s.last_loaded_ts
        WHEN NOT MATCHED THEN INSERT *""")
    # NOTE: active flag deliberately NOT updated on MATCHED — reloads never reactivate
    # sources deactivated in the control table. New YAMLs start active.

def get_active_configs():
    return [json.loads(r["config"]) for r in spark.sql(
        f"SELECT config FROM {CAT}.config.pipeline_configs WHERE active=true ORDER BY source_name").collect()]

def bronze_target(cfg):
    b = cfg.get("bronze") or {}
    return b.get("target_table", f"{CAT}.bronze.{cfg['source_name']}")

def silver_target(cfg):
    s = cfg.get("silver") or {}
    return s.get("target_table", f"{CAT}.silver.{cfg['source_name']}")

def quarantine_target(cfg):
    q = cfg.get("quarantine") or {}
    return q.get("target_table", f"{CAT}.quarantine.{cfg['source_name']}")

# Masking functions live in mdd_platform.config; extend MASK_FN mapping as needed.
# Governed tag policy on 'pii' key allows only: email, ssn, password, Account, phone.
MASK_FN = {"email": "mask_email", "phone": "mask_phone", "name": "mask_name",
           "street_address": "mask_full", "financial": "mask_full"}
GOVERNED_PII_ALLOWED = {"email", "phone"}

spark.sql(f"""CREATE OR REPLACE FUNCTION {CAT}.config.mask_email(v STRING)
  RETURNS STRING RETURN CASE WHEN v IS NULL THEN NULL
    ELSE CONCAT(LEFT(SPLIT_PART(v,'@',1),2),'***@',SPLIT_PART(v,'@',2)) END""")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CAT}.config.mask_phone(v STRING)
  RETURNS STRING RETURN CASE WHEN v IS NULL THEN NULL
    ELSE CONCAT('***-***-', RIGHT(regexp_replace(v,'[^0-9]',''), 4)) END""")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CAT}.config.mask_name(v STRING)
  RETURNS STRING RETURN CASE WHEN v IS NULL THEN NULL ELSE CONCAT(LEFT(v,1),'***') END""")
spark.sql(f"""CREATE OR REPLACE FUNCTION {CAT}.config.mask_full(v STRING)
  RETURNS STRING RETURN CASE WHEN v IS NULL THEN NULL ELSE '***MASKED***' END""")

def silver_pii_targets(cfg):
    """Silver-facing targets for PII masking: current-state table + SCD2 history table when enabled.
    For scd2-only the current state is a VIEW over history, so only the physical history table is masked."""
    s_cfg = cfg.get("silver") or {}
    strategy = (s_cfg.get("merge_strategy") or cfg.get("merge_strategy") or "scd1").lower()
    hist = (s_cfg.get("scd2") or {}).get("target_table") or f"{silver_target(cfg)}_history"
    if strategy == "scd2":
        return [hist]
    if strategy == "scd1+scd2":
        return [silver_target(cfg), hist]
    return [silver_target(cfg)]

ensure_framework_tables()
load_configs()
n = spark.sql(f"SELECT count(*) FROM {CAT}.config.pipeline_configs WHERE active").collect()[0][0]
print(f"configs loaded: {n} active sources")

def _table_exists(t):
    """New sources: bronze/silver tables don't exist until the first pipeline run creates them."""
    return spark.catalog.tableExists(t)

def _has_column(t, col):
    """Table AND column must exist: a transformation-derived PII column (e.g. phone_digits) only
    exists after the first pipeline run creates it — defer + catch up then, same as missing tables."""
    return _table_exists(t) and col in spark.sql(f"SELECT * FROM {t} LIMIT 0").columns

deferred = []
for cfg in get_active_configs():
    src = cfg["source_name"]
    for col, ptype in cfg["pii_columns"].items():
        tags = [f"'pii_type'='{ptype}'", "'sensitivity'='high'"]
        if ptype in GOVERNED_PII_ALLOWED:
            tags.append(f"'pii'='{ptype}'")
        # Quarantine rows carry the same PII as silver — mask + tag them too.
        # Masks apply at READ time, so the pipeline's quarantine writes are unaffected.
        # Transformation-derived columns (silver-only) never land in quarantine — skip it.
        t_cols = {t["column"] for t in ((cfg.get("silver") or {}).get("transformations") or [])}
        s_excluded = set(((cfg.get("silver") or {}).get("exclude_columns") or []))
        # excluded columns don't land in silver/history — skip their silver targets (quarantine + bronze still governed)
        for tgt in (([] if col in s_excluded else silver_pii_targets(cfg))
                    + ([] if col in t_cols else [quarantine_target(cfg)])):
            if _has_column(tgt, col):
                spark.sql(f"""ALTER TABLE {tgt} ALTER COLUMN `{col}` SET TAGS ({', '.join(tags)})""")
                spark.sql(f"""ALTER TABLE {tgt} ALTER COLUMN `{col}` SET MASK {CAT}.config.{MASK_FN[ptype]}""")
            else:
                deferred.append(f"{src}: {tgt}.{col}")
        # Bronze keeps raw values (pipeline reads bronze to build silver) — PII is governed by tags.
        # Transformation-derived columns (e.g. phone_digits) are silver-only — never land in bronze.
        if col in {t["column"] for t in ((cfg.get("silver") or {}).get("transformations") or [])}:
            pass
        elif _has_column(bronze_target(cfg), col):
            btags = ", ".join([f"'pii_type'='{ptype}'", "'sensitivity'='high'" ] +
                              ([f"'pii'='{ptype}'"] if ptype in GOVERNED_PII_ALLOWED else []))
            spark.sql(f"""ALTER TABLE {bronze_target(cfg)} ALTER COLUMN `{col}` SET TAGS ({btags})""")
        else:
            deferred.append(f"{src}: {bronze_target(cfg)}.{col}")
print("PII tags + masks applied to silver (incl. SCD2 history) + quarantine; PII tags applied to bronze raw columns")
if deferred:
    print(f"PII DEFERRED for {len(deferred)} column(s) — target table(s)/column(s) not created yet "
          f"(normal for a new source or a transformation-derived column before its first pipeline run; "
          f"re-run the config loader after that run to catch up):")
    for d in deferred:
        print("  -", d)