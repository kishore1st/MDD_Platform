# Databricks notebook source
# DBTITLE 1,Documentation — modes
# MAGIC %md
# MAGIC # MDD Target Cleanup — reset captured data before reprocessing
# MAGIC
# MAGIC Run **in the TARGET workspace** via the *MDD Target Cleanup (mode)* job with parameters:
# MAGIC
# MAGIC | Param | Values | Meaning |
# MAGIC |---|---|---|
# MAGIC | `mode` | `dry` (default) | Print the plan only — nothing is touched |
# MAGIC | | `soft` | Truncate all bronze/silver/quarantine tables (incl. SCD2 history), clear Auto Loader checkpoints, reset silver watermarks. **Keeps** catalog structure, YAML configs, audit history, PII masks, ops views. Landing files stay — Auto Loader re-reads every file from scratch (new file names AND old ones all re-ingest) |
# MAGIC | | `full` | DROP catalog `mdd_platform` CASCADE (tables, views, volumes, checkpoints, configs, audit — everything) and re-create the empty skeleton. Most thorough; re-run the *MDD Target Setup* job afterwards |
# MAGIC | `confirm` | `true` required for soft/full | Safety gate — destructive modes abort without it |
# MAGIC
# MAGIC **Which mode?** New files with different names + fresh environment → `full` then setup. Reprocess the same landing files after a config change → `soft`.

# COMMAND ----------

# DBTITLE 1,Parameters + inventory
# Cell 1 — parameters + state inventory
import json
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
# Catalog: bundle variable (cleanup job passes catalog_name; default mdd_platform)
dbutils.widgets.text("catalog_name", "mdd_platform", "MDD catalog (bundle passes ${var.catalog_name})")
CAT = dbutils.widgets.get("catalog_name").strip() or "mdd_platform"
try:
    mode = dbutils.widgets.get("mode")
    confirm = dbutils.widgets.get("confirm")
except Exception:
    mode, confirm = "dry", "false"
mode, confirm = mode.strip().lower(), confirm.strip().lower()
print(f"mode={mode}  confirm={confirm}")
assert mode in ("dry", "soft", "full"), f"Unknown mode: {mode}"

def table_inventory(schema):
    try:
        return [(r["namespace"], r["tableName"]) for r in spark.sql(f"SHOW TABLES IN {CAT}.{schema}").collect()]
    except Exception:
        return []

data_schemas = ["bronze", "silver", "quarantine"]
inv = {s: table_inventory(s) for s in data_schemas}
for s, tables in inv.items():
    print(f"{s}: {[t[1] for t in tables]}")

try:
    n_cfg = spark.sql(f"SELECT count(*) FROM {CAT}.config.pipeline_configs").first()[0]
    n_wm = spark.sql(f"SELECT count(*) FROM {CAT}.audit.silver_watermarks").first()[0]
except Exception as e:
    n_cfg = n_wm = None
    print(f"control tables not readable ({str(e)[:100]}) — catalog may not exist yet")
try:
    ckpts = dbutils.fs.ls(f"/Volumes/{CAT}/config/checkpoints")
except Exception:
    ckpts = []
print(f"pipeline_configs rows: {n_cfg} | watermarks: {n_wm} | checkpoint dirs: {[c.name for c in ckpts]}")

# COMMAND ----------

# DBTITLE 1,Dry run / safety gate
# Cell 2 — DRY RUN: print the plan and stop
# Views in silver (SCD2 current-state views) are skipped during truncation — detected via table_type.
def physical_tables(schema):
    rows = spark.sql(f"""SELECT table_name, table_type FROM system.information_schema.tables
        WHERE table_schema = '{schema}' AND table_catalog = '{CAT}'""").collect()
    return [r.table_name for r in rows if r.table_type in ("MANAGED", "EXTERNAL")]

plan = {"soft": [f"TRUNCATE TABLE {CAT}.{s}.{t}" for s in data_schemas for t in physical_tables(s)]
                + [f"rm -r /Volumes/{CAT}/config/checkpoints/*",
                   f"DELETE FROM {CAT}.audit.silver_watermarks"],
        "full": [f"DROP CATALOG {CAT} CASCADE",
                 f"CREATE CATALOG {CAT} + schemas + volumes (skeleton re-bootstrap)"]}
print(f"== PLAN for mode={mode} ==")
for step in plan[mode]:
    print("  ", step)
if mode == "dry":
    print("\nDRY RUN complete — nothing was changed. Re-run with mode=soft|full and confirm=true to execute.")
else:
    if confirm != "true":
        print(f"\nABORTED: mode={mode} is destructive and requires confirm=true.")
        mode = "dry"

# COMMAND ----------

# DBTITLE 1,Soft cleanup
# Cell 3 — execute (soft)
if mode == "soft":
    for s in data_schemas:
        for t in physical_tables(s):
            spark.sql(f"TRUNCATE TABLE {CAT}.{s}.{t}")
            print(f"  truncated {CAT}.{s}.{t}")
    for c in dbutils.fs.ls(f"/Volumes/{CAT}/config/checkpoints"):
        dbutils.fs.rm(c.path, recurse=True)
        print(f"  removed checkpoint dir {c.path}")
    try:
        spark.sql(f"DELETE FROM {CAT}.audit.silver_watermarks")
        print("  reset silver watermarks")
    except Exception as e:
        print(f"  WARN watermarks: {str(e)[:120]}")
    print("\nSOFT CLEANUP DONE — next pipeline run re-ingests every landing file from scratch.")
    print("Landing files kept; configs/audit/PII masks/ops views kept.")

# COMMAND ----------

# DBTITLE 1,Full cleanup + skeleton
# Cell 4 — execute (full): drop catalog + re-create skeleton
if mode == "full":
    spark.sql(f"DROP CATALOG {CAT} CASCADE")
    print(f"  dropped catalog {CAT} (all schemas, tables, views, volumes)")
    SCHEMAS = ["audit", "bronze", "config", "landing", "quarantine", "silver", "ops"]
    VOLUMES = [("config", "config_files"), ("config", "checkpoints"), ("landing", "source_files")]
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CAT}")
    for s in SCHEMAS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{s}")
    for sch, vol in VOLUMES:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {CAT}.{sch}.{vol}")
    print("  re-created empty catalog skeleton")
    print("\nFULL CLEANUP DONE — now run the 'MDD Target Setup' job to reload configs/views,"
          f" then upload your new landing files. /Volumes/{CAT}/landing/source_files is empty;")
    print("re-copy any landing files you want processed (the smoke test's samples are in the bundle at bundle_assets/landing_samples).")

# COMMAND ----------

