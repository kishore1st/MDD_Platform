# Databricks notebook source
# DBTITLE 1,MDD On-Demand Runbook
# MAGIC %md
# MAGIC # MDD On-Demand Runbook
# MAGIC Single-source operations, triggered on demand (no schedule). Parameters: **source_name** (required), **mode** (`dry` default | `load` | `restore` | `rebuild`), **restore_layers** (`silver` | `silver+history`), **restore_ts** (UTC 'YYYY-MM-DD HH:MM:SS'), **confirm** (must be `true` for the destructive restore/rebuild modes). `load` delegates to the framework notebook with `source_name` (SCD1/SCD2 follows the source's YAML `merge_strategy`). `restore` rolls back via Delta time travel and realigns the silver watermark. `rebuild` drops tables + checkpoint + watermark and reprocesses from source files.

# COMMAND ----------

# DBTITLE 1,Runbook — validate, then dry/load/restore/rebuild
import datetime, json
from pyspark.sql import functions as F

CAT = "mdd_platform"
FRAMEWORK_NB = "/MDD_DataPlatform/MDD Metadata-Driven Pipeline Framework"
CKPT_VOL = f"/Volumes/{CAT}/config/checkpoints"

dbutils.widgets.text("source_name", "", "REQUIRED: source to process (e.g. src_002_orders)")
dbutils.widgets.dropdown("mode", "dry", ["dry", "load", "restore", "rebuild"], "Runbook mode")
dbutils.widgets.dropdown("restore_layers", "silver+history", ["silver", "silver+history"], "restore: layers to roll back")
dbutils.widgets.text("restore_ts", "", "restore: UTC timestamp 'YYYY-MM-DD HH:MM:SS'")
dbutils.widgets.text("confirm", "false", "must be 'true' for restore/rebuild (destructive)")

SRC = dbutils.widgets.get("source_name").strip()
MODE = dbutils.widgets.get("mode")
LAYERS = dbutils.widgets.get("restore_layers")
RTS = dbutils.widgets.get("restore_ts").strip()
CONFIRM = dbutils.widgets.get("confirm").strip().lower() == "true"

def _q(s): return "'" + str(s).replace("'", "''") + "'"
spark.sql(f"""CREATE TABLE IF NOT EXISTS {CAT}.audit.run_log (
  run_id STRING, source_name STRING, layer STRING, status STRING, rows_read BIGINT,
  rows_written BIGINT, rows_rescued BIGINT, rows_quarantined BIGINT, rows_merged BIGINT,
  error_message STRING, start_ts TIMESTAMP, end_ts TIMESTAMP)""")

def log_runbook(src, status, msg):
    run_id = "runbook_" + datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    spark.sql(f"""INSERT INTO {CAT}.audit.run_log VALUES
      ('{run_id}', '{src}', 'runbook', '{status}', NULL, NULL, NULL, NULL, NULL,
       {('NULL' if msg is None else _q(msg))}, current_timestamp(), current_timestamp())""")

# --- validate source (fail fast on typos/inactive) ---
if not SRC:
    raise ValueError("source_name parameter is required")
cfg_rows = spark.sql(f"""SELECT config FROM {CAT}.config.pipeline_configs
    WHERE source_name = '{SRC.replace("'", "''")}' AND active = true""").collect()
if not cfg_rows:
    raise ValueError(f"source '{SRC}' not found or inactive in {CAT}.config.pipeline_configs")
cfg = json.loads(cfg_rows[0]["config"])
s_cfg = cfg.get("silver") or {}
strategy = (s_cfg.get("merge_strategy") or cfg.get("merge_strategy") or "scd1").lower()
silver_tbl = s_cfg.get("target_table", f"{CAT}.silver.{SRC}")
bronze_tbl = (cfg.get("bronze") or {}).get("target_table", f"{CAT}.bronze.{SRC}")
hist_tbl = ((s_cfg.get("scd2") or {}).get("target_table") or f"{silver_tbl}_history")

wm_row = spark.sql(f"""SELECT max(last_processed_ingest_ts) w FROM {CAT}.audit.silver_watermarks
    WHERE source_name = '{SRC}'""").collect()[0]["w"]
wm = wm_row if wm_row else datetime.datetime(1900, 1, 1)
pending = (spark.table(bronze_tbl).where(F.col("_ingest_ts") > F.lit(wm)).count()
           if spark.catalog.tableExists(bronze_tbl) else None)  # None = new source, bronze not created yet

print(f"source: {SRC}")
print(f"merge_strategy: {strategy}")
print(f"targets: bronze={bronze_tbl} silver={silver_tbl}" + (f" history={hist_tbl}" if strategy in ("scd2", "scd1+scd2") else ""))
print(f"watermark: {wm} | bronze rows pending past watermark: {pending}")
print(f"mode: {MODE}" + (f" | layers: {LAYERS} | restore_ts: '{RTS}'" if MODE == "restore" else ""))

if MODE == "dry":
    print("\nDRY RUN complete - no changes made.")
    print("Next: mode=load (safe, incremental) or mode=restore|rebuild with confirm=true.")
elif MODE == "load":
    print(f"\nLOAD: running framework for {SRC} only (strategy follows YAML)...")
    dbutils.notebook.run(FRAMEWORK_NB, 2400, {"source_name": SRC})
    log_runbook(SRC, "success", "on-demand load via framework notebook")
elif MODE == "restore":
    if not CONFIRM:
        raise ValueError("restore is destructive: set confirm=true (inspect first with mode=dry)")
    if not RTS:
        raise ValueError("restore_ts required: 'YYYY-MM-DD HH:MM:SS' (UTC)")
    layers = [silver_tbl]
    if LAYERS == "silver+history" and strategy in ("scd2", "scd1+scd2"):
        layers.append(hist_tbl)
    for t in layers:
        n_ver = spark.sql(f"DESCRIBE HISTORY {t}").count()
        print(f"{t}: {n_ver} versions available; RESTORE TO TIMESTAMP AS OF '{RTS}'")
        spark.sql(f"RESTORE TABLE {t} TO TIMESTAMP AS OF '{RTS}'")
    # realign watermark to the restored state so the next incremental run continues from there
    restored_max = spark.table(silver_tbl).agg(F.max("_ingest_ts")).collect()[0][0]
    spark.sql(f"""MERGE INTO {CAT}.audit.silver_watermarks t
      USING (SELECT '{SRC}' s, timestamp'{restored_max}' w) u ON t.source_name = u.s
      WHEN MATCHED THEN UPDATE SET last_processed_ingest_ts = u.w
      WHEN NOT MATCHED THEN INSERT (source_name, last_processed_ingest_ts) VALUES (u.s, u.w)""")
    print(f"watermark realigned to {restored_max} (restored silver max _ingest_ts)")
    log_runbook(SRC, "success", f"restore {LAYERS} to '{RTS}', watermark realigned to {restored_max}")
elif MODE == "rebuild":
    if not CONFIRM:
        raise ValueError("rebuild is destructive: set confirm=true")
    drops = [silver_tbl, f"{CAT}.quarantine.{SRC}"]
    if strategy in ("scd2", "scd1+scd2"):
        drops.append(hist_tbl)
    drops.append(bronze_tbl)
    for t in drops:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    dbutils.fs.rm(f"{CKPT_VOL}/{SRC}", True)
    spark.sql(f"DELETE FROM {CAT}.audit.silver_watermarks WHERE source_name = '{SRC}'")
    print("dropped tables + checkpoint + watermark; running framework for full rebuild...")
    dbutils.notebook.run(FRAMEWORK_NB, 2400, {"source_name": SRC})
    # PII catchup: masks/tags died with the dropped tables — re-run the config loader so PII
    # governance is re-applied to the rebuilt tables (previously required a manual loader run).
    print("re-applying PII governance (config loader)...")
    dbutils.notebook.run("/MDD_DataPlatform/MDD Config Loader", 1200)
    log_runbook(SRC, "success", "full rebuild from source files + PII governance catchup")
else:
    raise ValueError(f"unknown mode {MODE}")