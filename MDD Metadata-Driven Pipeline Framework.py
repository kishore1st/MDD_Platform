# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Architecture Overview
# MAGIC %md
# MAGIC # Metadata-Driven Pipeline Framework (MDD)
# MAGIC
# MAGIC **Flow (per pipeline run):** `Control table (active sources) → Auto Loader Reader (cloudFiles + _rescued_data) → Bronze Delta → Quality Rules Engine → Quarantine (failures) + Silver (SCD1/SCD2 MERGE on business keys) → Audit & Observability`
# MAGIC
# MAGIC **Off-run:** `YAML configs (UC Volume) → MDD Config Loader job → control table + PII governance (masks + tags)` — runs when configs or table schemas change, not on every pipeline run.
# MAGIC
# MAGIC ## Assets
# MAGIC - **Config volume:** `/Volumes/mdd_platform/config/config_files` — one YAML per source (schema, business keys, PII columns, quality rules)
# MAGIC - **Landing volume:** `/Volumes/mdd_platform/landing/source_files/<source_name>/` — incoming CSV files
# MAGIC - **Checkpoints:** `/Volumes/mdd_platform/config/checkpoints/<source_name>` — Auto Loader state (incremental)
# MAGIC - **Control table:** `mdd_platform.config.pipeline_configs` (active flag drives which sources run)
# MAGIC - **Config loader:** `MDD Config Loader` notebook/job — loads YAMLs into the control table and applies PII governance; runs on config/schema change, not per pipeline run
# MAGIC - **Audit tables:** `mdd_platform.audit.run_log`, `audit.dq_metrics`, `audit.silver_watermarks`
# MAGIC
# MAGIC ## YAML template
# MAGIC ```yaml
# MAGIC source_name: src_001_customers
# MAGIC source_path: /Volumes/mdd_platform/landing/source_files/src_001_customers
# MAGIC format: csv
# MAGIC options: {header: 'true', delimiter: ','}
# MAGIC columns: [{name: customer_id}, ...]
# MAGIC business_keys: [customer_id]
# MAGIC pii_columns: {email: email, phone: phone}
# MAGIC quality_rules:
# MAGIC - {name: id_not_null, rule: not_null, column: customer_id}
# MAGIC - {name: email_format, rule: regex, column: email, pattern: '^...$'}
# MAGIC - {name: amount_range, rule: range, column: amount, min: 0.0, max: 100000.0}
# MAGIC - {name: status_allowed, rule: allowed, column: status, values: [pending, shipped]}
# MAGIC ```
# MAGIC
# MAGIC Run stages 1→5 in order. Add a new source by dropping a YAML config + a source folder and running the Config Loader job (new sources start active); no code changes needed.

# COMMAND ----------

# DBTITLE 1,Stage 1 — Bootstrap (framework tables + active-config reader)
import json, datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window

CAT = "mdd_platform"                      # <-- change catalog here
CKPT_VOL = f"/Volumes/{CAT}/config/checkpoints"

# Optional single-source mode (used by the on-demand runbook; empty = all active sources)
dbutils.widgets.text("source_name", "", "Process only this source (optional)")
SOURCE_PARAM = dbutils.widgets.get("source_name").strip()

def ensure_framework_tables():
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {CAT}.config.pipeline_configs (
      source_name STRING, config STRING, business_keys STRING, pii_columns STRING,
      quality_rules STRING, file_path STRING, active BOOLEAN, last_loaded_ts TIMESTAMP)""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {CAT}.audit.run_log (
      run_id STRING, source_name STRING, layer STRING, status STRING, rows_read BIGINT,
      rows_written BIGINT, rows_rescued BIGINT, rows_quarantined BIGINT, rows_merged BIGINT,
      error_message STRING, start_ts TIMESTAMP, end_ts TIMESTAMP)""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {CAT}.audit.silver_watermarks (
      source_name STRING, last_processed_ingest_ts TIMESTAMP)""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {CAT}.audit.dq_metrics (
      run_id STRING, source_name STRING, rule_name STRING, passed_rows BIGINT, failed_rows BIGINT, run_ts TIMESTAMP)""")

# NOTE: YAML loading lives in the "MDD Config Loader" notebook/job (which also owns PII
# governance) — this framework only READS the control table via get_active_configs() below.
# The active flag is deliberately never flipped back by reloads: sources deactivated in the
# control table stay inactive; new YAMLs start active.

def get_active_configs():
    cfgs = [json.loads(r["config"]) for r in spark.sql(
        f"SELECT config FROM {CAT}.config.pipeline_configs WHERE active=true ORDER BY source_name").collect()]
    if SOURCE_PARAM:
        cfgs = [c for c in cfgs if c["source_name"] == SOURCE_PARAM]
        if not cfgs:
            raise ValueError(f"source_name '{SOURCE_PARAM}' not found or inactive in {CAT}.config.pipeline_configs")
    return cfgs

ensure_framework_tables()  # idempotent CREATE IF NOT EXISTS — bootstraps framework tables on a fresh environment
print("active sources:", spark.sql(f"SELECT count(*) FROM {CAT}.config.pipeline_configs WHERE active").collect()[0][0],
      "| source filter:", SOURCE_PARAM or "none (all)")

# COMMAND ----------

# DBTITLE 1,Stage 2 — Bronze Ingestion (Auto Loader + rescue)
def _q(s): return "'" + str(s).replace("'", "''") + "'"

def bronze_target(cfg):
    b = cfg.get("bronze") or {}
    return b.get("target_table", f"{CAT}.bronze.{cfg['source_name']}")

def create_bronze_table(cfg, bronze_tbl):
    b = cfg.get("bronze") or {}
    cols = ", ".join(f"`{c['name']}` STRING" + (f" COMMENT {_q(c['comment'])}" if c.get("comment") else "")
                     for c in cfg["columns"])
    ddl = (f"CREATE TABLE IF NOT EXISTS {bronze_tbl} (\n      {cols}, _rescued_data STRING, "
           "_source_file STRING, _file_mtime TIMESTAMP, _ingest_ts TIMESTAMP, _batch_id STRING, _row_hash BIGINT")
    if "_ingest_date" in (b.get("partition_by") or []):
        ddl += ",\n      _ingest_date DATE GENERATED ALWAYS AS (CAST(_ingest_ts AS DATE))"
    ddl += ")"
    if b.get("partition_by"):
        ddl += f"\n      PARTITIONED BY ({', '.join(b['partition_by'])})"
    if b.get("comment") or cfg.get("description"):
        ddl += f"\n      COMMENT {_q(b.get('comment') or cfg['description'])}"
    if b.get("table_properties"):
        props = ", ".join(f"{_q(k)} = {_q(v)}" for k, v in b["table_properties"].items())
        ddl += f"\n      TBLPROPERTIES ({props})"
    spark.sql(ddl)

def write_bronze(bdf, batch_id, cfg, bronze_tbl, run_id, start_ts):
    bdf = (bdf.withColumn("_ingest_ts", F.current_timestamp())
              .withColumn("_batch_id", F.lit(str(batch_id)))
              .withColumn("_row_hash",
                  F.xxhash64(F.concat_ws("||", *[F.col(f"`{c['name']}`") for c in cfg["columns"]]))))
    bdf.write.format("delta").mode("append").option("mergeSchema", "false").saveAsTable(bronze_tbl)
    # single-pass fix: one scan computes total + rescued (was: two separate counts)
    agg = (spark.table(bronze_tbl).filter(F.col("_batch_id") == str(batch_id)).agg(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.when(F.col("_rescued_data").isNotNull(), 1).otherwise(0)).alias("r")).collect()[0])
    total, rescued = agg["n"], (agg["r"] or 0)
    ex = (cfg.get("bronze") or {}).get("exception_rules") or {}
    warn = None
    if ex.get("max_rescued_pct") is not None and total:
        pct = 100.0 * rescued / total
        if pct > float(ex["max_rescued_pct"]):
            if (ex.get("on_exceed") or "warn").lower() == "fail":
                raise Exception(f"bronze exception rule tripped: rescued {pct:.1f}% > max_rescued_pct "
                                f"{ex['max_rescued_pct']}% for {cfg['source_name']}")
            warn = f"rescued {pct:.1f}% exceeds max_rescued_pct {ex['max_rescued_pct']}%"
    spark.sql(f"""INSERT INTO {CAT}.audit.run_log VALUES
      ('{run_id}', '{cfg['source_name']}', 'bronze', 'success', {total}, {total},
      {rescued}, NULL, NULL, {('NULL' if warn is None else _q(warn))}, timestamp'{start_ts}', current_timestamp())""")

def ingest_bronze(cfg, run_id):
    """Auto Loader (cloudFiles) per source: PERMISSIVE mode, _rescued_data for schema drift,
    checkpointed, availableNow trigger -> incremental batch-style processing.
    Captures _metadata.file_modification_time as _file_mtime — the recency signal for silver
    dedup (current_timestamp() is pinned per microbatch, so _ingest_ts alone can tie) — and
    _metadata.file_path as _source_file for file-level provenance (which file each record and
    each quarantined bad record came from)."""
    start_ts = datetime.datetime.now()
    src, tbl = cfg["source_name"], bronze_target(cfg)
    create_bronze_table(cfg, tbl)
    schema = StructType([StructField(c["name"], StringType()) for c in cfg["columns"]])
    reader = (spark.readStream.format("cloudFiles").schema(schema)
        .option("cloudFiles.format", cfg["format"])
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("mode", "PERMISSIVE")
        .option("rescuedDataColumn", "_rescued_data")
        .option("enforceSchema", "false"))
    for k, v in dict(cfg.get("options", {})).items():
        reader = reader.option(k, v)
    df_src = (reader.load(cfg["source_path"])
        .select("*", F.col("_metadata.file_modification_time").alias("_file_mtime"),
                F.col("_metadata.file_path").alias("_source_file")))
    q = (df_src.writeStream
        .foreachBatch(lambda bdf, bid: write_bronze(bdf, bid, cfg, tbl, run_id, start_ts))
        .option("checkpointLocation", f"{CKPT_VOL}/{src}")
        .trigger(availableNow=True).queryName(f"bronze_{src}").start())
    q.awaitTermination()

run_id = "run_" + datetime.datetime.now().strftime("%Y%m%d%H%M%S")
import time; t0 = time.time()
for cfg in get_active_configs():
    try:
        ingest_bronze(cfg, run_id)
    except Exception as e:
        spark.sql(f"""INSERT INTO {CAT}.audit.run_log VALUES ('{run_id}', '{cfg['source_name']}', 'bronze', 'failed',
          NULL, NULL, NULL, NULL, NULL, '{str(e).replace("'", "''")[:4000]}', current_timestamp(), current_timestamp())""")
print(f"bronze done in {(time.time()-t0):.0f}s")

# COMMAND ----------

# DBTITLE 1,Stage 3 — Quality Rules + Quarantine + Silver MERGE
def rule_expr(rule):
    """YAML rule -> Spark boolean expression. try_cast: malformed numerics fail the rule (quarantine) instead of crashing."""
    c = F.col(f"`{rule['column']}`")
    if rule["rule"] == "not_null":
        return c.isNotNull() & (F.length(F.trim(c)) > 0)
    if rule["rule"] == "regex":
        return c.rlike(rule["pattern"])
    if rule["rule"] == "range":
        d = F.expr(f"try_cast(`{rule['column']}` as double)")
        return d.isNotNull() & (d >= F.lit(rule["min"])) & (d <= F.lit(rule["max"]))
    if rule["rule"] == "allowed":
        return c.isin(rule["values"])
    raise ValueError(f"unknown rule {rule['rule']}")

def get_watermark(src):
    r = spark.sql(f"SELECT max(last_processed_ingest_ts) w FROM {CAT}.audit.silver_watermarks WHERE source_name='{src}'").collect()[0]["w"]
    return r if r else datetime.datetime(1900, 1, 1)

def silver_target(cfg):
    s = cfg.get("silver") or {}
    return s.get("target_table", f"{CAT}.silver.{cfg['source_name']}")

def silver_dedup_keys(cfg):
    s = cfg.get("silver") or {}
    return s.get("business_keys", cfg["business_keys"])

def silver_merge_keys(cfg):
    s = cfg.get("silver") or {}
    return s.get("merge_keys", silver_dedup_keys(cfg))

def silver_transformations(cfg):
    """YAML silver.transformations: list of {column, type, comment, expr} - lightweight derived
    columns (casting / arithmetic / string ops / regex) evaluated on raw bronze values AFTER the
    quality rules (rules validate the input; transformations shape the output). Targets live in
    silver + SCD2 history ONLY - never bronze, never _row_hash (hash stays the raw bronze hash,
    carried unchanged, so identical re-deliveries still skip and SCD2 stays idempotent)."""
    return (cfg.get("silver") or {}).get("transformations") or []

def transform_cols_ddl(cfg):
    return ", ".join(f"`{t['column']}` {(t.get('type') or 'STRING').upper()}"
                     + (f" COMMENT {_q(t['comment'])}" if t.get("comment") else "")
                     for t in silver_transformations(cfg))

def transform_selects(cfg):
    """Cast each expression result to its declared type so view values always match the DDL."""
    return [F.expr(f"cast(({t['expr']}) as {(t.get('type') or 'STRING').upper()})").alias(t["column"])
            for t in silver_transformations(cfg)]

def silver_excluded(cfg):
    """YAML silver.exclude_columns: raw config columns NOT written to silver/SCD2 history
    (e.g. full_name once a split transformation produces first_name/last_name). They stay in
    bronze (raw), quarantine (raw), and the _row_hash inputs — so changes to an excluded column
    still change the carried hash and still trigger a silver update."""
    return set((cfg.get("silver") or {}).get("exclude_columns") or [])

def col_type(c):
    t = (c.get("type") or "STRING").upper()
    return f"VARCHAR({int(c['length'])})" if (t == "STRING" and c.get("length")) else t

def col_cast(c):
    return f"cast(`{c['name']}` as {col_type(c)}) as `{c['name']}`"

def process_silver(cfg, run_id):
    """Incremental: bronze rows past watermark -> evaluate rules -> failures to quarantine,
    passers deduped on business keys -> MERGE (SCD1) into silver."""
    start_ts = datetime.datetime.now()
    src, keys = cfg["source_name"], silver_dedup_keys(cfg)
    excluded = silver_excluded(cfg)
    if excluded - {c["name"] for c in cfg["columns"]}:
        raise ValueError(f"{src}: silver.exclude_columns references unknown columns {sorted(excluded - {c['name'] for c in cfg['columns']})}")
    bad_keys = (set(keys) | set(silver_merge_keys(cfg))) & excluded
    if bad_keys:
        raise ValueError(f"{src}: exclude_columns cannot contain business/merge keys {sorted(bad_keys)}")
    kept = [c for c in cfg["columns"] if c["name"] not in excluded]
    cols = ", ".join(f"`{c['name']}` {col_type(c)}" + (f" COMMENT {_q(c['comment'])}" if c.get("comment") else "")
                     for c in kept)
    t_ddl = transform_cols_ddl(cfg)
    cols = cols + (f", {t_ddl}" if t_ddl else "")
    s_cfg = cfg.get("silver") or {}
    strategy = (s_cfg.get("merge_strategy") or cfg.get("merge_strategy") or "scd1").lower()
    hist_tbl = ((s_cfg.get("scd2") or {}).get("target_table") or f"{silver_target(cfg)}_history")
    # Liquid clustering (YAML silver.cluster_by): keeps key-based join pruning sharp on silver
    # and SCD2 history as volume grows - cluster on the dedup/merge keys (the lookup path).
    cb = s_cfg.get("cluster_by") or []
    bad_cb = set(cb) - ({c["name"] for c in kept} | {t["column"] for t in silver_transformations(cfg)})
    if bad_cb:
        raise ValueError(f"{src}: silver.cluster_by references unknown columns {sorted(bad_cb)}")
    cb_cols = ", ".join(f"`{c}`" for c in cb)
    silver_ddl = f"CREATE TABLE IF NOT EXISTS {silver_target(cfg)} ({cols}, _ingest_ts TIMESTAMP, _merged_ts TIMESTAMP, _row_hash BIGINT)"
    if s_cfg.get("comment") or cfg.get("description"):
        silver_ddl += f" COMMENT {_q(s_cfg.get('comment') or cfg['description'])}"
    if s_cfg.get("table_properties"):
        sprops = ", ".join(f"{_q(k)} = {_q(v)}" for k, v in s_cfg["table_properties"].items())
        silver_ddl += f"\n      TBLPROPERTIES ({sprops})"
    spark.sql(silver_ddl)
    if cb_cols and strategy != "scd2":  # scd1 / scd1+scd2: current state is a physical table
        spark.sql(f"ALTER TABLE {silver_target(cfg)} CLUSTER BY ({cb_cols})")  # idempotent metadata op; OPTIMIZE reclusters existing data
    if strategy in ("scd2", "scd1+scd2"):
        # ensure history table + clustering BEFORE the watermark early-return below, so
        # table management runs on every pipeline run even when no new bronze rows arrived
        hcols = ", ".join(f"`{c['name']}` {col_type(c)}" + (f" COMMENT {_q(c['comment'])}" if c.get("comment") else "")
                          for c in kept)
        if t_ddl:
            hcols = hcols + f", {t_ddl}"
        hist_ddl = (f"CREATE TABLE IF NOT EXISTS {hist_tbl} (\n      {hcols}, _ingest_ts TIMESTAMP, _row_hash BIGINT,\n"
                    "      _effective_from TIMESTAMP COMMENT 'version valid from (= row _ingest_ts)',\n"
                    "      _effective_to TIMESTAMP COMMENT 'version valid to (NULL while current)',\n"
                    "      _is_current BOOLEAN COMMENT 'true = latest version of the business key')")
        if s_cfg.get("comment") or cfg.get("description"):
            hist_ddl += f" COMMENT {_q((s_cfg.get('comment') or cfg['description']) + ' - SCD2 history')}"
        if s_cfg.get("table_properties"):
            hprops = ", ".join(f"{_q(k)} = {_q(v)}" for k, v in s_cfg["table_properties"].items())
            hist_ddl += f"\n      TBLPROPERTIES ({hprops})"
        spark.sql(hist_ddl)
        if cb_cols:  # history is the volume driver - clustering its keys keeps the close-MERGE and
            # NOT EXISTS anti-join pruned to a few files instead of scanning the whole history
            spark.sql(f"ALTER TABLE {hist_tbl} CLUSTER BY ({cb_cols})")
    qcols = ", ".join(f"`{c['name']}` STRING" for c in cfg["columns"])
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {CAT}.quarantine.{src} (
      {qcols}, _rescued_data STRING, _source_file STRING, _ingest_ts TIMESTAMP,
      _batch_id STRING, _failed_rules ARRAY<STRING>, _quarantine_ts TIMESTAMP)""")
    df = (spark.table(f"{CAT}.bronze.{src}").where(F.col("_ingest_ts") > F.lit(get_watermark(src)))
          .withColumn("_failed_rules",
              F.array_compact(F.array(*[F.when(~rule_expr(r), F.lit(r["name"])) for r in cfg["quality_rules"]]))))
    # single-pass fix: cache once — every action below (quar/updates/metrics/watermark)
    # reuses this instead of re-scanning bronze + re-evaluating all rules per action.
    df = df.cache()
    n_read = df.count()  # materializes the cache — the ONLY bronze scan of this stage
    if n_read == 0:
        df.unpersist()
        print(src, "skipped (watermark)"); return
    quar = (df.filter(F.size("_failed_rules") > 0).withColumn("_quarantine_ts", F.current_timestamp())
            .select(*[c["name"] for c in cfg["columns"]], "_rescued_data", "_source_file",
                    "_ingest_ts", "_batch_id", "_failed_rules", "_quarantine_ts"))
    n_quar = quar.count()  # single-pass fix: count BEFORE the write (on the cached df)
    quar.write.format("delta").mode("append").saveAsTable(f"{CAT}.quarantine.{src}")
    # Dedup ordering: _ingest_ts can tie when two versions of a key arrive in one microbatch
    # (current_timestamp() is pinned per microbatch) — _file_mtime is the deterministic tiebreak.
    w = Window.partitionBy(*[F.col(k) for k in keys]).orderBy(F.col("_ingest_ts").desc(), F.col("_file_mtime").desc())
    updates = (df.filter(F.size("_failed_rules") == 0)
               .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
               .withColumn("_merged_ts", F.current_timestamp()))
    # single-pass fix: materialize the dedup window ONCE — the count and all MERGEs below
    # (SCD2 close, SCD2 insert, SCD1 merge) read from this cache instead of re-running
    # the window + rule evaluation per action.
    updates = updates.cache()
    n_merge = updates.count()
    updates.select(*[F.expr(col_cast(c)) for c in kept], *transform_selects(cfg), "_ingest_ts", "_merged_ts", "_row_hash").createOrReplaceTempView(f"silver_updates_{src}")
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in silver_merge_keys(cfg))
    # --- merge strategy (YAML): silver.merge_strategy scd1 (default) | scd2 | scd1+scd2 ---
    # (strategy + hist_tbl computed above alongside s_cfg)
    if strategy in ("scd2", "scd1+scd2"):
        # (history table + clustering ensured above, before the watermark-skip early return)
        hon = " AND ".join(f"h.`{k}` = s.`{k}`" for k in silver_merge_keys(cfg))
        # 1) close superseded versions: hash changed -> old current version ends when the new row arrived
        spark.sql(f"""MERGE INTO {hist_tbl} t USING silver_updates_{src} s ON {on}
          WHEN MATCHED AND t._is_current AND t._row_hash <> s._row_hash
          THEN UPDATE SET t._effective_to = s._ingest_ts, t._is_current = false""")
        # 2) append new versions only for changed/new keys (identical hash = no new version - same guard as SCD1)
        hcols_in = ", ".join([f"`{c['name']}`" for c in kept] + [f"`{t['column']}`" for t in silver_transformations(cfg)] + ["_ingest_ts", "_row_hash"])
        hsel = ", ".join([f"s.`{c['name']}`" for c in kept] + [f"s.`{t['column']}`" for t in silver_transformations(cfg)] + ["s._ingest_ts", "s._row_hash"])
        spark.sql(f"""INSERT INTO {hist_tbl} ({hcols_in}, _effective_from, _effective_to, _is_current)
          SELECT {hsel}, s._ingest_ts, CAST(NULL AS TIMESTAMP), true
          FROM silver_updates_{src} s
          WHERE NOT EXISTS (SELECT 1 FROM {hist_tbl} h WHERE {hon} AND h._is_current AND h._row_hash = s._row_hash)""")
    if strategy != "scd2":  # scd1 (default) and scd1+scd2 keep the physical current-state table
        spark.sql(f"""MERGE INTO {silver_target(cfg)} t USING silver_updates_{src} s ON {on}
          WHEN MATCHED AND t._row_hash <> s._row_hash THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    else:  # scd2-only: current state is a VIEW over history (single source of truth, no dual-write drift)
        cur_cols = ", ".join([f"`{c['name']}`" for c in kept] + [f"`{t['column']}`" for t in silver_transformations(cfg)])
        spark.sql(f"""CREATE OR REPLACE VIEW {silver_target(cfg)} AS
          SELECT {cur_cols}, _ingest_ts, _row_hash FROM {hist_tbl} WHERE _is_current""")
    # single-pass fix: ONE aggregation computes passed/failed for ALL rules from the cached
    # df (was: two full scans + rule evaluations PER rule). Semantics preserved exactly:
    # passed = sum(rule TRUE), failed = sum(rule FALSE) — NULL evals count in neither.
    if cfg["quality_rules"]:
        agg_row = df.agg(*[x for i, r in enumerate(cfg["quality_rules"])
                           for x in (F.sum(F.when(rule_expr(r), 1).otherwise(0)).alias(f"p{i}"),
                                     F.sum(F.when(~rule_expr(r), 1).otherwise(0)).alias(f"f{i}"))]).collect()[0]
        for i, r in enumerate(cfg["quality_rules"]):
            spark.sql(f"""INSERT INTO {CAT}.audit.dq_metrics VALUES
              ('{run_id}', '{src}', '{r['name']}', {agg_row[f"p{i}"] or 0}, {agg_row[f"f{i}"] or 0}, current_timestamp())""")
    max_ing = df.agg(F.max("_ingest_ts")).collect()[0][0]
    spark.sql(f"""MERGE INTO {CAT}.audit.silver_watermarks t
      USING (SELECT '{src}' s, timestamp'{max_ing}' w) u ON t.source_name = u.s
      WHEN MATCHED THEN UPDATE SET last_processed_ingest_ts = greatest(t.last_processed_ingest_ts, u.w)
      WHEN NOT MATCHED THEN INSERT (source_name, last_processed_ingest_ts) VALUES (u.s, u.w)""")
    spark.sql(f"""INSERT INTO {CAT}.audit.run_log VALUES
      ('{run_id}', '{src}', 'silver', 'success', {n_read}, {n_merge}, NULL, {n_quar}, {n_merge},
       NULL, timestamp'{start_ts}', current_timestamp())""")
    df.unpersist()       # single-pass hygiene: release the cached stage data
    updates.unpersist()
    print({"source": src, "strategy": strategy, "read": n_read, "quarantined": n_quar, "merged": n_merge})

for cfg in get_active_configs():
    process_silver(cfg, run_id)

# COMMAND ----------

# DBTITLE 1,Stage 4 — PII Governance (tags + masking)
# PII governance (mask functions, silver masks/tags incl. SCD2 history, bronze tags) moved to the
# "MDD Config Loader" notebook/job — triggered on file arrival in the config volume, since it only
# needs to run when configs or table schemas change, not on every pipeline run.

# COMMAND ----------

# DBTITLE 1,Stage 5 — Observability Rollup
spark.sql(f"""
WITH b AS (SELECT source_name, sum(rows_read) bronze_rows, sum(rows_rescued) rescued
           FROM {CAT}.audit.run_log WHERE layer='bronze' GROUP BY 1),
     s AS (SELECT source_name, sum(rows_quarantined) quarantined, sum(rows_merged) silver_rows
           FROM {CAT}.audit.run_log WHERE layer='silver' GROUP BY 1)
SELECT b.source_name, b.bronze_rows, b.rescued, s.quarantined, s.silver_rows
FROM b LEFT JOIN s ON b.source_name=s.source_name ORDER BY 1""").display()

spark.sql(f"""SELECT source_name, rule_name, sum(failed_rows) failed
  FROM {CAT}.audit.dq_metrics WHERE failed_rows > 0 GROUP BY 1,2 ORDER BY 1,2""").display()

# COMMAND ----------

# DBTITLE 1,Operations Runbook
# MAGIC %md
# MAGIC ## Operations
# MAGIC * **Add a new source:** drop `*.yaml` into the config volume + a source folder in the landing volume, then run the Config Loader job (new sources start active) — the next pipeline run picks them up. No code changes.
# MAGIC * **Incremental processing:** Auto Loader checkpoints (bronze) and `audit.silver_watermarks` (silver) make every run incremental and idempotent — safe to re-run.
# MAGIC * **Data quality:** rules live in each YAML; failing rows land in `mdd_platform.quarantine.<source>` with `_failed_rules`; metrics in `audit.dq_metrics`.
# MAGIC * **Rescue:** schema-drifted columns land in bronze `_rescued_data` (JSON with column names + file path) — never lost, review and fold into the YAML schema when a drift is legitimate.
# MAGIC * **PII:** declared in `pii_columns` in YAML → column tags + masking functions applied at silver. Governed tag policy on the `pii` key restricts values to email/phone/etc.
# MAGIC * **Schedule:** this notebook runs as the hourly task of the "MDD Pipeline Orchestration Job" — everything is `availableNow`/watermark-based so runs drain only new data.
# MAGIC * **Config reloads preserve `active`:** the Config Loader's `load_configs()` refreshes config content but never flips the `active` flag on reload — deactivate a source via the control table (`UPDATE mdd_platform.config.pipeline_configs SET active=false WHERE source_name=...`), not by removing its YAML. Genuinely new YAMLs still start active on first load.
# MAGIC * **Dedup tiebreak:** silver dedup orders by `_ingest_ts`, then `_file_mtime`. `current_timestamp()` is pinned per microbatch, so two versions of the same business key arriving in one Auto Loader microbatch tie on `_ingest_ts`; the newer file wins deterministically via `_file_mtime` (captured from `_metadata.file_modification_time` at bronze ingestion).

# COMMAND ----------

