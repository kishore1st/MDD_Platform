# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Documentation — costing model
# MAGIC %md
# MAGIC # MDD Ops Costing Views — File Lifecycle Costing Layer
# MAGIC
# MAGIC Creates four costing views in `mdd_platform.ops` for the **MDD Platform — Ops Dashboard** (File Lifecycle Costing page):
# MAGIC
# MAGIC | View | Grain |
# MAGIC |---|---|
# MAGIC | `v_storage_footprint` | one row per managed table (bronze / silver / quarantine per source) + raw landing files |
# MAGIC | `v_file_lifecycle_cost` | one row per source file per bronze microbatch — full lifecycle time, rows, bytes, DBU cost |
# MAGIC | `v_stage_cost_summary` | one row per source × stage (bronze / dq / silver) — durations, allocated cost, cost per 1k rows |
# MAGIC | `v_lifecycle_cost_kpis` | one row per source + grand total (`ALL_SOURCES`) — cost KPIs, storage, memory-hours |
# MAGIC
# MAGIC **Derivation model** (per-stage compute is not instrumented, so stages are wall-clock spans between observed lifecycle events):
# MAGIC
# MAGIC * **arrival → bronze**: `_ingest_ts − _file_mtime` (exact; file name/size joined from the landing volume via `read_files`, matched on file mtime)
# MAGIC * **DQ evaluation**: quarantine write `_quarantine_ts − _ingest_ts` (upper bound of DQ time); when a file produced no quarantined rows the ingest→merge window is split 50/50 between DQ and silver merge (documented assumption)
# MAGIC * **silver merge**: first `_merged_ts − _quarantine_ts`; **total lifecycle**: first `_merged_ts − _file_mtime` (end-to-end)
# MAGIC * **Compute time for costing**: bronze/silver-layer durations from `mdd_platform.audit.run_log`, allocated to files pro-rata by row share within each run
# MAGIC
# MAGIC **Cost model**: DBU cost from `system.billing.usage` for the orchestration job (**resolved by name** — "MDD Pipeline Orchestration Job" — so it survives job recreation and workspace promotion) once telemetry lands (~1 day lag), allocated pro-rata by raw rows; until then **estimated** = `active_compute_seconds / 3600 × 1.0 DBU per active compute-hour (assumed single-worker serverless execution) × serverless jobs DBU list price` (`system.billing.list_prices`). `cost_source` marks which basis was used.
# MAGIC
# MAGIC **Storage model**: raw bytes exact from the landing volume; table bytes/file counts from `system.storage.table_metrics_history` (exact once the daily snapshot includes the tables) falling back to documented estimates (`row_count × raw bytes-per-row`; file count = distinct write events). **Memory**: `estimated_memory_gb_hours = 8 GB assumed worker × active compute hours` (serverless workers are not directly observable — planning assumption, not a metered value).
# MAGIC
# MAGIC **Regeneration**: all views are control-table-driven over `mdd_platform.config.pipeline_configs` — re-run these cells after adding new sources and they appear automatically (same convention as the existing union views).

# COMMAND ----------

# DBTITLE 1,Build ops.v_storage_footprint
# Build ops.v_storage_footprint — one row per managed table (bronze/silver/quarantine per source) + raw landing files.
# Control-table driven: iterates ACTIVE sources from mdd_platform.config.pipeline_configs (same convention as
# v_freshness_sla / v_rescued_data). Re-run after adding sources so new tables appear automatically.
LANDING_ROOT = "/Volumes/mdd_platform/landing/source_files"
SOURCES = [r.source_name for r in spark.sql(
    "SELECT source_name FROM mdd_platform.config.pipeline_configs WHERE active ORDER BY source_name").collect()]

WRITE_TS = {"bronze": "_ingest_ts", "silver": "_merged_ts", "quarantine": "_quarantine_ts"}
table_rows, landing_rows = [], []
for s in SOURCES:
    for layer, ts_col in WRITE_TS.items():
        table_rows.append(f"""
SELECT '{s}' AS source_name, '{layer}' AS layer, 'mdd_platform.{layer}.{s}' AS table_name,
       count(*) AS row_count, count(DISTINCT {ts_col}) AS est_write_events
FROM mdd_platform.{layer}.{s}""")
    landing_rows.append(f"""
SELECT '{s}' AS source_name, count(*) AS file_count, sum(file_bytes) AS bytes, sum(rws) AS rws
FROM (SELECT _metadata.file_name, max(_metadata.file_size) AS file_bytes, count(*) AS rws
      FROM read_files('{LANDING_ROOT}/{s}/', format => 'csv') GROUP BY 1)""")

ddl = f"""
CREATE OR REPLACE VIEW mdd_platform.ops.v_storage_footprint
COMMENT 'Storage footprint of every managed MDD table (bronze/silver/quarantine per source) plus raw landing files. Built dynamically over mdd_platform.config.pipeline_configs - re-run the generator cell after adding sources (same convention as the other union views). row_count is exact (count(*)). bytes_on_disk and file_count are EXACT from system.storage.table_metrics_history (latest daily snapshot; newly created tables first appear there the day after creation) falling back to ESTIMATES until telemetry lands: bytes estimated as row_count x raw bytes-per-row measured from the source landing CSVs (documented approximation, plus/minus 2x vs Parquet on disk); file_count estimated as the number of distinct write events (microbatch/merge timestamps). bytes_per_row = bytes_on_disk / row_count.'
AS
WITH metrics AS (
  SELECT schema_name, table_name, active_bytes, active_files,
         row_number() OVER (PARTITION BY schema_name, table_name ORDER BY snapshot_date DESC) AS rn
  FROM system.storage.table_metrics_history WHERE catalog_name = 'mdd_platform'
),
landing_stats AS (
  {' UNION ALL '.join(landing_rows)}
),
table_stats AS (
  {' UNION ALL '.join(table_rows)}
),
managed AS (
  SELECT t.source_name, t.layer, t.table_name, t.row_count,
         coalesce(m.active_files, t.est_write_events) AS file_count,
         coalesce(m.active_bytes, cast(round(t.row_count * greatest(ls.bytes / greatest(ls.rws, 1), 1), 0) AS bigint)) AS bytes_on_disk
  FROM table_stats t
  JOIN landing_stats ls ON ls.source_name = t.source_name
  LEFT JOIN metrics m ON m.table_name = t.source_name
        AND m.schema_name = CASE t.layer WHEN 'silver' THEN 'silver' WHEN 'quarantine' THEN 'quarantine' ELSE 'bronze' END
        AND m.rn = 1
)
SELECT source_name, layer, table_name, row_count, file_count, bytes_on_disk,
       round(bytes_on_disk / greatest(row_count, 1), 2) AS bytes_per_row
FROM managed
UNION ALL
SELECT ls.source_name, 'landing', '{LANDING_ROOT}/' || ls.source_name, ls.rws, ls.file_count, ls.bytes,
       round(ls.bytes / greatest(ls.rws, 1), 2)
FROM landing_stats ls
"""
spark.sql(ddl)
print(f"v_storage_footprint created over {len(SOURCES)} active sources ({len(SOURCES) * 3} managed tables + {len(SOURCES)} landing rows)")
spark.sql("SELECT * FROM mdd_platform.ops.v_storage_footprint ORDER BY source_name, layer").show(6, truncate=45)

# COMMAND ----------

# DBTITLE 1,Build ops.v_file_lifecycle_cost
# Build ops.v_file_lifecycle_cost — one row per source file per bronze microbatch: full lifecycle
# time / rows / storage bytes / allocated DBU cost. See the documentation cell for the derivation model.
# Grain: bronze (_batch_id, _ingest_ts, _file_mtime) uniquely identifies a file; exact file name/size is
# joined from the landing volume (read_files) by matching _file_mtime. Stage timings are wall-clock spans
# between observed lifecycle events; compute time for COSTING uses audit.run_log run durations pro-rated by rows.
LANDING_ROOT = "/Volumes/mdd_platform/landing/source_files"
from databricks.sdk import WorkspaceClient
JOB_ID = str(next(iter(WorkspaceClient().jobs.list(name="MDD Pipeline Orchestration Job"))).job_id)  # name-based: survives job recreation + workspace promotion (ids differ per workspace)
# NOTE: the nightly "MDD Ops Views Refresh Job" regenerates v_file_lifecycle_cost via the dedicated
# builder notebook (exact file attribution via bronze._source_file). Prefer that path for routine
# regeneration; this cell is the original authoring version kept for reference.
SOURCES = [r.source_name for r in spark.sql(
    "SELECT source_name FROM mdd_platform.config.pipeline_configs WHERE active ORDER BY source_name").collect()]

landing_union = " UNION ALL ".join(f"""
SELECT '{s}' AS source_name, _metadata.file_name AS file_name,
       cast(max(_metadata.file_modification_time) AS timestamp) AS file_mtime,
       max(_metadata.file_size) AS raw_file_bytes
FROM read_files('{LANDING_ROOT}/{s}/', format => 'csv') GROUP BY 1, 2""" for s in SOURCES)
bronze_union = " UNION ALL ".join(f"""
SELECT '{s}' AS source_name, cast(_batch_id AS bigint) AS batch_id, _ingest_ts, _file_mtime,
       count(*) AS raw_rows, count(_rescued_data) AS rescued_rows
FROM mdd_platform.bronze.{s} GROUP BY 1, 2, 3, 4""" for s in SOURCES)
quarantine_union = " UNION ALL ".join(f"""
SELECT '{s}' AS source_name, cast(_batch_id AS bigint) AS batch_id, _ingest_ts,
       min(_quarantine_ts) AS dq_ts, count(*) AS quarantined_rows
FROM mdd_platform.quarantine.{s} GROUP BY 1, 2, 3""" for s in SOURCES)
silver_union = " UNION ALL ".join(f"""
SELECT '{s}' AS source_name, _ingest_ts, count(*) AS silver_rows, min(_merged_ts) AS first_merge_ts
FROM mdd_platform.silver.{s} GROUP BY 1, 2""" for s in SOURCES)

ddl = f"""
CREATE OR REPLACE VIEW mdd_platform.ops.v_file_lifecycle_cost
COMMENT 'Per-file, per-microbatch lifecycle costing. DERIVATION (per-stage compute is not instrumented): stage durations are wall-clock spans between observed lifecycle events. arrival_to_bronze_seconds = bronze _ingest_ts - file mtime (exact). dq_seconds = quarantine write ts - bronze ingest ts (upper bound of DQ evaluation; when a file produced no quarantined rows the ingest-to-merge window is split 50/50 between DQ and silver merge - documented assumption). silver_merge_seconds = first silver _merged_ts - quarantine write ts. total_lifecycle_seconds = first _merged_ts - file mtime (end-to-end). Compute time for costing uses audit.run_log bronze/silver-layer run durations allocated pro-rata by raw row share within each run (bronze_run_seconds, silver_run_seconds). COST PRO-RATION: DBU cost is taken from system.billing.usage for orchestration job {JOB_ID} when telemetry has landed (about 1 day lag) and allocated to files pro-rata by raw rows; until then it is ESTIMATED as active_compute_seconds / 3600 x 1.0 DBU per active compute-hour (assumed single-worker serverless jobs execution) x the serverless jobs DBU list price from system.billing.list_prices (cost_source marks the basis). STORAGE: raw_file_bytes exact from the landing volume; bronze/silver/quarantine bytes are pro-rata shares (by rows) of the table sizes in ops.v_storage_footprint (exact from system.storage.table_metrics_history once daily snapshots include the tables, estimated before that). Silver row attribution is pro-rata by raw rows when multiple files share one microbatch (documented).'
AS WITH
landing AS (
{landing_union}
),
bronze_files AS (
{bronze_union}
),
quarantine_batches AS (
{quarantine_union}
),
silver_batches AS (
{silver_union}
),
bronze_runs AS (
  SELECT run_id, source_name, start_ts, end_ts, coalesce(rows_read, 0) AS run_rows,
         greatest(unix_timestamp(end_ts) - unix_timestamp(start_ts), 0) AS run_seconds
  FROM mdd_platform.audit.run_log WHERE layer = 'bronze'
),
silver_runs AS (
  SELECT run_id, source_name, start_ts, coalesce(rows_merged, 0) AS run_rows,
         greatest(unix_timestamp(end_ts) - unix_timestamp(start_ts), 0) AS run_seconds
  FROM mdd_platform.audit.run_log WHERE layer = 'silver'
),
files AS (
  SELECT b.source_name, l.file_name, b.batch_id, b._ingest_ts AS bronze_ingest_ts,
         l.file_mtime AS arrival_ts, l.raw_file_bytes,
         b.raw_rows, b.raw_rows AS bronze_rows, b.rescued_rows
  FROM bronze_files b
  JOIN landing l ON l.source_name = b.source_name AND l.file_mtime = b._file_mtime
),
batch_totals AS (
  SELECT source_name, bronze_ingest_ts, sum(raw_rows) AS batch_raw_rows
  FROM files GROUP BY 1, 2
),
f2 AS (
  SELECT fs.*, bt.batch_raw_rows, qb.dq_ts,
         round(coalesce(qb.quarantined_rows, 0) * fs.raw_rows / greatest(bt.batch_raw_rows, 1), 2) AS quarantined_rows,
         sb.silver_rows AS silver_batch_rows, sb.first_merge_ts,
         jr.run_id AS bronze_run_id, jr.start_ts AS bronze_run_ts,
         coalesce(jr.run_seconds, 0) AS bronze_run_seconds_total, coalesce(jr.run_rows, 0) AS bronze_run_rows
  FROM files fs
  JOIN batch_totals bt ON bt.source_name = fs.source_name AND bt.bronze_ingest_ts = fs.bronze_ingest_ts
  LEFT JOIN quarantine_batches qb ON qb.source_name = fs.source_name AND qb._ingest_ts = fs.bronze_ingest_ts
  LEFT JOIN silver_batches sb ON sb.source_name = fs.source_name AND sb._ingest_ts = fs.bronze_ingest_ts
  LEFT JOIN bronze_runs jr ON jr.source_name = fs.source_name AND fs.bronze_ingest_ts BETWEEN jr.start_ts AND jr.end_ts
),
f3 AS (
  SELECT f2.*, sr.run_seconds AS silver_run_seconds_total, sr.run_rows AS silver_run_rows,
         row_number() OVER (
           PARTITION BY f2.source_name, f2.file_name
           ORDER BY abs(unix_timestamp(sr.start_ts) - coalesce(unix_timestamp(f2.first_merge_ts), unix_timestamp(f2.bronze_ingest_ts)))
         ) AS rn
  FROM f2 LEFT JOIN silver_runs sr ON sr.source_name = f2.source_name
),
f4 AS (
  SELECT f3.*,
         greatest(unix_timestamp(f3.bronze_ingest_ts) - unix_timestamp(f3.arrival_ts), 0) AS arrival_to_bronze_seconds,
         round(coalesce(f3.bronze_run_seconds_total, 0) * f3.raw_rows / greatest(f3.bronze_run_rows, 1), 3) AS bronze_run_seconds,
         CASE WHEN f3.dq_ts IS NOT NULL THEN greatest(unix_timestamp(f3.dq_ts) - unix_timestamp(f3.bronze_ingest_ts), 0)
              ELSE greatest(unix_timestamp(f3.first_merge_ts) - unix_timestamp(f3.bronze_ingest_ts), 0) * 0.5 END AS dq_seconds,
         CASE WHEN f3.dq_ts IS NOT NULL THEN greatest(unix_timestamp(f3.first_merge_ts) - unix_timestamp(f3.dq_ts), 0)
              ELSE greatest(unix_timestamp(f3.first_merge_ts) - unix_timestamp(f3.bronze_ingest_ts), 0) * 0.5 END AS silver_merge_seconds,
         greatest(unix_timestamp(f3.first_merge_ts) - unix_timestamp(f3.arrival_ts), 0) AS total_lifecycle_seconds,
         round(coalesce(f3.silver_batch_rows, 0) * f3.raw_rows / greatest(f3.batch_raw_rows, 1), 1) AS silver_merged_rows,
         round(coalesce(f3.silver_run_seconds_total, 0) * (coalesce(f3.silver_batch_rows, 0) * f3.raw_rows / greatest(f3.batch_raw_rows, 1)) / greatest(f3.silver_run_rows, 1), 3) AS silver_run_seconds
  FROM f3 WHERE rn = 1
),
f5 AS (
  SELECT f4.*,
         fpb.bytes_on_disk * f4.raw_rows / greatest(fpb.row_count, 1) AS bronze_bytes_pro_rata,
         fps.bytes_on_disk * f4.silver_merged_rows / greatest(fps.row_count, 1) AS silver_bytes_pro_rata,
         fpq.bytes_on_disk * f4.quarantined_rows / greatest(fpq.row_count, 1) AS quarantine_bytes_pro_rata,
         coalesce(f4.bronze_run_seconds, 0) + coalesce(f4.silver_run_seconds, 0) AS active_compute_seconds
  FROM f4
  LEFT JOIN mdd_platform.ops.v_storage_footprint fpb ON fpb.source_name = f4.source_name AND fpb.layer = 'bronze'
  LEFT JOIN mdd_platform.ops.v_storage_footprint fps ON fps.source_name = f4.source_name AND fps.layer = 'silver'
  LEFT JOIN mdd_platform.ops.v_storage_footprint fpq ON fpq.source_name = f4.source_name AND fpq.layer = 'quarantine'
),
job_usage AS (
  SELECT sum(usage_quantity) AS actual_dbus
  FROM system.billing.usage WHERE usage_metadata.job_id = '{JOB_ID}'
),
price AS (
  SELECT min(pricing.default) AS price_per_dbu
  FROM system.billing.list_prices WHERE sku_name LIKE '%JOBS_SERVERLESS%' AND price_end_time IS NULL
),
grand AS (SELECT sum(raw_rows) AS total_raw_rows FROM f5)
SELECT
  f5.source_name, f5.file_name, f5.batch_id, f5.bronze_run_id AS run_id, f5.bronze_run_ts AS run_ts,
  f5.arrival_ts, f5.bronze_ingest_ts, f5.dq_ts, f5.first_merge_ts AS silver_merge_ts,
  cast(f5.arrival_to_bronze_seconds AS bigint) AS arrival_to_bronze_seconds,
  f5.bronze_run_seconds, round(f5.dq_seconds, 3) AS dq_seconds, round(f5.silver_merge_seconds, 3) AS silver_merge_seconds,
  f5.silver_run_seconds, cast(f5.total_lifecycle_seconds AS bigint) AS total_lifecycle_seconds,
  cast(f5.raw_rows AS bigint) AS raw_rows, cast(f5.bronze_rows AS bigint) AS bronze_rows,
  f5.quarantined_rows, cast(f5.rescued_rows AS bigint) AS rescued_rows, f5.silver_merged_rows,
  cast(f5.raw_file_bytes AS bigint) AS raw_file_bytes,
  cast(round(f5.bronze_bytes_pro_rata, 0) AS bigint) AS bronze_bytes_pro_rata,
  cast(round(f5.silver_bytes_pro_rata, 0) AS bigint) AS silver_bytes_pro_rata,
  cast(round(f5.quarantine_bytes_pro_rata, 0) AS bigint) AS quarantine_bytes_pro_rata,
  cast(round(f5.raw_file_bytes + coalesce(f5.bronze_bytes_pro_rata, 0) + coalesce(f5.silver_bytes_pro_rata, 0) + coalesce(f5.quarantine_bytes_pro_rata, 0), 0) AS bigint) AS total_storage_bytes,
  round(f5.active_compute_seconds, 3) AS active_compute_seconds,
  p.price_per_dbu,
  CASE WHEN ju.actual_dbus IS NOT NULL THEN round(ju.actual_dbus * f5.raw_rows / greatest(g.total_raw_rows, 1), 6)
       ELSE round(f5.active_compute_seconds / 3600.0, 8) END AS dbus_allocated,
  CASE WHEN ju.actual_dbus IS NOT NULL THEN round(ju.actual_dbus * p.price_per_dbu * f5.raw_rows / greatest(g.total_raw_rows, 1), 6)
       ELSE round(f5.active_compute_seconds / 3600.0 * p.price_per_dbu, 6) END AS cost_usd,
  CASE WHEN ju.actual_dbus IS NOT NULL THEN 'billing_usage' ELSE 'estimated' END AS cost_source,
  round((CASE WHEN ju.actual_dbus IS NOT NULL THEN ju.actual_dbus * p.price_per_dbu * f5.raw_rows / greatest(g.total_raw_rows, 1)
       ELSE f5.active_compute_seconds / 3600.0 * p.price_per_dbu END) / greatest(f5.raw_rows, 1), 8) AS cost_per_row,
  CASE WHEN ju.actual_dbus IS NOT NULL THEN round(ju.actual_dbus * p.price_per_dbu * f5.raw_rows / greatest(g.total_raw_rows, 1), 6)
       ELSE round(f5.active_compute_seconds / 3600.0 * p.price_per_dbu, 6) END AS cost_per_file,
  round(f5.total_lifecycle_seconds / greatest(f5.raw_rows, 1), 4) AS seconds_per_row
FROM f5
CROSS JOIN job_usage ju
CROSS JOIN price p
CROSS JOIN grand g
"""
spark.sql(ddl)
print("v_file_lifecycle_cost created")
spark.sql("SELECT source_name, file_name, batch_id, run_id, arrival_to_bronze_seconds, bronze_run_seconds, dq_seconds, silver_merge_seconds, total_lifecycle_seconds, raw_rows, quarantined_rows, rescued_rows, silver_merged_rows, cost_usd, cost_source FROM mdd_platform.ops.v_file_lifecycle_cost ORDER BY source_name, file_name").show(6, truncate=16)

# COMMAND ----------

# DBTITLE 1,Build ops.v_stage_cost_summary
# Build ops.v_stage_cost_summary — one row per source x stage (bronze / dq / silver).
# Built from ops.v_file_lifecycle_cost: stage durations use that view's per-file stage seconds
# (bronze = bronze-layer run_log compute duration pro-rated by rows; dq / silver_merge = observed
# wall-clock spans; see v_file_lifecycle_cost comments). Allocated DBU cost per stage = per-file cost
# allocated pro-rata by the stage's share of (bronze + dq + silver) stage time.
spark.sql("""
CREATE OR REPLACE VIEW mdd_platform.ops.v_stage_cost_summary
COMMENT 'Cost summary per source x pipeline stage (bronze ingest / data quality / silver merge). Built from ops.v_file_lifecycle_cost: stage durations are that view's per-file stage seconds (bronze = bronze-layer run_log compute duration pro-rated by row share; dq and silver = observed wall-clock spans between lifecycle events, upper bounds of stage compute since per-stage compute is not instrumented). total_cost_usd = per-file allocated DBU cost (see v_file_lifecycle_cost cost model) allocated to the stage pro-rata by stage seconds share of total (bronze+dq+silver) stage time. cost_per_1k_rows = stage cost per 1,000 rows processed. avg_stage_latency_seconds = average per-file stage duration.'
AS
WITH stages AS (
  SELECT source_name, 'bronze' AS layer, bronze_run_seconds AS stage_seconds, raw_rows AS rows_processed
  FROM mdd_platform.ops.v_file_lifecycle_cost
  UNION ALL
  SELECT source_name, 'dq' AS layer, dq_seconds AS stage_seconds, raw_rows
  FROM mdd_platform.ops.v_file_lifecycle_cost
  UNION ALL
  SELECT source_name, 'silver' AS layer, silver_merge_seconds AS stage_seconds, silver_merged_rows
  FROM mdd_platform.ops.v_file_lifecycle_cost
),
file_cost AS (
  SELECT source_name,
         sum(cost_usd) AS total_cost,
         sum(coalesce(bronze_run_seconds, 0) + coalesce(dq_seconds, 0) + coalesce(silver_merge_seconds, 0)) AS total_stage_seconds
  FROM mdd_platform.ops.v_file_lifecycle_cost GROUP BY 1
)
SELECT s.source_name, s.layer,
       round(sum(s.stage_seconds), 1) AS total_duration_seconds,
       round(sum(coalesce(s.stage_seconds, 0)) / greatest(fc.total_stage_seconds, 0.001) * fc.total_cost, 6) AS total_cost_usd,
       cast(sum(s.rows_processed) AS bigint) AS rows_processed,
       round(1000 * sum(coalesce(s.stage_seconds, 0)) / greatest(fc.total_stage_seconds, 0.001) * fc.total_cost
             / greatest(sum(s.rows_processed), 1), 6) AS cost_per_1k_rows,
       round(avg(s.stage_seconds), 1) AS avg_stage_latency_seconds
FROM stages s JOIN file_cost fc USING (source_name)
GROUP BY s.source_name, s.layer, fc.total_cost, fc.total_stage_seconds
""")
print("v_stage_cost_summary created")
spark.sql("SELECT * FROM mdd_platform.ops.v_stage_cost_summary WHERE source_name = 'src_001_customers'").show()

# COMMAND ----------

# DBTITLE 1,Build ops.v_lifecycle_cost_kpis
# Build ops.v_lifecycle_cost_kpis — one row per source plus a grand-total row (source_name = 'ALL_SOURCES').
# estimated_memory_gb_hours = 8 GB assumed worker memory x active compute hours (bronze+silver run
# durations). total_storage_bytes = raw landing + bronze + silver + quarantine bytes from v_storage_footprint.
spark.sql("""
CREATE OR REPLACE VIEW mdd_platform.ops.v_lifecycle_cost_kpis
COMMENT 'Cost and capacity KPIs per source plus grand total (source_name = ALL_SOURCES). total_estimated_cost_usd = sum of per-file allocated DBU cost (billing-based once system.billing.usage rows for the orchestration job (job id resolved by name in v_file_lifecycle_cost) land, about 1 day lag; estimated from active compute time x DBU list price before that - see v_file_lifecycle_cost). cost_per_mb_usd is over raw landing MB. total_storage_bytes = raw landing + bronze + silver + quarantine table bytes from v_storage_footprint (exact from system.storage.table_metrics_history once daily snapshots include the tables, estimated before). estimated_memory_gb_hours = assumed 8 GB worker memory x active compute hours (bronze+silver run durations) - serverless jobs workers are not directly observable, so this is a documented planning assumption, not a metered value.'
AS
WITH f AS (SELECT * FROM mdd_platform.ops.v_file_lifecycle_cost),
storage AS (
  SELECT source_name, sum(bytes_on_disk) AS total_storage_bytes
  FROM mdd_platform.ops.v_storage_footprint GROUP BY 1
),
per_source AS (
  SELECT f.source_name,
         sum(f.cost_usd) AS total_cost,
         count(*) AS files,
         sum(f.raw_rows) AS raw_rows,
         sum(f.raw_file_bytes) AS raw_bytes,
         avg(f.total_lifecycle_seconds) AS avg_life,
         sum(f.active_compute_seconds) AS active_seconds,
         max(s.total_storage_bytes) AS storage_bytes
  FROM f JOIN storage s USING (source_name)
  GROUP BY 1
)
SELECT source_name,
       round(total_cost, 4) AS total_estimated_cost_usd,
       round(total_cost / greatest(files, 1), 4) AS cost_per_file_usd,
       round(total_cost / greatest(raw_rows, 1), 8) AS cost_per_row_usd,
       round(total_cost / greatest(raw_bytes / 1048576.0, 0.000001), 8) AS cost_per_mb_usd,
       round(avg_life, 1) AS avg_lifecycle_seconds_per_file,
       cast(storage_bytes AS bigint) AS total_storage_bytes,
       round(8.0 * active_seconds / 3600.0, 4) AS estimated_memory_gb_hours,
       cast(files AS bigint) AS files_processed,
       cast(raw_rows AS bigint) AS total_rows_processed
FROM per_source
UNION ALL
SELECT 'ALL_SOURCES',
       round(sum(total_cost), 4),
       round(sum(total_cost) / greatest(sum(files), 1), 4),
       round(sum(total_cost) / greatest(sum(raw_rows), 1), 8),
       round(sum(total_cost) / greatest(sum(raw_bytes) / 1048576.0, 0.000001), 8),
       round(sum(avg_life * files) / greatest(sum(files), 1), 1),
       cast(sum(storage_bytes) AS bigint),
       round(8.0 * sum(active_seconds) / 3600.0, 4),
       cast(sum(files) AS bigint),
       cast(sum(raw_rows) AS bigint)
FROM per_source
""")
print("v_lifecycle_cost_kpis created")
spark.sql("SELECT * FROM mdd_platform.ops.v_lifecycle_cost_kpis ORDER BY source_name").show(12, truncate=20)

# COMMAND ----------

# DBTITLE 1,Validate all four costing views
# Validate all four costing views: non-empty results, reconciliations, hand spot-check,
# estimate accuracy vs DESCRIBE DETAIL, and final schemas (for dashboard widget binding).
# Each view is evaluated ONCE and cached, then all checks run against the cached results
# (the views re-scan the landing volume via read_files, so we avoid repeated evaluations).
flc = spark.sql("SELECT * FROM mdd_platform.ops.v_file_lifecycle_cost").cache()
stg = spark.sql("SELECT * FROM mdd_platform.ops.v_stage_cost_summary").cache()
fp = spark.sql("SELECT * FROM mdd_platform.ops.v_storage_footprint").cache()
kpi = spark.sql("SELECT * FROM mdd_platform.ops.v_lifecycle_cost_kpis").cache()
counts = {v: df.count() for v, df in
          [("v_file_lifecycle_cost", flc), ("v_stage_cost_summary", stg),
           ("v_storage_footprint", fp), ("v_lifecycle_cost_kpis", kpi)]}
flc.createOrReplaceTempView("flc"); stg.createOrReplaceTempView("stg")
fp.createOrReplaceTempView("fp"); kpi.createOrReplaceTempView("kpi")

print("== 1. Row counts (expect 20 files / 30 stage rows / 40 footprint rows / 11 KPI rows) ==")
for v, n in counts.items():
    print(f"   {v}: {n} rows")

print("\n== 2. Reconciliation: grand total cost == per-file sum == per-source KPI sum == stage-cost sum ==")
spark.sql("""
SELECT (SELECT round(sum(cost_usd), 6) FROM flc) AS file_cost_sum,
       (SELECT total_estimated_cost_usd FROM kpi WHERE source_name = 'ALL_SOURCES') AS kpi_grand_total,
       (SELECT round(sum(total_estimated_cost_usd), 6) FROM kpi WHERE source_name != 'ALL_SOURCES') AS kpi_source_sum,
       (SELECT round(sum(total_cost_usd), 6) FROM stg) AS stage_cost_sum,
       (SELECT sum(raw_rows) FROM flc) AS total_raw_rows
""").show()

print("\n== 3. Storage reconciliation: KPI ALL_SOURCES storage == footprint total bytes ==")
spark.sql("""
SELECT (SELECT total_storage_bytes FROM kpi WHERE source_name = 'ALL_SOURCES') AS kpi_storage_bytes,
       (SELECT sum(bytes_on_disk) FROM fp) AS footprint_bytes
""").show()

print("\n== 4. Hand spot-check: src_001_customers / batch_002.csv (drift file) ==")
spark.sql("""
SELECT source_name, file_name, batch_id, run_id, arrival_ts, bronze_ingest_ts, dq_ts, silver_merge_ts,
       arrival_to_bronze_seconds, bronze_run_seconds, dq_seconds, silver_merge_seconds, total_lifecycle_seconds,
       raw_rows, bronze_rows, quarantined_rows, rescued_rows, silver_merged_rows,
       raw_file_bytes, total_storage_bytes, active_compute_seconds, price_per_dbu,
       dbus_allocated, cost_usd, cost_source, cost_per_row, cost_per_file, seconds_per_row
FROM flc WHERE source_name = 'src_001_customers' AND file_name = 'batch_002.csv'
""").show(truncate=15)
print("   Independent cross-check from the landing volume (expect 31 rows / 3220 bytes):")
spark.sql("""
SELECT count(*) AS rows, max(_metadata.file_size) AS bytes
FROM read_files('/Volumes/mdd_platform/landing/source_files/src_001_customers/', format => 'csv')
WHERE _metadata.file_name = 'batch_002.csv'
""").show()

print("\n== 5. Estimate accuracy: v_storage_footprint vs DESCRIBE DETAIL (bronze src_001) ==")
try:
    d = spark.sql("DESCRIBE DETAIL mdd_platform.bronze.src_001_customers").collect()[0]
    est = spark.sql("SELECT row_count, file_count, bytes_on_disk FROM fp WHERE table_name = 'mdd_platform.bronze.src_001_customers'").collect()[0]
    print(f"   actual: sizeInBytes={d['sizeInBytes']:,} numFiles={d['numFiles']} | view: row_count={est['row_count']} file_count={est['file_count']} bytes_on_disk={est['bytes_on_disk']:,}")
    print(f"   estimate/actual ratio = {est['bytes_on_disk'] / max(d['sizeInBytes'], 1):.2f}x")
except Exception as e:
    print("   DESCRIBE DETAIL unavailable:", str(e)[:200])

print("\n== 6. Final schemas (column names & types for dashboard binding) ==")
for v in ["v_file_lifecycle_cost", "v_stage_cost_summary", "v_storage_footprint", "v_lifecycle_cost_kpis"]:
    print(f"--- mdd_platform.ops.{v} ---")
    for r in spark.sql(f"DESCRIBE mdd_platform.ops.{v}").collect():
        if not str(r[0]).startswith("#"):
            print(f"   {r[0]:34} {r[1]}")