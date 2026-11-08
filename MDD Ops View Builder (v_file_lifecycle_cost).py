# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # MDD Ops View Builder: v_file_lifecycle_cost
# MAGIC
# MAGIC **Purpose:** (Re)generates `mdd_platform.ops.v_file_lifecycle_cost` — per-file, per-microbatch lifecycle costing — from the **active sources in `mdd_platform.config.pipeline_configs`**. No source names are hard-coded: when a new source is onboarded (config loaded + first pipeline run creates bronze/silver/quarantine tables), re-running this notebook includes it automatically.
# MAGIC
# MAGIC **When to run:** after onboarding any new source, or after changing bronze/silver/quarantine target tables, or if the orchestration job ID changes.
# MAGIC
# MAGIC **How it scales:**
# MAGIC - Landing inventory comes from a single `read_files` scan of the landing volume root — new source folders are discovered automatically.
# MAGIC - Bronze/silver/quarantine UNION blocks are generated from `pipeline_configs` (respects custom `bronze.target_table` / `silver.target_table`, including SCD2 `_history` handling) with an existence guard that skips sources whose tables don't exist yet.
# MAGIC - Storage sizes come from `mdd_platform.ops.v_storage_footprint` (**build order: run the *MDD Ops View Builder (v_storage_footprint)* notebook first** — this view depends on it); costs from `system.billing.usage` (orchestration job) with `system.billing.list_prices` fallback estimation.
# MAGIC
# MAGIC Run cells top-to-bottom.

# COMMAND ----------

# DBTITLE 1,Build view from active configs
# Parameters — adjust only these
dbutils.widgets.text("catalog_name", "mdd_platform", "MDD catalog (bundle passes ${var.catalog_name})")
CAT = dbutils.widgets.get("catalog_name").strip() or 'mdd_platform'  # MDD platform catalog
VIEW_FQN = f'{CAT}.ops.v_file_lifecycle_cost'      # view to (re)generate
# Orchestration job id resolved by NAME at runtime — never hardcode workspace-specific ids.
from databricks.sdk import WorkspaceClient as _WC
ORCH_JOB_ID = str(next((j.job_id for j in _WC().jobs.list()
                        if j.settings and j.settings.name == "MDD Pipeline Orchestration Job"), ""))
assert ORCH_JOB_ID, "Orchestration job not found in this workspace — deploy the bundle first."
STORAGE_VIEW = f'{CAT}.ops.v_storage_footprint'   # storage sizes source; BUILD THIS FIRST (its own builder notebook)
LANDING_ROOT = f'/Volumes/{CAT}/landing/source_files/'

import yaml

# ---- 1) active sources + declared layer targets from the control table ----
def norm(t, schema):
    """Normalize a target_table config value to a 3-part name."""
    parts = t.split('.')
    if len(parts) == 3: return t
    if len(parts) == 2: return f'{CAT}.{parts[0]}.{parts[1]}'
    return f'{CAT}.{schema}.{parts[0]}'

cfgs = []
for r in spark.sql(f"SELECT source_name, config, active FROM {CAT}.config.pipeline_configs ORDER BY source_name").collect():
    if not r.active:
        print(f"SKIP (inactive): {r.source_name}")
        continue
    cfg = yaml.safe_load(r.config) or {}
    cfgs.append({
        'source_name': r.source_name,
        'bronze_t': norm((cfg.get('bronze') or {}).get('target_table') or r.source_name, 'bronze'),
        'silver_t': norm((cfg.get('silver') or {}).get('target_table') or r.source_name, 'silver'),
        'quar_t':   norm((cfg.get('quarantine') or {}).get('target_table') or r.source_name, 'quarantine'),
    })

# ---- 2) existence guard: include a source only once its layer tables exist ----
existing = {(r.table_schema, r.table_name) for r in spark.sql(
    f"SELECT table_schema, table_name FROM {CAT}.information_schema.tables WHERE table_schema IN ('bronze','silver','quarantine')"
).collect()}

def exists(t):
    _, sch, tbl = t.split('.')
    return (sch, tbl) in existing

usable = []
for c in cfgs:
    missing = [t for t in (c['bronze_t'], c['silver_t'], c['quar_t']) if not exists(t)]
    if missing:
        print(f"SKIP (tables not yet created — run the pipeline first): {c['source_name']} -> {missing}")
    else:
        usable.append(c)
print(f"Sources included in view: {[c['source_name'] for c in usable]}")

# ---- 3) generated UNION ALL blocks ----
def blocks(field_sql, layer_key):
    return "\nUNION ALL\n".join(
        f"SELECT '{c['source_name']}' AS source_name, {field_sql.format(t=c[layer_key])}" for c in usable)

bronze_union = blocks("regexp_extract(_source_file, '([^/]+)$', 1) AS file_name,\n       cast(_batch_id AS bigint) AS batch_id, max(_ingest_ts) AS _ingest_ts,\n       max(_file_mtime) AS _file_mtime, count(*) AS raw_rows, count(_rescued_data) AS rescued_rows\nFROM {t} GROUP BY 1, 2, 3", 'bronze_t')
quar_union   = blocks("cast(_batch_id AS bigint) AS batch_id, _ingest_ts,\n       min(_quarantine_ts) AS dq_ts, count(*) AS quarantined_rows\nFROM {t} GROUP BY 1, 2, 3", 'quar_t')
silver_union = blocks("_ingest_ts, count(*) AS silver_rows, min(_merged_ts) AS first_merge_ts\nFROM {t} GROUP BY 1, 2", 'silver_t')

COMMENT = (
 "Per-file, per-microbatch lifecycle costing. CONFIG-DRIVEN: source/layer UNION blocks are generated from "
 f"{CAT}.config.pipeline_configs (active sources) - re-run the builder notebook to include new sources. "
 "DERIVATION (per-stage compute is not instrumented): stage durations are wall-clock spans between observed "
 "lifecycle events. arrival_to_bronze_seconds = bronze _ingest_ts - file mtime (exact). File-to-bronze attribution "
 "is exact via bronze._source_file. dq_seconds = quarantine write ts - bronze ingest ts (upper bound of DQ "
 "evaluation; when a file produced no quarantined rows the ingest-to-merge window is split 50/50 between DQ and "
 "silver merge - documented assumption). silver_merge_seconds = first silver _merged_ts - quarantine write ts. "
 "total_lifecycle_seconds = first _merged_ts - file mtime (end-to-end). Compute time for costing uses "
 "audit.run_log bronze/silver-layer run durations allocated pro-rata by raw row share within each run "
 "(bronze_run_seconds, silver_run_seconds). COST PRO-RATION: DBU cost is taken from system.billing.usage for "
 f"orchestration job {ORCH_JOB_ID} when telemetry has landed (about 1 day lag) and allocated to files pro-rata "
 "by raw rows; until then it is ESTIMATED as active_compute_seconds / 3600 x 1.0 DBU per active compute-hour "
 "(assumed single-worker serverless jobs execution) x the serverless jobs DBU list price from "
 "system.billing.list_prices (cost_source marks the basis). STORAGE: raw_file_bytes exact from the landing "
 f"volume; bronze/silver/quarantine bytes are pro-rata shares (by rows) of the per-layer table sizes from "
 f"{STORAGE_VIEW} (silver layer includes SCD2 _history bytes; that view sources system.storage.table_metrics_history "
 "latest daily snapshots - run its builder BEFORE this one). Silver row attribution is pro-rata by raw rows "
 "when multiple files share one microbatch (documented)."
)

sql = f"""CREATE OR REPLACE VIEW {VIEW_FQN} (
  source_name, file_name, batch_id, run_id, run_ts,
  arrival_ts, bronze_ingest_ts, dq_ts, silver_merge_ts,
  arrival_to_bronze_seconds, bronze_run_seconds, dq_seconds, silver_merge_seconds,
  silver_run_seconds, total_lifecycle_seconds,
  raw_rows, bronze_rows, quarantined_rows, rescued_rows, silver_merged_rows,
  raw_file_bytes, bronze_bytes_pro_rata, silver_bytes_pro_rata, quarantine_bytes_pro_rata, total_storage_bytes,
  active_compute_seconds, price_per_dbu, dbus_allocated, cost_usd, cost_source, cost_per_row, cost_per_file,
  seconds_per_row)
COMMENT '{COMMENT.replace(chr(39), chr(39)*2)}'
AS WITH
landing AS (
  SELECT regexp_extract(_metadata.file_path, 'source_files/([^/]+)/', 1) AS source_name,
         _metadata.file_name AS file_name,
         cast(max(_metadata.file_modification_time) AS timestamp) AS file_mtime,
         max(_metadata.file_size) AS raw_file_bytes
  FROM read_files('{LANDING_ROOT}', format => 'csv')
  GROUP BY 1, 2
),
bronze_files AS (
{chr(10).join('  ' + line for line in bronze_union.split(chr(10)))}
),
quarantine_batches AS (
{chr(10).join('  ' + line for line in quar_union.split(chr(10)))}
),
silver_batches AS (
{chr(10).join('  ' + line for line in silver_union.split(chr(10)))}
),
bronze_runs AS (
  SELECT run_id, source_name, start_ts, end_ts, coalesce(rows_read, 0) AS run_rows,
         greatest(unix_timestamp(end_ts) - unix_timestamp(start_ts), 0) AS run_seconds
  FROM {CAT}.audit.run_log WHERE layer = 'bronze'
),
silver_runs AS (
  SELECT run_id, source_name, start_ts, coalesce(rows_merged, 0) AS run_rows,
         greatest(unix_timestamp(end_ts) - unix_timestamp(start_ts), 0) AS run_seconds
  FROM {CAT}.audit.run_log WHERE layer = 'silver'
),
files AS (
  SELECT b.source_name, l.file_name, b.batch_id, b._ingest_ts AS bronze_ingest_ts,
         l.file_mtime AS arrival_ts, l.raw_file_bytes,
         b.raw_rows, b.raw_rows AS bronze_rows, b.rescued_rows
  FROM bronze_files b
  JOIN landing l ON l.source_name = b.source_name AND l.file_name = b.file_name
),
batch_totals AS (
  SELECT source_name, bronze_ingest_ts, sum(raw_rows) AS batch_raw_rows
  FROM files GROUP BY 1, 2
),
storage_footprint AS (
  -- Sourced from ops.v_storage_footprint (build that view first - see its builder notebook).
  -- silver_history (SCD2) bytes roll into 'silver'; row_count counts the primary silver table only.
  SELECT source_name,
         CASE layer WHEN 'silver_history' THEN 'silver' ELSE layer END AS layer,
         sum(bytes_on_disk) AS bytes_on_disk,
         sum(CASE WHEN layer <> 'silver_history' THEN row_count ELSE 0 END) AS row_count
  FROM {STORAGE_VIEW}
  WHERE layer IN ('bronze', 'silver', 'silver_history', 'quarantine')
  GROUP BY 1, 2
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
  LEFT JOIN storage_footprint fpb ON fpb.source_name = f4.source_name AND fpb.layer = 'bronze'
  LEFT JOIN storage_footprint fps ON fps.source_name = f4.source_name AND fps.layer = 'silver'
  LEFT JOIN storage_footprint fpq ON fpq.source_name = f4.source_name AND fpq.layer = 'quarantine'
),
job_usage AS (
  SELECT sum(usage_quantity) AS actual_dbus
  FROM system.billing.usage WHERE usage_metadata.job_id = '{ORCH_JOB_ID}'
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
CROSS JOIN grand g"""

print(f"Generated DDL: {len(sql)} chars, {len(usable)} sources")
spark.sql(sql)
print(f"CREATED/REPLACED {VIEW_FQN}")

# COMMAND ----------

# DBTITLE 1,Sanity check
# Post-build sanity check: per-source row/cost reconciliation against the physical tables
spark.sql(f"""
SELECT source_name, count(*) files,
       sum(raw_rows) raw_total, sum(silver_merged_rows) silver_total,
       sum(quarantined_rows) quar_total, sum(rescued_rows) rescued_total,
       round(sum(cost_usd), 4) cost_total, min(cost_source) cost_basis,
       min(price_per_dbu) price_per_dbu
FROM {VIEW_FQN}
GROUP BY source_name ORDER BY source_name
""").display()

# Notes for interpretation:
# - raw_total/silver_total/quar_total should match the physical bronze/silver/quarantine table counts,
#   EXCEPT rows attributed to landing files that have since been deleted (view grain = files in landing).
# - cost_basis = 'billing_usage' once system.billing.usage telemetry for the orchestration job has landed
#   (~1 day lag); 'estimated' before that (active_compute_seconds-based).

# COMMAND ----------

