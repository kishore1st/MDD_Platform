# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # MDD Ops View Builder: v_storage_footprint
# MAGIC
# MAGIC **Purpose:** (Re)generates `mdd_platform.ops.v_storage_footprint` — storage footprint of every managed MDD table (bronze / silver / silver_history / quarantine per source) plus raw landing files — from the **active sources in `mdd_platform.config.pipeline_configs`**. No source or table names are hard-coded.
# MAGIC
# MAGIC **When to run:** after onboarding a new source (once its bronze/silver/quarantine tables exist after the first pipeline run), or after changing layer target tables.
# MAGIC
# MAGIC **How it scales:**
# MAGIC - Table rows are generated from `pipeline_configs` per source and layer, with an existence guard (missing tables are skipped, SCD2 `_history` tables are auto-detected and labeled `silver_history`).
# MAGIC - Landing stats come from a single `read_files` scan of the landing volume root — new source folders are discovered automatically.
# MAGIC - `bytes_on_disk` / `file_count` are exact from `system.storage.table_metrics_history` (latest daily snapshot) with documented fallback estimates until telemetry lands.
# MAGIC
# MAGIC Run cells top-to-bottom. Companion builder: *MDD Ops View Builder (v_file_lifecycle_cost)*.

# COMMAND ----------

# DBTITLE 1,Build view from active configs
# Parameters — adjust only these
dbutils.widgets.text("catalog_name", "mdd_platform", "MDD catalog (bundle passes ${var.catalog_name})")
CAT = dbutils.widgets.get("catalog_name").strip() or 'mdd_platform'
VIEW_FQN = f'{CAT}.ops.v_storage_footprint'
LANDING_ROOT = f'/Volumes/{CAT}/landing/source_files/'

import yaml

# ---- 1) active sources + declared layer targets from the control table ----
def norm(t, schema):
    """Normalize a target_table config value to a 3-part name."""
    parts = t.split('.')
    if len(parts) == 3: return t
    if len(parts) == 2: return f'{CAT}.{parts[0]}.{parts[1]}'
    return f'{CAT}.{schema}.{parts[0]}'

sources = []
for r in spark.sql(f"SELECT source_name, config, active FROM {CAT}.config.pipeline_configs ORDER BY source_name").collect():
    if not r.active:
        print(f"SKIP (inactive): {r.source_name}")
        continue
    cfg = yaml.safe_load(r.config) or {}
    sources.append({
        'src': r.source_name,
        'bronze_t': norm((cfg.get('bronze') or {}).get('target_table') or r.source_name, 'bronze'),
        'silver_t': norm((cfg.get('silver') or {}).get('target_table') or r.source_name, 'silver'),
        'quar_t':   norm((cfg.get('quarantine') or {}).get('target_table') or r.source_name, 'quarantine'),
    })

# ---- 2) per-table stats rows (existence guard; SCD2 _history auto-detected) ----
existing = {(r.table_schema, r.table_name) for r in spark.sql(
    f"SELECT table_schema, table_name FROM {CAT}.information_schema.tables WHERE table_schema IN ('bronze','silver','quarantine')"
).collect()}

rows = []
for s in sources:
    sch, tbl = s['bronze_t'].split('.')[-2:]
    if (sch, tbl) in existing: rows.append(('bronze', s['bronze_t'], '_ingest_ts'))
    sch, tbl = s['silver_t'].split('.')[-2:]
    if (sch, tbl) in existing: rows.append(('silver', s['silver_t'], '_merged_ts'))
    hist_t = s['silver_t'] + '_history'
    sch, tbl = hist_t.split('.')[-2:]
    if (sch, tbl) in existing: rows.append(('silver_history', hist_t, '_ingest_ts'))
    sch, tbl = s['quar_t'].split('.')[-2:]
    if (sch, tbl) in existing: rows.append(('quarantine', s['quar_t'], '_quarantine_ts'))

print(f"{len(sources)} active sources -> {len(rows)} table rows:")
for layer, t, _ in rows: print(f"  {layer:15s} {t}")

table_stats = "\nUNION ALL\n".join(
    f"SELECT '{t.split('.')[-2]}' AS sch_name, '{t.split('.')[-1]}' AS tbl_name, '{t}' AS table_name, "
    f"'{layer}' AS layer, count(*) AS row_count, count(DISTINCT {ts}) AS est_write_events FROM {t}"
    for layer, t, ts in rows)

COMMENT = (
 "Storage footprint of every managed MDD table (bronze/silver/silver_history/quarantine per source) plus raw "
 f"landing files. CONFIG-DRIVEN: generated from active sources in {CAT}.config.pipeline_configs - re-run "
 "the builder notebook to include new sources (tables must exist; landing source folders are auto-discovered by "
 "the root scan). row_count is exact (count(*)). bytes_on_disk and file_count are EXACT from "
 "system.storage.table_metrics_history (latest daily snapshot, active_bytes; newly created tables first appear "
 "there the day after creation) falling back to ESTIMATES until telemetry lands: bytes estimated as row_count x "
 "raw bytes-per-row measured from the source landing CSVs (documented approximation, plus/minus 2x vs Parquet "
 "on disk); file_count estimated as distinct write events (bronze _ingest_ts / silver _merged_ts / quarantine "
 "_quarantine_ts). silver_history rows are the SCD2 physical history tables. bytes_per_row = "
 "bytes_on_disk / row_count."
)

sql = f"""CREATE OR REPLACE VIEW {VIEW_FQN} (
  source_name, layer, table_name, row_count, file_count, bytes_on_disk, bytes_per_row)
COMMENT '{COMMENT.replace(chr(39), chr(39)*2)}'
AS WITH metrics AS (
  SELECT schema_name, table_name, active_bytes, active_files,
         row_number() OVER (PARTITION BY schema_name, table_name ORDER BY snapshot_date DESC) AS rn
  FROM system.storage.table_metrics_history WHERE catalog_name = '{CAT}'
),
landing_stats AS (
  SELECT source_name, count(*) AS file_count, sum(file_bytes) AS bytes, sum(rws) AS rws
  FROM (
    SELECT regexp_extract(_metadata.file_path, 'source_files/([^/]+)/', 1) AS source_name,
           _metadata.file_name AS file_name,
           max(_metadata.file_size) AS file_bytes, count(*) AS rws
    FROM read_files('{LANDING_ROOT}', format => 'csv')
    GROUP BY 1, 2
  )
  GROUP BY 1
),
table_stats AS (
{table_stats}
),
managed AS (
  SELECT t.tbl_name, t.table_name, t.layer, t.row_count,
         coalesce(m.active_files, t.est_write_events) AS file_count,
         coalesce(m.active_bytes, cast(round(t.row_count * greatest(ls.bytes / greatest(ls.rws, 1), 1), 0) AS bigint)) AS bytes_on_disk
  FROM table_stats t
  LEFT JOIN landing_stats ls ON ls.source_name = regexp_replace(t.tbl_name, '_history$', '')
  LEFT JOIN metrics m ON m.table_name = t.tbl_name AND m.schema_name = t.sch_name AND m.rn = 1
)
SELECT regexp_replace(t.tbl_name, '_history$', '') AS source_name,
       t.layer, t.table_name, t.row_count, t.file_count, t.bytes_on_disk,
       round(t.bytes_on_disk / greatest(t.row_count, 1), 2) AS bytes_per_row
FROM managed t
UNION ALL
SELECT ls.source_name, 'landing', '{LANDING_ROOT}' || ls.source_name, ls.rws, ls.file_count, ls.bytes,
       round(ls.bytes / greatest(ls.rws, 1), 2) AS bytes_per_row
FROM landing_stats ls"""

print(f"Generated DDL: {len(sql)} chars, {len(rows)} table rows")
spark.sql(sql)
print(f"CREATED/REPLACED {VIEW_FQN}")

# COMMAND ----------

# DBTITLE 1,Sanity check
# Post-build sanity check: full footprint, then reconciliation vs ground truth
spark.sql(f"""
SELECT source_name, layer, table_name, row_count, file_count, bytes_on_disk, bytes_per_row
FROM {VIEW_FQN}
ORDER BY source_name, layer
""").display()

# Expected shape: one row per managed table (bronze/silver/silver_history/quarantine per active source)
# + one 'landing' row per discovered source folder. row_count is exact (compare with count(*) on any table);
# bytes_on_disk/file_count are exact from system.storage.table_metrics_history latest snapshot once daily
# telemetry has landed (day after table creation); before that they fall back to documented estimates.
#
# Known wrinkle: sources whose landing files were deleted after ingestion still show their managed-table rows
# (landing_stats is LEFT-joined), but the byte ESTIMATE fallback for those is unavailable (NULL) — exact
# metrics still apply once telemetry lands.

# COMMAND ----------

