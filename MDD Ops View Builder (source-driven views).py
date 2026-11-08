# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Databricks notebook source
# MAGIC # MAGIC %md
# MAGIC # MAGIC # MDD Ops View Builder: source-driven views (freshness, late arrivals, quarantine, rescued)
# MAGIC # MAGIC
# MAGIC # MAGIC **Purpose:** (Re)generates the four source-driven `ops` views — `v_freshness_sla`, `v_late_arrivals`, `v_quarantine_detail`, `v_rescued_data` — from the **active sources in `{catalog}.config.pipeline_configs`**. No source names are hard-coded: onboard a new source (drop its YAML + landing files) and re-running this notebook includes it automatically.
# MAGIC # MAGIC
# MAGIC # MAGIC **Why this exists:** the bundle's `ops_views_capture.json` ships DDL snapshots from the source workspace with hardcoded `src_*` table unions — those can never apply in a client workspace with different sources. This builder replaces them: it is run by the **MDD Ops Views Refresh Job** (daily 03:00 UTC) at setup time and by the smoke test, so the views always exist and always match THIS catalog's actual sources.
# MAGIC # MAGIC
# MAGIC # MAGIC **Guarantees:**
# MAGIC # MAGIC - Existence-guarded: a source's branch appears only once its bronze/silver/quarantine tables exist.
# MAGIC # MAGIC - Never-missing: if no tables exist yet, each view is created as an EMPTY placeholder (correct columns, zero rows) instead of failing — the daily refresh rebuilds it with real branches.
# MAGIC # MAGIC - `catalog_name` parameter (bundle variable; default `mdd_platform`).
# MAGIC # MAGIC
# MAGIC # MAGIC Run cells top-to-bottom. Companion builders: *MDD Ops View Builder (v_storage_footprint)*, *MDD Ops View Builder (v_file_lifecycle_cost)*.

# COMMAND ----------

# DBTITLE 1,Build views from active configs
# Parameters — adjust only these
dbutils.widgets.text("catalog_name", "mdd_platform", "MDD catalog (bundle passes ${var.catalog_name})")
CAT = dbutils.widgets.get("catalog_name").strip() or 'mdd_platform'
OPS = f'{CAT}.ops'

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
    silver_cfg = cfg.get('silver') or {}
    keys = silver_cfg.get('business_keys') or []
    record_key = (keys[0] if keys else None) or ((cfg.get('columns') or [{}])[0].get('name')) or 'record_key'
    cfgs.append({
        'src': r.source_name,
        'bronze_t': norm((cfg.get('bronze') or {}).get('target_table') or r.source_name, 'bronze'),
        'silver_t': norm(silver_cfg.get('target_table') or r.source_name, 'silver'),
        'quar_t':   norm((cfg.get('quarantine') or {}).get('target_table') or r.source_name, 'quarantine'),
        'record_key': record_key,
    })
print(f"{len(cfgs)} active sources: {[c['src'] for c in cfgs]}")

# ---- 2) existence guard ----
existing = {(r.table_schema, r.table_name) for r in spark.sql(
    f"SELECT table_schema, table_name FROM {CAT}.information_schema.tables WHERE table_schema IN ('bronze','silver','quarantine')"
).collect()}

def exists(t):
    _, sch, tbl = t.split('.')
    return (sch, tbl) in existing

# ---- 3) view factory: never-missing (empty placeholder if no tables exist yet) ----
def placeholder(cols):
    return ("SELECT " + ", ".join(f"cast(NULL AS STRING) AS {c}" for c in cols)
            + "\nWHERE 1=0  -- no layer tables exist yet; the daily refresh rebuilds with real branches")

def apply(view, cols, body, n):
    sql = (f"CREATE OR REPLACE VIEW {OPS}.{view} ({', '.join(cols)})\n"
           f"COMMENT 'CONFIG-DRIVEN: generated from active sources in {CAT}.config.pipeline_configs - re-run this builder to include new sources'\n"
           f"AS {body}")
    spark.sql(sql)
    print(f"  CREATED/REPLACED {OPS}.{view} ({n} source branches)")

# ---- 4) v_freshness_sla (needs bronze + silver per source) ----
cols = ['source_name', 'last_bronze_ingest', 'last_silver_merge', 'bronze_age_hours', 'silver_age_hours', 'freshness_status']
br = [f"SELECT '{c['src']}' AS source_name, (SELECT max(_ingest_ts) FROM {c['bronze_t']}) AS last_bronze_ingest, (SELECT max(_merged_ts) FROM {c['silver_t']}) AS last_silver_merge"
      for c in cfgs if exists(c['bronze_t']) and exists(c['silver_t'])]
body = (("WITH base AS (\n" + "\nUNION ALL\n".join(br) + ")\nSELECT source_name, last_bronze_ingest, last_silver_merge, "
        "round((unix_timestamp(current_timestamp())-unix_timestamp(last_bronze_ingest))/3600.0,2) AS bronze_age_hours, "
        "round((unix_timestamp(current_timestamp())-unix_timestamp(last_silver_merge))/3600.0,2) AS silver_age_hours, "
        "CASE WHEN (unix_timestamp(current_timestamp())-unix_timestamp(last_bronze_ingest))/3600.0 < 2 THEN 'fresh' "
        "WHEN (unix_timestamp(current_timestamp())-unix_timestamp(last_bronze_ingest))/3600.0 < 24 THEN 'stale' "
        "ELSE 'critical' END AS freshness_status FROM base") if br else placeholder(cols))
apply('v_freshness_sla', cols, body, len(br))

# ---- 5) v_late_arrivals (needs bronze) ----
cols = ['source_name', 'rows_ingested', 'late_rows', 'avg_arrival_lag_minutes', 'max_arrival_lag_minutes']
br = [f"SELECT '{c['src']}' AS source_name, count(*) AS rows_ingested, "
      "sum(CASE WHEN (unix_timestamp(_ingest_ts)-unix_timestamp(_file_mtime)) > 3600 THEN 1 ELSE 0 END) AS late_rows, "
      "round(avg(unix_timestamp(_ingest_ts)-unix_timestamp(_file_mtime))/60.0, 1) AS avg_arrival_lag_minutes, "
      "round(max(unix_timestamp(_ingest_ts)-unix_timestamp(_file_mtime))/60.0, 1) AS max_arrival_lag_minutes "
      f"FROM {c['bronze_t']} WHERE _file_mtime IS NOT NULL"
      for c in cfgs if exists(c['bronze_t'])]
apply('v_late_arrivals', cols, "\nUNION ALL\n".join(br) if br else placeholder(cols), len(br))

# ---- 6) v_quarantine_detail (needs quarantine; record_key = first business key) ----
cols = ['source_name', 'record_key', 'failed_rule', '_batch_id', '_ingest_ts', '_quarantine_ts', 'quarantine_age_hours']
br = [f"SELECT '{c['src']}' AS source_name, `{c['record_key']}` AS record_key, explode(_failed_rules) AS failed_rule, "
      "_batch_id, _ingest_ts, _quarantine_ts, "
      "round((unix_timestamp(current_timestamp())-unix_timestamp(_quarantine_ts))/3600.0, 2) AS quarantine_age_hours "
      f"FROM {c['quar_t']}"
      for c in cfgs if exists(c['quar_t'])]
apply('v_quarantine_detail', cols, "\nUNION ALL\n".join(br) if br else placeholder(cols), len(br))

# ---- 7) v_rescued_data (needs bronze) ----
cols = ['source_name', 'drift_column', 'source_file', '_ingest_ts', '_file_mtime', '_batch_id']
br = [f"SELECT '{c['src']}' AS source_name, explode(filter(map_keys(m), col -> col != '_file_path')) AS drift_column, "
      "m['_file_path'] AS source_file, _ingest_ts, _file_mtime, _batch_id "
      f"FROM (SELECT from_json(_rescued_data, 'map<string,string>') AS m, _ingest_ts, _file_mtime, _batch_id "
      f"FROM {c['bronze_t']} WHERE _rescued_data IS NOT NULL)"
      for c in cfgs if exists(c['bronze_t'])]
apply('v_rescued_data', cols, "\nUNION ALL\n".join(br) if br else placeholder(cols), len(br))

print("\nsource-driven views rebuild complete")

# COMMAND ----------

