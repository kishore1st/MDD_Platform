# Databricks notebook source
# MAGIC %md
# MAGIC # MDD Test — src_009 Employee Billing Onboarding
# MAGIC
# MAGIC Proves the YAML transformations feature on a real HR source: bronze raw (6 columns, header-normalized), silver **SCD1** with Full Name split into first_name/last_name, manager/project renames, and decimal casts — all declared in YAML. `_row_hash` stays carried from bronze (never recomputed); PII masked at silver (full_name + derived first_name/last_name). Source file: employee_dataset.csv (10,010 rows) moved from Drafts into the landing volume.

# COMMAND ----------

# DBTITLE 1,Land employee_dataset.csv (normalized headers)
# Land employee_dataset.csv into the source volume (headers normalized to snake_case; data rows verbatim).
# Bronze speaks the framework's identifier-safe column names; values are untouched.
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
SRC = "src_009_employee_billing"
SRC_FILE = "/Workspace/Users/kothahemanthsai58@gmail.com/Drafts/employee_dataset.csv"

raw = w.workspace.download(SRC_FILE).read().decode("utf-8")
lines = raw.strip().split("\n")
header, data = lines[0], lines[1:]
renames = {"Employee ID": "employee_id", "Full Name": "full_name", "Manager": "manager",
           "Project": "project", "Hour Price ($)": "hour_price", "Project Cost ($)": "project_cost"}
new_header = ",".join(renames.get(h.strip(), h.strip()) for h in header.split(","))
print("headers:", header, "->", new_header)
VOL_PATH = f"/Volumes/mdd_platform/landing/source_files/{SRC}/{SRC}/batch_001.csv"
dbutils.fs.put(VOL_PATH, "\n".join([new_header] + data) + "\n", overwrite=True)
print(f"landed {len(data)} data rows at {VOL_PATH}")

# COMMAND ----------

# DBTITLE 1,Author src_009 YAML (transformations)
# Author src_009 YAML — SCD1, business key employee_id. silver.transformations (silver-only,
# never bronze, never _row_hash): split Full Name, renames, decimal casts. first/last/full_name masked.
import yaml
CFG = {
    "source_name": "src_009_employee_billing",
    "source_path": "/Volumes/mdd_platform/landing/source_files/src_009_employee_billing",
    "format": "csv",
    "options": {"header": "true", "delimiter": ",", "multiLine": "true"},
    "description": "Employee billing + project cost allocation (HR) - SCD1; silver splits Full Name into first/last, renames manager/project, casts financials (transformations)",
    "columns": [
        {"name": "employee_id", "type": "string", "length": 50,
         "comment": "Unique employee identifier (quasi-identifier; business key - unmasked)"},
        {"name": "full_name", "type": "string", "length": 150,
         "comment": "PII - employee full name (masked at silver, tagged at bronze)"},
        {"name": "manager", "type": "string", "length": 150,
         "comment": "Reporting manager name (direct identifier; clear text per spec)"},
        {"name": "project", "type": "string", "length": 150,
         "comment": "Assigned project name"},
        {"name": "hour_price", "type": "decimal(10,2)",
         "comment": "Hourly billing rate in USD (sensitive financial)"},
        {"name": "project_cost", "type": "decimal(12,2)",
         "comment": "Total project cost allocation in USD (sensitive financial)"},
    ],
    "business_keys": ["employee_id"],
    "pii_columns": {"full_name": "name", "first_name": "name", "last_name": "name"},
    "quality_rules": [
        {"name": "employee_id_not_null", "rule": "not_null", "column": "employee_id"},
        {"name": "employee_id_format", "rule": "regex", "column": "employee_id", "pattern": "^EMP[0-9]{5}$"},
        {"name": "full_name_not_null", "rule": "not_null", "column": "full_name"},
        {"name": "project_not_null", "rule": "not_null", "column": "project"},
        {"name": "hour_price_not_null", "rule": "not_null", "column": "hour_price"},
        {"name": "project_cost_not_null", "rule": "not_null", "column": "project_cost"},
        {"name": "hour_price_range", "rule": "range", "column": "hour_price", "min": 0, "max": 10000},
        {"name": "project_cost_range", "rule": "range", "column": "project_cost", "min": 0, "max": 10000000},
    ],
    "bronze": {
        "target_table": "mdd_platform.bronze.src_009_employee_billing",
        "comment": "Raw landing zone for employee billing + project cost (HR) - SCD1 onboarding source",
        "partition_by": ["_ingest_date"],
        "table_properties": {"delta.deletedFileRetentionDuration": "interval 7 days",
                              "mdd.quality_layer": "bronze", "mdd.source_system": "HR"},
        "exception_rules": {"max_rescued_pct": 15, "on_exceed": "warn"},
    },
    "quarantine_action": "route_and_log",
    "silver": {
        "target_table": "mdd_platform.silver.src_009_employee_billing",
        "comment": "Conformed employee billing - typed, deduped, name split + renames + casts via transformations, SCD1",
        "business_keys": ["employee_id"],
        "merge_keys": ["employee_id"],
        "merge_strategy": "scd1",
        "cluster_by": ["employee_id"],
        "table_properties": {"delta.deletedFileRetentionDuration": "interval 7 days",
                              "mdd.quality_layer": "silver"},
        "transformations": [
            {"column": "first_name", "type": "varchar(75)",
             "comment": "Split from Full Name (before first space) - PII, masked at silver",
             "expr": "regexp_extract(full_name, '^([^ ]+)', 1)"},
            {"column": "last_name", "type": "varchar(75)",
             "comment": "Split from Full Name (after first space) - PII, masked at silver",
             "expr": "regexp_extract(full_name, '^[^ ]+ +(.+)$', 1)"},
            {"column": "manager_name", "type": "varchar(150)",
             "comment": "Renamed from manager for clarity",
             "expr": "manager"},
            {"column": "project_name", "type": "varchar(150)",
             "comment": "Renamed from project for clarity",
             "expr": "project"},
            {"column": "hourly_rate_usd", "type": "decimal(10,2)",
             "comment": "Hourly billing rate cast to standard decimal (sensitive financial)",
             "expr": "cast(hour_price as decimal(10,2))"},
            {"column": "project_cost_usd", "type": "decimal(12,2)",
             "comment": "Project cost cast to standard decimal (sensitive financial)",
             "expr": "cast(project_cost as decimal(12,2))"},
        ],
    },
}
YAML_PATH = "/Volumes/mdd_platform/config/config_files/src_009_employee_billing.yaml"
dbutils.fs.put(YAML_PATH, yaml.safe_dump(CFG, sort_keys=False), overwrite=True)
print("YAML written to", YAML_PATH)
print("transformations:")
for t in CFG["silver"]["transformations"]:
    print(f"  {t['column']:16s} {t['type']:12s} <- {t['expr']}")

# COMMAND ----------

# DBTITLE 1,Run config loader (validate + MERGE)
# MAGIC %run "/MDD_DataPlatform/MDD Config Loader"

# COMMAND ----------

# DBTITLE 1,Trigger runbook load (src_009)
import time, datetime
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
print("start:", datetime.datetime.now().strftime("%H:%M:%S"), "UTC")
run = w.jobs.run_now("180017381911014", notebook_params={
    "source_name": "src_009_employee_billing", "mode": "load", "confirm": "true"})
print("load run_id:", run.run_id)
state, rr = None, None
for _ in range(50):  # poll up to ~8.5 min (job continues server-side if cell returns first)
    time.sleep(10)
    rr = w.jobs.get_run(run.run_id)
    state = rr.state.life_cycle_state
    if state not in ("PENDING", "RUNNING", "QUEUED", "BLOCKED"):
        break
print("final lifecycle state:", state, "| result:", getattr(rr.state, "result_state", None))

# COMMAND ----------

# DBTITLE 1,Run config loader (PII catchup)
# MAGIC %run "/MDD_DataPlatform/MDD Config Loader"

# COMMAND ----------

# DBTITLE 1,Verify src_009 end-to-end
B = "mdd_platform.bronze.src_009_employee_billing"
S = "mdd_platform.silver.src_009_employee_billing"
Q = "mdd_platform.quarantine.src_009_employee_billing"
from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("counts:", {t: spark.table(x).count() for t, x in [("bronze", B), ("silver", S), ("quarantine", Q)]})

ddl = spark.sql(f"SHOW CREATE TABLE {S}").first()[0]
print("\ntransformed columns in silver DDL:")
for l in ddl.splitlines():
    if any(c in l for c in ("first_name", "last_name", "manager_name", "project_name", "hourly_rate_usd", "project_cost_usd")):
        print("  ", l.strip())

print("\nsample rows (PII masked, financials clear):")
spark.sql(f"""SELECT employee_id, full_name, first_name, last_name, manager_name, project_name,
               hour_price, hourly_rate_usd, project_cost, project_cost_usd
               FROM {S} ORDER BY employee_id LIMIT 6""").show(truncate=35)

# split completeness vs bronze (masked silver nulls still reveal missing splits)
wb = Window.partitionBy("employee_id").orderBy(F.col("_ingest_ts").desc(), F.col("_file_mtime").desc())
bl = (spark.table(B).withColumn("_rn", F.row_number().over(wb)).filter("_rn = 1")
      .select("employee_id", "full_name", "_row_hash"))
chk = (spark.table(S).select("employee_id", "first_name", "last_name", F.col("_row_hash").alias("sh"))
       .join(bl, "employee_id"))
bad_split = chk.where((F.col("first_name").isNull() & F.col("full_name").isNotNull()) |
                     (F.col("last_name").isNull() & F.col("full_name").rlike("^.+ .+$"))).count()
hash_mismatch = chk.where(F.col("sh") != F.col("_row_hash")).count()
print("split disagrees with Full Name shape:", bad_split, "(0 = split complete)")
print("silver vs latest-bronze _row_hash mismatches:", hash_mismatch, "(0 = hash carried from bronze, never recomputed)")

print("\nquarantine summary (which rules fired):")
spark.sql(f"SELECT _failed_rules, count(*) c FROM {Q} GROUP BY 1 ORDER BY 2 DESC").show(truncate=False)

print("PII tags on silver (incl. derived first/last):")
spark.sql("""SELECT column_name, tag_value FROM mdd_platform.information_schema.column_tags
  WHERE table_name = 'src_009_employee_billing' AND tag_name = 'pii_type' ORDER BY column_name""").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Add exclude_columns to YAML
# Add silver.exclude_columns to src_009 — raw originals stay in bronze + quarantine + _row_hash,
# but are NOT written to silver now that transformations produce the conformed columns.
import yaml
P = "/Volumes/mdd_platform/config/config_files/src_009_employee_billing.yaml"
cfg = yaml.safe_load(dbutils.fs.head(P))
cfg["silver"]["exclude_columns"] = ["full_name", "manager", "project", "hour_price", "project_cost"]
dbutils.fs.put(P, yaml.safe_dump(cfg, sort_keys=False), overwrite=True)
print("exclude_columns:", cfg["silver"]["exclude_columns"])
print("silver will contain only: employee_id + 6 derived columns + tech columns")

# COMMAND ----------

# DBTITLE 1,Run config loader (validate exclusions)
# MAGIC %run "/MDD_DataPlatform/MDD Config Loader"

# COMMAND ----------

# DBTITLE 1,Rebuild src_009 with exclusions
import time, datetime
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
def sn(s): return getattr(s, "name", None) or str(s)
print("start:", datetime.datetime.now().strftime("%H:%M:%S"), "UTC")
run = w.jobs.run_now("180017381911014", notebook_params={
    "source_name": "src_009_employee_billing", "mode": "rebuild", "confirm": "true"})
print("rebuild run_id:", run.run_id)
for i in range(80):
    time.sleep(15)
    rr = w.jobs.get_run(run.run_id)
    st = sn(rr.state.life_cycle_state)
    if st not in ("PENDING", "RUNNING", "QUEUED", "BLOCKED"):
        break
print("final:", st, "| result:", sn(rr.state.result_state))
if sn(rr.state.result_state) == "FAILED":
    out = w.jobs.get_run_output(rr.tasks[0].run_id)
    print("error:", (getattr(out, "error", None) or "")[:500])

# COMMAND ----------

# DBTITLE 1,Run config loader (PII catchup)
# MAGIC %run "/MDD_DataPlatform/MDD Config Loader"

# COMMAND ----------

# DBTITLE 1,Verify exclusion end-to-end
B = "mdd_platform.bronze.src_009_employee_billing"
S = "mdd_platform.silver.src_009_employee_billing"
Q = "mdd_platform.quarantine.src_009_employee_billing"
from pyspark.sql import functions as F
from pyspark.sql.window import Window

scols = spark.sql(f"SELECT * FROM {S} LIMIT 0").columns
expected = ["employee_id", "first_name", "last_name", "manager_name", "project_name",
            "hourly_rate_usd", "project_cost_usd", "_ingest_ts", "_merged_ts", "_row_hash"]
print("silver columns:", scols)
print("EXACT match to target spec:", scols == expected)
print("bronze columns (all 6 raw kept):", spark.sql(f"SELECT * FROM {B} LIMIT 0").columns)
print("counts:", {t: spark.table(x).count() for t, x in [("bronze", B), ("silver", S), ("quarantine", Q)]})

print("\nsilver sample (no raw originals, no repeats):")
spark.sql(f"""SELECT employee_id, first_name, last_name, manager_name, project_name,
               hourly_rate_usd, project_cost_usd FROM {S} ORDER BY employee_id LIMIT 6""").show(truncate=35)

print("quarantine keeps RAW columns (full_name masked, hour_price visible):")
spark.sql(f"SELECT employee_id, full_name, hour_price, _failed_rules FROM {Q} LIMIT 4").show(truncate=45)

wb = Window.partitionBy("employee_id").orderBy(F.col("_ingest_ts").desc(), F.col("_file_mtime").desc())
bl = spark.table(B).withColumn("_rn", F.row_number().over(wb)).filter("_rn = 1").select("employee_id", "_row_hash")
mm = (spark.table(S).select("employee_id", F.col("_row_hash").alias("sh")).join(bl, "employee_id")
      .where(F.col("sh") != F.col("_row_hash"))).count()
print("silver vs latest-bronze _row_hash mismatches:", mm,
      "(0 = hash still covers ALL raw columns incl. excluded — a full_name change still updates silver)")