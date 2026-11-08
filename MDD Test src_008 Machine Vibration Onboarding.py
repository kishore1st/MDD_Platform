# Databricks notebook source
# DBTITLE 1,MDD Test — src_008 Machine Vibration Onboarding
# MAGIC %md
# MAGIC # MDD Test — src_008 Machine Vibration Onboarding
# MAGIC Zero-touch onboarding test for the machine vibration (MES) dataset: 3 batches of 1,000 rows, composite key (timestamp, machine_id), SCD1+SCD2, liquid clustering, PII masking (employee_phone, supplier_contact), 8 DQ rules. Runs the real config loader + hourly pipeline job and verifies every layer.

# COMMAND ----------

# DBTITLE 1,Land batches into src_008 folder (user-confirmed path)
WS = "/Workspace/Users/kothahemanthsai58@gmail.com"
DEST = "/Volumes/mdd_platform/landing/source_files/src_008_machine_vibration"
dbutils.fs.mkdirs(DEST)
for n in (1, 2, 3):
    lines = open(f"{WS}/manufacturing_batch_{n}.csv").read().splitlines()
    assert lines[0].startswith("Timestamp,Machine_ID"), lines[0][:50]  # original Title_Case header
    lines[0] = lines[0].lower()  # -> snake_case matching YAML column names (positional mapping also works, this removes all ambiguity)
    content = "\n".join(lines) + "\n"
    try:
        dbutils.fs.put(f"{DEST}/manufacturing_batch_{n}.csv", content, overwrite=True)
    except Exception as e:
        print("put fallback to local file API:", str(e)[:150])
        with open(f"{DEST}/manufacturing_batch_{n}.csv", "w") as fh:
            fh.write(content)
    print(f"manufacturing_batch_{n}.csv: {len(lines)-1} data rows landed, header normalized")
print("folder contents:", [f.name for f in dbutils.fs.ls(DEST)])

# COMMAND ----------

# DBTITLE 1,Author src_008 YAML (composite key, SCD1+SCD2, clustering, PII, 8 DQ rules)
import yaml
CAT = "mdd_platform"
LAND = f"/Volumes/{CAT}/landing/source_files/src_008_machine_vibration"
cols = [
    {"name": "timestamp", "type": "timestamp", "comment": "Composite business key part 1 - sensor reading time (MES telemetry)"},
    {"name": "machine_id", "type": "string", "length": 50, "comment": "Composite business key part 2 - physical asset identifier"},
    {"name": "operator_id", "type": "string", "length": 50, "comment": "Corporate personnel identifier code (restricted)"},
    {"name": "operator_name", "type": "string", "length": 100, "comment": "Technician full name (operational metric)"},
    {"name": "operator_email", "type": "string", "length": 150, "comment": "Operator personal email handle (privacy-review candidate; format-checked)"},
    {"name": "employee_phone", "type": "string", "length": 50, "comment": "PII - staff mobile contact (masked at silver, tagged at bronze)"},
    {"name": "shift_schedule", "type": "string", "length": 20, "comment": "Processing window: Morning | Evening | Night"},
    {"name": "manager_name", "type": "string", "length": 100, "comment": "Shift supervisor name (operational metric)"},
    {"name": "mgr_contact", "type": "string", "length": 150, "comment": "Shift manager escalation contact email"},
    {"name": "machine_supplier", "type": "string", "length": 100, "comment": "Asset vendor name (protected supply classification)"},
    {"name": "supplier_contact", "type": "string", "length": 150, "comment": "PII - vendor support contact email (masked at silver, tagged at bronze)"},
    {"name": "temperature_c", "type": "decimal(6,2)", "comment": "Sensor: line temperature in Celsius"},
    {"name": "vibration_mms", "type": "decimal(5,2)", "comment": "Sensor: vibration displacement in mm/s"},
    {"name": "pressure_psi", "type": "decimal(6,2)", "comment": "Sensor: line pressure in PSI"},
    {"name": "status", "type": "string", "length": 20, "comment": "Machine condition: Normal | Warning | Fault"},
    {"name": "batch", "type": "string", "length": 50, "comment": "Source batch token (Batch_1 / Batch_2 / Batch_3 / Batch_4)"},
]
cfg = {
    "source_name": "src_008_machine_vibration",
    "source_path": LAND,
    "format": "csv",
    "options": {"header": "true", "delimiter": ",", "multiLine": "true"},
    "description": "Machine vibration telemetry (MES) - composite key (timestamp, machine_id), PII: employee_phone + supplier_contact, SCD1+SCD2 with liquid clustering",
    "columns": cols,
    "business_keys": ["timestamp", "machine_id"],
    "pii_columns": {"employee_phone": "phone", "supplier_contact": "email"},
    "quality_rules": [
        {"name": "timestamp_not_null", "rule": "not_null", "column": "timestamp"},
        {"name": "machine_id_not_null", "rule": "not_null", "column": "machine_id"},
        {"name": "status_allowed", "rule": "allowed", "column": "status", "values": ["Normal", "Warning", "Fault"]},
        {"name": "shift_allowed", "rule": "allowed", "column": "shift_schedule", "values": ["Morning", "Evening", "Night"]},
        {"name": "temperature_range", "rule": "range", "column": "temperature_c", "min": -40, "max": 300},
        {"name": "vibration_range", "rule": "range", "column": "vibration_mms", "min": 0, "max": 100},
        {"name": "pressure_range", "rule": "range", "column": "pressure_psi", "min": 0, "max": 500},
        {"name": "operator_email_format", "rule": "regex", "column": "operator_email", "pattern": "^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$"},
    ],
    "bronze": {
        "target_table": f"{CAT}.bronze.src_008_machine_vibration",
        "comment": "Raw landing zone for machine vibration telemetry (MES) - SCD1+SCD2 onboarding test source",
        "partition_by": ["_ingest_date"],
        "table_properties": {"delta.deletedFileRetentionDuration": "interval 7 days", "mdd.quality_layer": "bronze", "mdd.source_system": "MES"},
        "exception_rules": {"max_rescued_pct": 15, "on_exceed": "warn"},
    },
    "quarantine_action": "route_and_log",
    "silver": {
        "target_table": f"{CAT}.silver.src_008_machine_vibration",
        "comment": "Conformed machine vibration telemetry - composite key (timestamp, machine_id), typed, deduped, SCD2 history",
        "business_keys": ["timestamp", "machine_id"],
        "merge_keys": ["timestamp", "machine_id"],
        "merge_strategy": "scd1+scd2",
        "table_properties": {"delta.deletedFileRetentionDuration": "interval 7 days", "mdd.quality_layer": "silver"},
        "cluster_by": ["timestamp", "machine_id"],
    },
}
out = yaml.safe_dump(cfg, sort_keys=False, default_flow_style=False, width=120)
rp = yaml.safe_load(out)
assert [c["name"] for c in rp["columns"]] == [c["name"] for c in cols]
assert rp["silver"]["merge_strategy"] == "scd1+scd2" and rp["silver"]["cluster_by"] == ["timestamp", "machine_id"]
assert rp["pii_columns"] == {"employee_phone": "phone", "supplier_contact": "email"}
assert len(rp["quality_rules"]) == 8
dbutils.fs.put(f"/Volumes/{CAT}/config/config_files/src_008_machine_vibration.yaml", out, overwrite=True)
print("src_008_machine_vibration.yaml written:", len(cols), "columns")

# COMMAND ----------

# DBTITLE 1,Config load — expect the known new-source PII ordering wrinkle
try:
    dbutils.notebook.run("/MDD_DataPlatform/MDD Config Loader", 600, {})
    print("config loader: clean run")
except Exception as e:
    print("PREDICTED wrinkle (src_008 PII ALTER before bronze/silver tables exist):", str(e)[:400])
print("active sources:", spark.sql("SELECT count(*) FROM mdd_platform.config.pipeline_configs WHERE active").collect()[0][0])
print("src_008 loaded:", spark.sql("SELECT count(*) FROM mdd_platform.config.pipeline_configs WHERE source_name='src_008_machine_vibration' AND active").collect()[0][0])

# COMMAND ----------

# DBTITLE 1,Pipeline run 1 — initial load of 3 batches
from databricks.sdk import WorkspaceClient
import time
w = WorkspaceClient()
res_run_id = 996736030725447  # pipeline run 1 already triggered (job 736782493336258); poll it to completion
while True:
    r = w.jobs.get_run(run_id=res_run_id)
    state = str(r.state.life_cycle_state)  # SDK returns enum repr like 'RunLifeCycleState.RUNNING'
    if not any(k in state for k in ("PENDING", "RUNNING", "QUEUED")):
        break
    time.sleep(30)
print("pipeline run 1:", r.state.life_cycle_state, r.state.result_state, "| run_id:", res_run_id)

# COMMAND ----------

# DBTITLE 1,Verify run 1
B = "mdd_platform.bronze.src_008_machine_vibration"
S = "mdd_platform.silver.src_008_machine_vibration"
H = "mdd_platform.silver.src_008_machine_vibration_history"
Q = "mdd_platform.quarantine.src_008_machine_vibration"
b, s = spark.table(B), spark.table(S)
print("bronze:", b.count(), "| hash:", b.where("_row_hash IS NOT NULL").count())
print("silver:", s.count(), "| hash:", s.where("_row_hash IS NOT NULL").count())
print("history:", spark.table(H).count(), "| current:", spark.table(H).where("_is_current").count())
print("quarantine:", spark.table(Q).count())
print("\nrun_log:")
spark.sql("SELECT layer, status, rows_read, rows_written, rows_rescued, rows_quarantined, rows_merged, error_message FROM mdd_platform.audit.run_log WHERE source_name='src_008_machine_vibration' ORDER BY start_ts").show(truncate=60)
print("dq failures:")
spark.sql("SELECT rule_name, failed_rows FROM mdd_platform.audit.dq_metrics WHERE source_name='src_008_machine_vibration' AND failed_rows > 0").show()
print("watermark:", spark.sql("SELECT * FROM mdd_platform.audit.silver_watermarks WHERE source_name='src_008_machine_vibration'").collect())
s.select("timestamp", "machine_id", "temperature_c", "status", "employee_phone", "supplier_contact").show(3, truncate=40)

# COMMAND ----------

# DBTITLE 1,PII catch-up — config loader re-run now that tables exist
dbutils.notebook.run("/MDD_DataPlatform/MDD Config Loader", 600, {})
S = "mdd_platform.silver.src_008_machine_vibration"
H = "mdd_platform.silver.src_008_machine_vibration_history"
for t in (S, H):
    ddl = spark.sql(f"SHOW CREATE TABLE {t}").collect()[0][0]
    for col in ("employee_phone", "supplier_contact"):
        m = [l.strip() for l in ddl.split("\n") if f"`{col}`" in l and "MASK" in l.upper()]
        print(f"{t.split('.')[-1]}.{col}: {m[0] if m else 'NO MASK FOUND'}")
print("\ncolumn tags (bronze + silver + history):")
spark.sql("""SELECT schema_name, table_name, column_name, tag_name, tag_value FROM mdd_platform.information_schema.column_tags
             WHERE column_name IN ('employee_phone','supplier_contact') ORDER BY table_name, column_name, tag_name""").show(30, truncate=30)
print("masked-read demo:")
spark.sql(f"SELECT machine_id, employee_phone, supplier_contact FROM {S} LIMIT 3").show(truncate=40)

# COMMAND ----------

# DBTITLE 1,Verify history masked read + real DDL mask lines
# Confirm masks on the SCD2 history table by direct read, and show the real DDL mask lines
H = "mdd_platform.silver.src_008_machine_vibration_history"
print("history masked-read demo:")
spark.sql(f"SELECT machine_id, employee_phone, supplier_contact FROM {H} LIMIT 3").show(truncate=40)
print("silver DDL lines mentioning PII columns or MASK:")
ddl = spark.sql("SHOW CREATE TABLE mdd_platform.silver.src_008_machine_vibration").collect()[0][0]
print("\n".join(l.strip() for l in ddl.split("\n") if "MASK" in l.upper() or "employee_phone" in l or "supplier_contact" in l))

# COMMAND ----------

# DBTITLE 1,Write batch_004 re-delivery test (5 exact dupes + 5 changed)
DEST = "/Volumes/mdd_platform/landing/source_files/src_008_machine_vibration"
lines = open(f"{DEST}/manufacturing_batch_1.csv").read().splitlines()
hdr, data = lines[0], [l.split(",") for l in lines[1:]]
# Diagnose run-1 delta (silver 2800 of bronze 3000): expect 2800 distinct composite keys = in-batch dedup
b = spark.table("mdd_platform.bronze.src_008_machine_vibration")
print("bronze distinct (timestamp, machine_id):", b.select("timestamp", "machine_id").distinct().count())
# batch_004 must use keys unique in bronze, so silver holds exactly these batch_1 values (hash will match for dupes)
once = (b.groupBy("timestamp", "machine_id").count().where("count = 1").select("timestamp", "machine_id"))
keyset = {(r["timestamp"], r["machine_id"]) for r in once.collect()}
uniq_b1 = [r for r in data if (r[0], r[1]) in keyset]
print("batch_1 rows with unique composite key:", len(uniq_b1))
assert len(uniq_b1) >= 10, len(uniq_b1)
out = [",".join(r) for r in uniq_b1[:5]]          # 5 exact re-deliveries -> hash guard must skip
for r in uniq_b1[5:10]:                          # 5 changed -> SCD1 update + SCD2 close/insert
    r = list(r)
    r[11] = f"{float(r[11]) + 5:.2f}"                     # temperature_c +5
    r[14] = "Warning" if r[14] == "Normal" else "Normal"  # status flip
    r[15] = "Batch_4"                                     # batch token
    out.append(",".join(r))
dbutils.fs.put(f"{DEST}/manufacturing_batch_4_redelivery.csv", "\n".join([hdr] + out) + "\n", overwrite=True)
print("batch_004:", len(out), "rows (5 exact dupes, 5 changed)")

# COMMAND ----------

# DBTITLE 1,Pipeline run 2 — incremental + SCD2 versioning
from databricks.sdk import WorkspaceClient
import time
w = WorkspaceClient()
res = w.jobs.run_now(job_id=736782493336258)
while True:
    r = w.jobs.get_run(run_id=res.run_id)
    state = str(r.state.life_cycle_state)  # SDK returns enum repr like 'RunLifeCycleState.RUNNING'
    if not any(k in state for k in ("PENDING", "RUNNING", "QUEUED")):
        break
    time.sleep(30)
print("pipeline run 2:", r.state.life_cycle_state, r.state.result_state, "| run_id:", res.run_id)

# COMMAND ----------

# DBTITLE 1,Verify run 2 — SCD2, hash guard, clustering, continuity
S = "mdd_platform.silver.src_008_machine_vibration"
H = "mdd_platform.silver.src_008_machine_vibration_history"
print("silver:", spark.table(S).count(), "(updates in place - count unchanged from run 1)")
h = spark.table(H)
print("history:", h.count(), "| current:", h.where("_is_current").count(), "| closed:", h.where("NOT _is_current").count())
print("\nMERGE metrics (silver):")
from pyspark.sql import functions as F
(spark.sql(f"DESCRIBE HISTORY {S}").where("operation='MERGE'")
     .select("version",
             F.expr("operationMetrics['numTargetRowsUpdated']").alias("numTargetRowsUpdated"),
             F.expr("operationMetrics['numTargetRowsInserted']").alias("numTargetRowsInserted")).show())
print("\nSCD2 continuity (closed._effective_to == next._effective_from):")
spark.sql(f"""SELECT c.machine_id, c._effective_to, n._effective_from, (c._effective_to = n._effective_from) AS continuity_ok
  FROM {H} c JOIN {H} n ON c.timestamp = n.timestamp AND c.machine_id = n.machine_id AND c._effective_to = n._effective_from
  WHERE NOT c._is_current""").show(10)
print("clustering:")
for t in (S, H):
    ddl = spark.sql(f"SHOW CREATE TABLE {t}").collect()[0][0]
    print(t.split(".")[-1], [l.strip() for l in ddl.split("\n") if "CLUSTER BY" in l.upper()])
print("watermark:", spark.sql("SELECT last_processed_ingest_ts FROM mdd_platform.audit.silver_watermarks WHERE source_name='src_008_machine_vibration'").collect())
print("\nrun_log:")
spark.sql("SELECT layer, status, rows_read, rows_written, rows_quarantined, rows_merged FROM mdd_platform.audit.run_log WHERE source_name='src_008_machine_vibration' ORDER BY start_ts").show(truncate=40)

# COMMAND ----------

# DBTITLE 1,Validate config loader PII guard (synthetic new source, atomic)
# Atomic guard validation (post-gap-fix): synthetic new source with PII but NO tables yet.
# Before the patch, this scenario failed the config loader; after, it must complete cleanly with DEFERRED lines.
# Insert row -> run patched config loader -> verify -> remove row, all in ONE execution (no exposure window).
import json
CAT = "mdd_platform"
synth = {
    "source_name": "zz_guard_test",
    "columns": [{"name": "employee_phone", "type": "string", "length": 50}],
    "business_keys": ["k"],
    "pii_columns": {"employee_phone": "phone"},
    "quality_rules": [],
}
try:
    spark.sql(f"""INSERT INTO {CAT}.config.pipeline_configs VALUES
      ('zz_guard_test', '{json.dumps(synth)}', '["k"]',
       '{{"employee_phone": "phone"}}', '[]', 'synthetic://guard-test', true, current_timestamp())""")
    print("synthetic row inserted; running PATCHED config loader (target tables intentionally missing)...")
    dbutils.notebook.run("/MDD_DataPlatform/MDD Config Loader", 600, {})
    print("CONFIG LOADER COMPLETED CLEAN — PII guard works (no exception for missing tables)")
except Exception as e:
    print("GUARD TEST FAILED:", str(e)[:500])
finally:
    spark.sql(f"DELETE FROM {CAT}.config.pipeline_configs WHERE source_name='zz_guard_test'")
    print("synthetic row removed; active sources:",
          spark.sql(f"SELECT count(*) FROM {CAT}.config.pipeline_configs WHERE active").collect()[0][0])

# COMMAND ----------

# DBTITLE 1,Clean up test artifacts (staging CSVs + batch_004)
# Test artifact cleanup (user-authorized via 'fix all the gaps'):
# - 3 staging CSVs in workspace home (upload-transfer intermediates, no longer needed)
# - batch_004_redelivery.csv from landing (test re-delivery file; already ingested + checkpointed into bronze,
#   so removing it does NOT affect bronze/silver/history — the data stays where it belongs)
import os
WS = "/Workspace/Users/kothahemanthsai58@gmail.com"
removed = []
for n in (1, 2, 3):
    p = f"{WS}/manufacturing_batch_{n}.csv"
    if os.path.exists(p):
        os.remove(p)
        removed.append(p)
B4 = "/Volumes/mdd_platform/landing/source_files/src_008_machine_vibration/manufacturing_batch_4_redelivery.csv"
try:
    dbutils.fs.rm(B4)
    removed.append(B4)
except Exception as e:
    print("batch_004 removal:", str(e)[:150])
print("removed:", len(removed))
for r in removed:
    print("  -", r)
print("landing folder now:", [f.name for f in dbutils.fs.ls("/Volumes/mdd_platform/landing/source_files/src_008_machine_vibration")])
print("workspace home CSVs remaining:", [f for f in os.listdir(WS) if f.endswith(".csv")])
print("src_008 data intact -> silver:", spark.table("mdd_platform.silver.src_008_machine_vibration").count(),
      "| history:", spark.table("mdd_platform.silver.src_008_machine_vibration_history").count())

# COMMAND ----------

# DBTITLE 1,Diagnose manual dirty-samples run
# Diagnose the manual dirty-samples run: job run history + dirty file + src_008 state
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
print("recent job runs (736782493336258):")
runs = sorted(list(w.jobs.list_runs(job_id=736782493336258)), key=lambda x: x.start_time or 0, reverse=True)[:5]
for r in runs:
    wall = ((r.end_time or r.start_time or 0) - (r.start_time or 0)) / 1000
    print(f"  run {r.run_id}: {r.state.life_cycle_state} / {r.state.result_state} | wall {wall:.0f}s")
P = "/Volumes/mdd_platform/landing/source_files/src_008_machine_vibration/manufacturing_dirty_samples.csv"
print("\ndirty file preview:")
print(open(P).read()[:800])
print("\nsrc_008 state after the manual run:")
print("bronze:", spark.table("mdd_platform.bronze.src_008_machine_vibration").count())
print("quarantine:", spark.table("mdd_platform.quarantine.src_008_machine_vibration").count())
print("\nrecent dq_metrics:")
spark.sql("SELECT rule_name, passed_rows, failed_rows, run_ts FROM mdd_platform.audit.dq_metrics WHERE source_name='src_008_machine_vibration' ORDER BY run_ts DESC").show(12)
print("recent run_log:")
spark.sql("SELECT layer, status, rows_read, rows_written, rows_quarantined, start_ts FROM mdd_platform.audit.run_log WHERE source_name='src_008_machine_vibration' ORDER BY start_ts DESC").show(8)

# COMMAND ----------

# DBTITLE 1,Pipeline run 3 — dirty samples quarantine test
# Pipeline run 3: complete the dirty-samples processing (prior manual run was USER_CANCELED before silver;
# the 10 dirty rows are already in bronze at 15:53, past the watermark, awaiting silver DQ evaluation)
from databricks.sdk import WorkspaceClient
import time
w = WorkspaceClient()
res = w.jobs.run_now(job_id=736782493336258)
while True:
    r = w.jobs.get_run(run_id=res.run_id)
    state = str(r.state.life_cycle_state)
    if not any(k in state for k in ("PENDING", "RUNNING", "QUEUED")):
        break
    time.sleep(30)
print("pipeline run 3 (dirty samples):", r.state.life_cycle_state, r.state.result_state, "| run_id:", res.run_id)

# COMMAND ----------

# DBTITLE 1,Verify run 3 — quarantine + dq results
# Verify run 3: dirty rows must split -> rule violators to quarantine, clean rows to silver
B = "mdd_platform.bronze.src_008_machine_vibration"
S = "mdd_platform.silver.src_008_machine_vibration"
H = "mdd_platform.silver.src_008_machine_vibration_history"
Q = "mdd_platform.quarantine.src_008_machine_vibration"
print("bronze:", spark.table(B).count())
print("silver:", spark.table(S).count(), "(2800 + any clean dirty rows with new composite keys)")
print("history:", spark.table(H).count())
print("quarantine:", spark.table(Q).count(), "(dirty rows failing rules — first quarantine for src_008)")
print("\nlatest dq evaluation:")
spark.sql("SELECT rule_name, passed_rows, failed_rows, run_ts FROM mdd_platform.audit.dq_metrics WHERE source_name='src_008_machine_vibration' ORDER BY run_ts DESC, rule_name").show(10)
print("quarantined rows:")
spark.table(Q).select("timestamp", "machine_id", "operator_email", "employee_phone", "status").show(10, truncate=40)
print("run_log latest:")
spark.sql("SELECT layer, status, rows_read, rows_written, rows_quarantined, start_ts FROM mdd_platform.audit.run_log WHERE source_name='src_008_machine_vibration' ORDER BY start_ts DESC").show(6)

# COMMAND ----------

# DBTITLE 1,Run patched config loader (quarantine PII)
# MAGIC %run "/MDD_DataPlatform/MDD Config Loader"

# COMMAND ----------

# DBTITLE 1,Verify quarantine PII masks + tags
# Verify quarantine PII masking + tags (all sources; src_008 shown in detail)
QT = "mdd_platform.quarantine.src_008_machine_vibration"
ddl = spark.sql(f"SHOW CREATE TABLE {QT}").collect()[0][0]
mask_lines = [l.strip() for l in ddl.splitlines() if "MASK" in l.upper() and "COLUMN" not in l.upper() or "MASK " in l]
print("quarantine DDL mask lines:")
for l in [x for x in ddl.splitlines() if "MASK" in x.upper()]:
    print(" ", l.strip())
print("\nmasked read (src_008 quarantine):")
spark.sql(f"SELECT employee_phone, supplier_contact, operator_email FROM {QT} LIMIT 5").show(truncate=45)
print("quarantine PII tags:")
spark.sql("""SELECT table_name, column_name, tag_name, tag_value FROM mdd_platform.information_schema.column_tags
  WHERE schema_name = 'quarantine' AND table_name = 'src_008_machine_vibration' ORDER BY column_name, tag_name""").show(truncate=False)
print("masked columns across ALL quarantine tables:")
spark.sql("""SELECT table_name, column_name, tag_value FROM mdd_platform.information_schema.column_tags
  WHERE schema_name = 'quarantine' AND tag_name = 'pii_type' ORDER BY table_name, column_name""").show(30, truncate=False)

# COMMAND ----------

# DBTITLE 1,Author src_008 transformations YAML
# Author silver.transformations for src_008 (cast+arithmetic, string ops, regex extract, regex replace)
# and declare the derived phone_digits column as PII so it is masked/tagged like any other column.
# Rule honored: transformations are SILVER-ONLY derived columns; _row_hash stays the bronze raw hash.
import yaml
P = "/Volumes/mdd_platform/config/config_files/src_008_machine_vibration.yaml"
cfg = yaml.safe_load(dbutils.fs.head(P))
cfg["silver"]["transformations"] = [
    {"column": "temperature_f", "type": "decimal(8,2)",
     "comment": "Derived sensor temperature in Fahrenheit (cast + arithmetic)",
     "expr": "round(cast(temperature_c as decimal(8,2)) * 1.8 + 32, 2)"},
    {"column": "operator_name_std", "type": "varchar(100)",
     "comment": "Normalized operator name (string ops)",
     "expr": "upper(trim(operator_name))"},
    {"column": "supplier_domain", "type": "varchar(100)",
     "comment": "Vendor support domain extracted from supplier email (regex extract)",
     "expr": "regexp_extract(supplier_contact, '@(.+)$', 1)"},
    {"column": "phone_digits", "type": "varchar(20)",
     "comment": "Derived staff phone digits - PII, masked at silver (regex replace)",
     "expr": "regexp_replace(employee_phone, '[^0-9]', '')"},
]
cfg["pii_columns"]["phone_digits"] = "phone"
dbutils.fs.put(P, yaml.safe_dump(cfg, sort_keys=False), overwrite=True)
print("YAML written. silver.transformations:")
for t in cfg["silver"]["transformations"]:
    print(f"  {t['column']:20s} {t['type']:12s} <- {t['expr']}")
print("pii_columns now:", cfg["pii_columns"])

# COMMAND ----------

# DBTITLE 1,Run config loader (validate transformations)
# MAGIC %run "/MDD_DataPlatform/MDD Config Loader"

# COMMAND ----------

# DBTITLE 1,Trigger runbook rebuild (full reprocess)
import time, datetime
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
print("start:", datetime.datetime.now().strftime("%H:%M:%S"), "UTC")
run = w.jobs.run_now("180017381911014", notebook_params={
    "source_name": "src_008_machine_vibration", "mode": "rebuild", "confirm": "true"})
print("rebuild run_id:", run.run_id)
state, rr = None, None
for _ in range(50):  # poll up to ~8.5 min (job continues server-side if cell returns first)
    time.sleep(10)
    rr = w.jobs.get_run(run.run_id)
    state = rr.state.life_cycle_state
    if state not in ("PENDING", "RUNNING", "QUEUED"):
        break
print("final lifecycle state:", state,
      "| result:", getattr(rr.state, "result_state", None))

# COMMAND ----------

# DBTITLE 1,Run config loader (post-rebuild PII catchup)
# MAGIC %run "/MDD_DataPlatform/MDD Config Loader"

# COMMAND ----------

# DBTITLE 1,Verify transformations end-to-end
S = "mdd_platform.silver.src_008_machine_vibration"
H = S + "_history"
B = "mdd_platform.bronze.src_008_machine_vibration"
Q = "mdd_platform.quarantine.src_008_machine_vibration"
from pyspark.sql import functions as F
print("counts:", {t: spark.table(x).count() for t, x in
      [("bronze", B), ("silver", S), ("history", H), ("quarantine", Q)]})
ddl = spark.sql(f"SHOW CREATE TABLE {S}").first()[0]
print("\ntransformed columns in silver DDL:")
for l in ddl.splitlines():
    if any(c in l for c in ("temperature_f", "operator_name_std", "supplier_domain", "phone_digits")):
        print("  ", l.strip())
print("\nsample transformed values (PII masked):")
spark.sql(f"""SELECT timestamp, machine_id, temperature_c, temperature_f, operator_name_std,
             supplier_domain, employee_phone, phone_digits FROM {S} ORDER BY timestamp LIMIT 5""").show(truncate=45)
hash_mismatch = (spark.table(S).alias("s").join(spark.table(B).alias("b"), ["timestamp", "machine_id"])
                 .where(F.col("s._row_hash") != F.col("b._row_hash")).count())
print("_row_hash mismatches silver-vs-bronze:", hash_mismatch,
      "(0 = hash carried from bronze, never recomputed)")
r = spark.sql(f"SELECT temperature_c, temperature_f FROM {S} WHERE temperature_c IS NOT NULL LIMIT 1").first()
exp = round(float(str(r["temperature_c"])) * 1.8 + 32, 2)
print(f"arithmetic cross-check: {r['temperature_c']}C -> {r['temperature_f']}F (expected {exp})")
print("\nSCD2 history (rebuild collapses within-run versions - documented behavior):")
spark.sql(f"""SELECT count(*) versions, sum(CASE WHEN _is_current THEN 1 ELSE 0 END) current_v,
               sum(CASE WHEN _effective_to IS NOT NULL THEN 1 ELSE 0 END) closed FROM {H}""").show()
spark.sql(f"""SELECT timestamp, machine_id, temperature_f, phone_digits, _is_current
             FROM {H} ORDER BY timestamp LIMIT 3""").show(truncate=45)
print("history phone_digits governance (derived PII in history):")
spark.sql(f"""SELECT table_name, column_name, tag_name, tag_value FROM mdd_platform.information_schema.column_tags
  WHERE table_name = 'src_008_machine_vibration_history' AND column_name = 'phone_digits'
  ORDER BY tag_name""").show(truncate=False)