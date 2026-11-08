# Databricks notebook source
# DBTITLE 1,MDD Platform — Table Maintenance
# MAGIC %md
# MAGIC # MDD Platform — Table Maintenance (OPTIMIZE + VACUUM)
# MAGIC
# MAGIC Daily maintenance for the `mdd_platform` catalog: compacts Delta files (OPTIMIZE) and removes stale deleted files (VACUUM). Retention is 168 hours, matching the `delta.deletedFileRetentionDuration = interval 7 days` table property set on all bronze/silver/quarantine tables by the pipeline. Run by the scheduled maintenance job daily at 02:00 UTC.

# COMMAND ----------

# DBTITLE 1,Maintenance — OPTIMIZE + VACUUM all pipeline tables
# Catalog: bundle variable (maintenance job passes catalog_name; default mdd_platform)
dbutils.widgets.text("catalog_name", "mdd_platform", "MDD catalog (bundle passes ${var.catalog_name})")
CAT = dbutils.widgets.get("catalog_name").strip() or "mdd_platform"
RETAIN_HOURS = 168  # matches delta.deletedFileRetentionDuration = 7 days

tables = [f"{CAT}.{r[0]}.{r[1]}" for r in spark.sql(f"""
  SELECT table_schema, table_name FROM {CAT}.information_schema.tables
  WHERE table_schema IN ('bronze','silver','quarantine','audit')""").collect()]

print(f"maintaining {len(tables)} tables in {CAT}")
for t in sorted(tables):
    before = spark.sql(f"DESCRIBE DETAIL {t}").collect()[0]
    spark.sql(f"OPTIMIZE {t}")
    spark.sql(f"VACUUM {t} RETAIN {RETAIN_HOURS} HOURS")
    d = spark.sql(f"DESCRIBE DETAIL {t}").collect()[0]
    print(f"{t}: files {before.numFiles} -> {d.numFiles}, size {before.sizeInBytes} -> {d.sizeInBytes}")
print("maintenance complete")