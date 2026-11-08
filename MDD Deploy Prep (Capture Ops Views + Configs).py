# Databricks notebook source
# DBTITLE 1,Documentation
# MAGIC %md
# MAGIC # MDD Deploy Prep — capture ops-view DDL, configs, and sample data for the deployment bundle
# MAGIC
# MAGIC Runs **in the SOURCE workspace** (here), once, as part of packaging. Produces everything the bundle needs that lives only in this workspace's metastore and volumes:
# MAGIC
# MAGIC | Output | Destination |
# MAGIC |---|---|
# MAGIC | `SHOW CREATE VIEW` DDL for every `mdd_platform.ops.v_*` view | `/Workspace/MDD_DataPlatform/bundle_assets/ops_views_capture.json` |
# MAGIC | Metric-view verification queries (exact display names / dims / measures) | same capture JSON (for authoring `mv_*.yaml`) |
# MAGIC | The 7 source YAML configs (from the config volume) | `/Workspace/MDD_DataPlatform/bundle_configs/*.yaml` |
# MAGIC | Sample landing files (`batch_001.csv` for src_001_customers + src_006_order_lines) | `/Workspace/MDD_DataPlatform/bundle_assets/landing_samples/<source>/` |
# MAGIC
# MAGIC All writes are new files under the bundle — nothing existing is modified. Read-only with respect to catalog data.

# COMMAND ----------

# DBTITLE 1,Capture v_* view DDL
# Cell 1 — capture DDL for every v_* view in mdd_platform.ops
import json
CAT = "mdd_platform"
view_names = [r.table_name for r in spark.sql(f"""SELECT table_name FROM system.information_schema.tables
    WHERE table_catalog = '{CAT}' AND table_schema = 'ops' AND table_name RLIKE '^v_'""").collect()]
view_names.sort()
print(f"views found: {view_names}")

captured = {}
for v in view_names:
    try:
        ddl = spark.sql(f"SHOW CREATE TABLE {CAT}.ops.{v}").first()[0]
        captured[v] = ddl
    except Exception as e:
        print(f"  WARN could not capture {v}: {str(e)[:150]}")
print(f"captured {len(captured)}/{len(view_names)} view DDLs")
assert len(captured) >= 15, "too few views captured — investigate before packaging"

# COMMAND ----------

# DBTITLE 1,Metric view proofs
# Cell 2 — metric views: SHOW CREATE (expected to fail) + proof queries with display names.
# The mv_*.yaml bundle files are authored from these proofs.
capture_mv = {}
for mv in ("mv_data_quality_kpis", "mv_governance_kpis", "mv_pipeline_kpis"):
    try:
        ddl = spark.sql(f"SHOW CREATE TABLE {CAT}.ops.{mv}").first()[0]
        capture_mv[mv] = {"show_create": ddl}
        print(f"{mv}: SHOW CREATE succeeded")
    except Exception as e:
        print(f"{mv}: SHOW CREATE unsupported ({str(e)[:90]}) — recording proof queries instead")
        capture_mv[mv] = {"show_create_error": str(e)[:200]}

proofs = {
    "mv_pipeline_kpis": "SELECT `Run Layer` AS layer, `Run Status` AS status, `Source` AS src, MEASURE(`Total Runs`) AS total_runs, MEASURE(`Rows Read`) AS rows_read, MEASURE(`Avg Duration Seconds`) AS avg_dur FROM mdd_platform.ops.mv_pipeline_kpis GROUP BY `Run Layer`, `Run Status`, `Source`",
    "mv_data_quality_kpis": "SELECT `Source` AS src, `Rule` AS rule, `Rule Status` AS rule_status, MEASURE(`Rules Evaluated`) AS rules_eval, MEASURE(`Failure Rate Pct`) AS fail_pct FROM mdd_platform.ops.mv_data_quality_kpis GROUP BY `Source`, `Rule`, `Rule Status`",
    "mv_governance_kpis": "SELECT `Table` AS tbl, `PII Type` AS pii_type, `Governance Status` AS gov, MEASURE(`PII Columns`) AS pii_cols, MEASURE(`Coverage Pct`) AS cov FROM mdd_platform.ops.mv_governance_kpis GROUP BY `Table`, `PII Type`, `Governance Status`",
}
for mv, sql in proofs.items():
    try:
        rows = spark.sql(sql).collect()
        capture_mv[mv]["proof_rows"] = len(rows)
        capture_mv[mv]["proof_sample"] = str(rows[:3])
        print(f"{mv}: proof query OK ({len(rows)} rows) — dims/measures confirmed")
    except Exception as e:
        capture_mv[mv]["proof_error"] = str(e)[:200]
        print(f"{mv}: PROOF FAILED — {str(e)[:150]}")

# COMMAND ----------

# DBTITLE 1,Copy configs + samples, write capture JSON
# Cell 3 — copy configs + sample landing files into the bundle, write the capture JSON.
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat
w = WorkspaceClient()
BUNDLE = "/Workspace/MDD_DataPlatform"

import io, posixpath
from databricks.sdk.errors.platform import DatabricksError

def put_ws_file(ws_path, text):
    """Create-only write: skips identical existing files, errors on differing content (never overwrites)."""
    w.workspace.mkdirs(posixpath.dirname(ws_path))
    data = text.encode("utf-8")
    try:
        w.workspace.upload(ws_path, data, format=ImportFormat.AUTO, overwrite=False)
    except DatabricksError as e:
        if "already exists" not in str(e).lower() and "exists" not in str(e).lower():
            raise
        resp = w.workspace.download(ws_path)
        if resp.read() != data:
            raise RuntimeError(f"{ws_path} already exists with DIFFERENT content — refusing to overwrite; inspect manually")
        print(f"  skip (identical content already present): {ws_path}")

def get_vol_text(vol_path):
    return dbutils.fs.head(vol_path)

# 3a. YAML configs -> bundle_configs/
cfg_files = [f.path for f in dbutils.fs.ls("/Volumes/mdd_platform/config/config_files") if f.path.endswith(".yaml")]
print(f"config YAMLs: {[p.split('/')[-1] for p in cfg_files]}")
for p in cfg_files:
    put_ws_file(f"{BUNDLE}/bundle_configs/{p.split('/')[-1]}", get_vol_text(p))

# 3b. Sample landing files -> bundle_assets/landing_samples/<source>/
# Landing layout can nest one extra level (<src>/<src>/batch_001.csv) or be flat — handle both.
def find_landing_file(src, fname):
    root = f"/Volumes/mdd_platform/landing/source_files/{src}"
    candidates = [f"{root}/{fname}", f"{root}/{src}/{fname}"]
    for c in candidates:
        try:
            dbutils.fs.head(c, 1)
            return c
        except Exception:
            continue
    raise FileNotFoundError(f"{fname} not found under {root} (tried {candidates})")

SAMPLES = [("src_001_customers", "batch_001.csv"), ("src_006_order_lines", "batch_001.csv")]
for src, fname in SAMPLES:
    text = get_vol_text(find_landing_file(src, fname))
    put_ws_file(f"{BUNDLE}/bundle_assets/landing_samples/{src}/{fname}", text)
    print(f"sample {src}/{fname}: {len(text)} chars")

# 3c. Capture JSON -> bundle_assets/
capture_doc = {"views": captured, "metric_views": capture_mv,
               "source_orchestration_job_id": "736782493336258"}
put_ws_file(f"{BUNDLE}/bundle_assets/ops_views_capture.json", json.dumps(capture_doc, indent=2))
print("ops_views_capture.json written")

# COMMAND ----------

# DBTITLE 1,Validate capture
# Cell 4 — validation: read back everything the bundle depends on and print sizes.
import io
def ws_read_text(path):
    return w.workspace.download(path).read().decode("utf-8")

checks = []
cap = json.loads(ws_read_text(f"{BUNDLE}/bundle_assets/ops_views_capture.json"))
checks.append(("capture views", len(cap["views"]), len(cap["views"]) >= 15))
checks.append(("capture metric proofs", len(cap["metric_views"]), len(cap["metric_views"]) == 3))

configs = [o.path for o in w.workspace.list(f"{BUNDLE}/bundle_configs") if o.path.endswith(".yaml")]
checks.append(("bundle_configs YAMLs", len(configs), len(configs) >= 7))

samples = [o.path for o in w.workspace.list(f"{BUNDLE}/bundle_assets/landing_samples")]
n_samples = 0
for s in samples:
    n_samples += len([o for o in w.workspace.list(s) if o.path.endswith(".csv")])
checks.append(("landing sample CSVs", n_samples, n_samples >= 2))

print("== CAPTURE VALIDATION ==")
ok = True
for name, value, passed in checks:
    print(f"  [{'OK ' if passed else 'BAD'}] {name}: {value}")
    ok &= passed
assert ok, "capture validation failed — fix above before deploying"
print("\nCAPTURE COMPLETE — bundle_assets and bundle_configs are ready for export.")

# COMMAND ----------

