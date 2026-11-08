# Databricks notebook source
# DBTITLE 1,Documentation
# MAGIC %md
# MAGIC # MDD Target Smoke Test — end-to-end proof in the target workspace
# MAGIC
# MAGIC Run via the *MDD Target Smoke Test (one-shot)* job, **after** the MDD Target Setup job succeeded.
# MAGIC
# MAGIC 1. Copies the bundled sample landing files (`bundle_assets/landing_samples/`) into the landing volume
# MAGIC 2. Triggers one orchestration run and waits
# MAGIC 3. Retries any ops views deferred by setup (e.g. quarantine-dependent views)
# MAGIC 4. Verifies and prints a PASS/FAIL summary, raising at the end if anything failed:
# MAGIC    * `audit.run_log`: latest run per source = success
# MAGIC    * silver row counts > 0 for the sampled sources
# MAGIC    * PII masks live (masked reads from silver)
# MAGIC    * key ops views + metric views queryable
# MAGIC    * two real dashboard dataset queries execute
# MAGIC
# MAGIC When all checks pass: unpause the four MDD alerts and open the deployed Ops Dashboard.

# COMMAND ----------

# DBTITLE 1,Helpers
# Cell 1 — helpers (same contract as the setup notebook)
import io, json, time, posixpath
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
CAT = "mdd_platform"
BUNDLE_WS = "/Workspace/MDD_DataPlatform/files"
SAMPLE_SOURCES = ["src_001_customers", "src_006_order_lines"]  # kept in sync with bundle_assets/landing_samples

def job_id_by_name(name):
    for j in w.jobs.list():
        if j.settings and j.settings.name == name:
            return j.job_id
    return None

def run_job_and_wait(name, timeout_minutes=25):
    jid = job_id_by_name(name)
    assert jid, f"Job not found: {name}"
    run = w.jobs.run_now(int(jid))
    deadline = time.time() + timeout_minutes * 60
    while True:
        r = w.jobs.get_run(run.run_id)
        state = str(r.state.life_cycle_state)
        if state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            result = str(r.state.result_state)
            print(f"{name}: {state} / {result}")
            if result != "SUCCESS":
                raise RuntimeError(f"Job {name} run {run.run_id} -> {result}: {r.state.state_message}")
            return run
        if time.time() > deadline:
            raise TimeoutError(f"Job {name} run {run.run_id} exceeded {timeout_minutes} min")
        time.sleep(15)

def ws_read_text(path):
    resp = w.workspace.download(path)
    data = resp.content if hasattr(resp, "content") else resp.read()
    return data.decode("utf-8")

print("helpers ready")

# COMMAND ----------

# DBTITLE 1,Load samples + run pipeline
# Cell 2 — copy sample landing files into the landing volume, run the pipeline once
for src in SAMPLE_SOURCES:
    src_dir = f"{BUNDLE_WS}/bundle_assets/landing_samples/{src}"
    dst_dir = f"/Volumes/{CAT}/landing/source_files/{src}"
    for obj in w.workspace.list(src_dir):
        if (obj.path or "").endswith(".csv"):
            text = ws_read_text(obj.path)
            fname = obj.path.split("/")[-1]
            dbutils.fs.put(f"{dst_dir}/{fname}", text, overwrite=True)
            print(f"  uploaded {src}/{fname} ({len(text)} bytes)")

run_job_and_wait("MDD Pipeline Orchestration Job", timeout_minutes=30)
print("pipeline run finished")

# COMMAND ----------

# DBTITLE 1,Retry deferred views
# Cell 3 — retry ops views that setup deferred (quarantine tables now have data)
OLD_JOB_IDS = ["736782493336258", "433265226360788"]
target_job_id = str(job_id_by_name("MDD Pipeline Orchestration Job"))

capture = json.loads(ws_read_text(f"{BUNDLE_WS}/bundle_assets/ops_views_capture.json"))
existing = {r.table_name for r in spark.sql(
    f"SELECT table_name FROM system.information_schema.tables WHERE table_catalog = '{CAT}' AND table_schema = 'ops'").collect()}
pending = {k: v for k, v in capture["views"].items() if k not in existing}
if pending:
    print(f"Retrying {len(pending)} deferred views: {list(pending)}")
    still = {}
    for attempt in range(4):
        if not pending:
            break
        progress = False
        for name, ddl in list(pending.items()):
            for old in OLD_JOB_IDS:
                ddl = ddl.replace(old, target_job_id)
            try:
                spark.sql(ddl)
                del pending[name]
                print(f"  applied {name}")
                progress = True
            except Exception as e:
                still[name] = str(e)[:180]
        if not progress:
            break
    if pending:
        print(f"STILL DEFERRED: {pending}")
else:
    print("all captured ops views already present")

# COMMAND ----------

# DBTITLE 1,Verification suite
# Cell 4 — verification suite (each check recorded, summary raises on any FAIL)
results = []
def check(name, fn):
    try:
        detail = fn()
        results.append((name, "PASS", detail))
    except Exception as e:
        results.append((name, "FAIL", str(e)[:200]))

def chk_runlog():
    bad = spark.sql(f"""SELECT count(*) FROM {CAT}.audit.run_log
        WHERE status <> 'success'""").first()[0]
    assert bad == 0, f"{bad} failed rows in run_log"
    n = spark.sql(f"SELECT count(*) FROM {CAT}.audit.run_log").first()[0]
    return f"{n} run_log rows, 0 failures"

def chk_silver(src):
    def _f():
        n = spark.sql(f"SELECT count(*) FROM {CAT}.silver.{src}").first()[0]
        assert n > 0, "silver table empty"
        return f"{n} silver rows"
    return _f

def chk_pii():
    df = spark.sql(f"SELECT email FROM {CAT}.silver.src_001_customers LIMIT 3").collect()
    vals = [r.email for r in df if r.email]
    assert vals, "no email values returned"
    assert all(("*" in v) or ("REDACTED" in v.upper()) or ("sha" in v.lower()) for v in vals), \
        f"email does not look masked: {vals}"
    return f"masked reads OK, e.g. {vals[0]}"

def chk_view(fqn):
    def _f():
        n = spark.sql(f"SELECT count(*) FROM {fqn}").first()[0]
        return f"{n} rows"
    return _f

def chk_mv_pipeline_kpis():
    df = spark.sql(f"""SELECT `Run Status` AS run_status, MEASURE(`Total Runs`) AS total_runs
        FROM {CAT}.ops.mv_pipeline_kpis GROUP BY `Run Status`""").collect()
    assert df, "metric view returned no rows"
    return ", ".join(f"{r.run_status}={r.total_runs}" for r in df)

def chk_dashboard_kpi_cards():
    sql = "SELECT active_sources FROM mdd_platform.ops.v_ops_kpi_summary LIMIT 1"
    r = spark.sql(sql).first()
    assert r is not None and r[0] > 0, "v_ops_kpi_summary empty"
    return f"active_sources={r[0]}"

def chk_dashboard_runs_over_time():
    n = spark.sql("""SELECT count(*) FROM (
        SELECT date_trunc('hour', start_ts) AS run_hour, status AS run_status, count(*) AS runs
        FROM mdd_platform.ops.v_pipeline_runs GROUP BY 1, 2)""").first()[0]
    assert n > 0, "runs_over_time query returned 0 rows"
    return f"{n} rows"

check("run_log all success", chk_runlog)
for src in SAMPLE_SOURCES:
    check(f"silver rows {src}", chk_silver(src))
check("PII masks (src_001 email)", chk_pii)
check("ops.v_pipeline_runs", chk_view(f"{CAT}.ops.v_pipeline_runs"))
check("ops.v_ops_kpi_summary", chk_view(f"{CAT}.ops.v_ops_kpi_summary"))
check("ops.v_dq_rule_metrics", chk_view(f"{CAT}.ops.v_dq_rule_metrics"))
check("ops.v_quarantine_detail", chk_view(f"{CAT}.ops.v_quarantine_detail"))
check("ops.v_storage_footprint", chk_view(f"{CAT}.ops.v_storage_footprint"))
check("ops.v_file_lifecycle_cost", chk_view(f"{CAT}.ops.v_file_lifecycle_cost"))
check("mv_pipeline_kpis (MEASURE)", chk_mv_pipeline_kpis)
check("dashboard dataset: kpi_cards", chk_dashboard_kpi_cards)
check("dashboard dataset: runs_over_time", chk_dashboard_runs_over_time)

print("\n================ SMOKE TEST SUMMARY ================")
failed = 0
for name, status, detail in results:
    print(f"  [{status}] {name}: {detail}")
    failed += (status == "FAIL")
print(f"\n{len(results) - failed}/{len(results)} checks passed")
assert failed == 0, f"{failed} smoke test checks FAILED — review output above before going live."

# COMMAND ----------

