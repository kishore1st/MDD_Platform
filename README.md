# MDD DataPlatform — Cross-Workspace Deployment Bundle

Deploys the complete MDD metadata-driven pipeline framework to another Databricks workspace in **60–90 minutes**: 14 notebooks, 9 jobs, the 8-page Ops Dashboard, 4 SQL alerts, 7 YAML source configs, sample data, plus one-shot setup/cleanup/smoke-test jobs. **Multi-client:** the catalog name is parameterized — deploy with `--var="catalog_name=<client>"` and everything (catalog, volumes, configs, views, deployed notebook copies, dashboard, alert queries) lands under the client's own catalog.

## What's in the bundle

| Path | Contents |
|---|---|
| `databricks.yml`, `resources/` | Bundle manifest + job, dashboard, and alert definitions (`catalog_name` / `warehouse_name` variables) |
| `MDD *.py` (14 notebooks) | Framework, config loader, on-demand + schema-evolution runbooks, maintenance, 2 ops view builders, ops costing views, deploy prep, 2 onboarding test harnesses |
| `MDD Target Setup / Cleanup / Smoke Test` | Deployment utility notebooks (run as one-shot jobs) |
| `MDD Deploy Prep (Capture …)` | Run once in the SOURCE workspace to capture ops-view DDL, configs, and samples into `bundle_assets/` + `bundle_configs/` |
| `bundle_assets/ops_views_capture.json` | Captured `SHOW CREATE VIEW` DDL for all `ops.v_*` views + metric-view proofs |
| `bundle_assets/mv_*.yaml` | Metric-view definitions (3 KPI metric views, recreated for the target) |
| `bundle_configs/*.yaml` | The 7 source YAML configs (src_001 … src_008) |
| `bundle_assets/landing_samples/` | Sample CSVs for the smoke test |
| `MDD Platform — Ops Dashboard.lvdash.json` | Ops Dashboard source (alert queries live in `resources/alerts.yml`, parameterized with `${var.catalog_name}`) |

Not included: the Workspace Usage Dashboard (account template, unrelated), the source-workspace `.dbalert.json` files (alerts deploy from `resources/alerts.yml`), and src_007_creditcard (never created).

## Prerequisites

* A laptop with the Databricks CLI (`pip install databricks-cli` or brew) — v0.281+ recommended (see Troubleshooting)
* Target workspace: you need **CREATE CATALOG** rights (workspace admin typically) and a **SQL warehouse** (any size; default name expected: `Starter Warehouse`)
* Git access to this repository (or, alternatively, export rights on the source workspace)

## Deploy runbook (60–90 min total)

**Step 0 — Capture (in the SOURCE workspace, ~5 min).** Run the notebook `MDD Deploy Prep (Capture Ops Views + Configs)` top-to-bottom. It writes `bundle_assets/` + `bundle_configs/` — verify the final validation cell prints all OK.

**Step 1 — Get the bundle (~2 min).** Clone this repo — it is the canonical package (notebook `.py` sources, `databricks.yml`, `resources/`, YAML configs, captured assets, and metric-view YAMLs are all versioned here):

```
git clone https://github.com/kishore1st/MDD_Platform.git
cd MDD_Platform
```

(Alternative: from the source workspace, `databricks workspace export-dir /MDD_DataPlatform ./mdd_platform_bundle` produces the same layout.)

**Step 2 — Authenticate to the TARGET workspace (~1 min).**

```
databricks auth login --host https://<TARGET-WORKSPACE-URL>
```

**Step 3 — Validate + deploy (~5 min).**

```
databricks bundle validate --strict -t prod
databricks bundle deploy -t prod --var="catalog_name=<client>"
```

`catalog_name` defaults to `mdd_platform` — override per client. Same for `warehouse_name` if the target warehouse differs from `Starter Warehouse`.

**Step 4 — Bootstrap the target environment (~15 min).**

```
databricks bundle run mdd_target_setup -t prod
```

Creates the client catalog (from `catalog_name`) with schemas + volumes, loads the 7 YAML configs **with the client catalog substituted into each YAML**, runs the config loader + one framework run, then applies all ops views / metric views and patches **every deployed notebook + the Ops Dashboard** with the target's orchestration job ID and the client catalog. Fully idempotent — safe to re-run.

**Step 5 — Already-used target? Clean it first (optional, ~2 min).** If the target workspace already captured MDD data and you want a fresh start with your new file names, run the job `MDD Target Cleanup (mode)` from the target's Jobs UI (Run with parameters): `mode=full` + `confirm=true`, then re-run `mdd_target_setup`. `mode=soft` keeps configs/audit but truncates data + checkpoints + watermarks. `mode=dry` (default) only prints the plan.

**Step 6 — Smoke test (~10 min).**

```
databricks bundle run mdd_target_smoke_test -t prod
```

Uploads sample landing files, triggers one pipeline run, and verifies: run_log all-success, silver rows > 0, PII masks live, ops + metric views queryable, and dashboard dataset SQL executes. All 13 checks must print PASS.

**Step 7 — Go live (~5 min).**

* Unpause the 4 MDD alerts (deployed PAUSED to avoid firing on an empty environment)
* Open the deployed **MDD Platform — Ops Dashboard** (first render of the File Lifecycle page takes 1–2 min — it scans the landing volume; expected)
* Optional: add a **file-arrival trigger** to the `MDD Config Loader Job` on `/Volumes/<catalog_name>/config/config_files` so new/changed YAML configs auto-load
* The hourly orchestration, 02:00 UTC maintenance, and 03:00 UTC ops-views-refresh schedules deploy UNPAUSED and start working immediately

**Step 8 — Process your new files (different names).**

1. Author the YAML config for each new source (copy an existing config and rename `source_name`, landing folder, targets — remember to quote types containing commas, e.g. `'decimal(10,2)'`)
2. Drop the YAML into `/Volumes/<catalog_name>/config/config_files/` (then run the Config Loader job, or add the file-arrival trigger)
3. Drop your new-named files into `/Volumes/<catalog_name>/landing/source_files/<source>/`
4. The next hourly orchestration run (or a manual run) onboards them — zero framework code changes
5. Re-run `MDD Ops Views Refresh Job` (or wait for the daily 03:00 UTC run) so costing views include the new sources

## Timing summary

| Step | Time |
|---|---|
| Capture (source) | 5 min |
| Clone + auth + validate + deploy | 10 min |
| Setup | 15 min |
| (Optional) cleanup | 2 min |
| Smoke test | 10 min |
| Go-live config | 5 min |
| **Total** | **~45–60 min** (well inside the 1–2 hr budget) |

## Troubleshooting

* **`bundle validate` fails on `warehouse_id`** — variable references in `lookups` need CLI v0.281+; or hardcode the target warehouse name in `databricks.yml` (`lookup: warehouse: <name>`)
* **CREATE CATALOG permission error in setup** — ask the target workspace admin to run Step 4 or grant you CREATE CATALOG on the metastore
* **Setup reports DEFERRED views** — normal for quarantine-dependent views; the smoke test retries them after sample data lands
* **Dashboard pages empty right after deploy** — the ops views refresh nightly at 03:00 UTC and cost/billing telemetry lags ~1 day in a new workspace (documented behavior, same as source)
* **Redeploys overwrite the setup notebook's patches** (job IDs + catalog in every notebook + dashboard) — simply re-run `mdd_target_setup` after any redeploy (it is idempotent)

## Notes

* Alert thresholds are deployment defaults (failed_runs > 0, quarantine rate > 5%, stale sources > 0, rescue warnings > 0) — edit `resources/alerts.yml` before deploy to change
* `system.billing.usage`-based costing in the target starts accruing from deploy time; the ~1-day telemetry lag in a new workspace is expected
* The dashboard deploys with `embedCredentials` per the source file; adjust sharing in the target after deploy
* This repo (github.com/kishore1st/MDD_Platform) is the canonical package. Before a release, re-run `MDD Deploy Prep` in the source workspace and re-sync changed files into the Git folder (bundle_configs currently holds 7 YAMLs — src_009 ships after the next Deploy Prep run)
* The `mdd-lakehouse-framework` assistant skill (in the authoring user's `~/.assistant/skills/` folder) encodes onboarding/extension guidance — copy it to client users' skills folders for AI-assisted onboarding

## CI/CD (GitHub Actions)

* **`ci.yml`** — runs `databricks bundle validate --strict -t prod` on every pull request and push to `main`. Uses repo-level secrets for a **source-workspace service principal**.
* **`deploy.yml`** (`deploy-client`) — manual workflow-dispatch deploy to a client: pick a **GitHub environment** (e.g. `client-acme`) that holds that client's secrets, and pass `catalog_name` (+ `warehouse_name` if different). It validates, then deploys; run `mdd_target_setup` + `mdd_target_smoke_test` afterwards (CLI or the client's Jobs UI).

**Secrets setup (per workspace):**

1. Databricks workspace → Settings → Identity and access → Service principals → New — give it workspace admin on client workspaces (needs CREATE CATALOG + deploy rights)
2. On the SP's OAuth tab → Generate secret
3. GitHub → Settings → Secrets and variables → Actions: add `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` — at **repo level** for `ci.yml` (source workspace), and inside a **per-client environment** for `deploy.yml` (Settings → Environments → New environment → add the same three secrets)
4. Recommended: protect `main` (require the `ci` workflow to pass before merge) and require PRs for all changes