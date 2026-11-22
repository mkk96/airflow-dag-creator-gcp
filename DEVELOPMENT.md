# Development guide

For contributors working on the code of **Airflow DAG Creator for GCP**. To *use* the app, see [README.md](README.md) or open https://mkk96.github.io/airflow-dag-creator-gcp/.

## Scenarios

| # | Scenario | Status |
|---|---|---|
| 1–14 | All 14 scenarios: GCS → BQ, GCS sensor → load → archive, Cloud SQL → BQ, BQ SQL, BQ → GCS, BQ copy, Dataflow, Dataproc, Dataproc Serverless, Cloud Run / Functions, Pub/Sub, DQ checks, Dataform, full ELT | ✅ Available (one file each in `src/scenarios/`) |

## What the generated DAGs look like

- `with DAG(...) as dag:`, with a header comment naming the scenario and generator version, plus a `doc_md` that lists the pipeline steps, owner and contact.
- `catchup=False`, `max_active_runs=1`, `dagrun_timeout`, tags.
- `default_args` set retries, `retry_delay`, exponential backoff, `execution_timeout` and an `on_failure_callback`.
- Env-specific values (project, buckets, datasets, service accounts) are never written into task logic. The DAG reads `DEPLOY_ENV` (dev / stg / prd), and Jinja resolves the values at run time from either:
  - a **per-env config dict** in the DAG (`{{ cfg.<key> }}`, the drop-in default), or
  - an **Airflow Variable** per env (`{{ var.json.get(cfg_var).<key> }}`, imported from `config/<dag_id>_variables.json`).
- Parse time runs no `Variable.get()`, API calls or database queries.
- Loads are idempotent: `WRITE_TRUNCATE` into the run's partition (`table$YYYYMMDD` / `table$YYYYMM`).
- `deferrable=True` is set where supported, labels (`env, team, owner, cost_center, dag_id`) go on every BigQuery job, and related tasks are grouped in a `TaskGroup`.
- SQL lives in `.sql` files under `dags/sql/<dag_id>/` and is loaded through `template_searchpath`.

When a DAG needs more than its `.py` (SQL files, scripts, Variables), download the `.zip`:

```
<dag_id>.zip
├── dags/
│   ├── <dag_id>.py
│   └── sql/<dag_id>/01_check_<table>_row_count.sql
├── scripts/<dag_id>/          (future scenarios: upload to the code bucket)
├── config/<dag_id>_variables.json
└── README.md                  (gcloud commands, Variables, IAM roles)
```

## Run locally

Requires Node 26 (see `.nvmrc`).

```bash
npm install
npm run dev          # http://localhost:5173
```

| Command | What it does |
|---|---|
| `npm run dev` | Dev server with hot reload |
| `npm run build` | Type-check and build the static site into `dist/` |
| `npm test` | Unit tests (vitest): escaping, validators, naming, scenario contract |
| `npm run lint` / `npm run typecheck` | ESLint / TypeScript |
| `npm run generate` | Write every scenario sample into `generated/` |

### Validate generated DAGs locally

Airflow doesn't officially support native Windows, so use Linux, macOS, WSL or Docker (CI runs these checks on every PR). In practice the pytest suite also runs on native Windows Python with a warning.

```bash
npm run generate
bash scripts/test-dags.sh            # Docker: same steps as CI
```

To run them without Docker (Python 3.11):

```bash
pip install "apache-airflow==2.10.5" "apache-airflow-providers-google==12.0.0" apache-airflow-providers-apache-beam apache-airflow-providers-openlineage pytest \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt
pytest tests/dags
pip install -r requirements-lint.txt   # preferably in a separate venv
ruff check generated tests && ruff format --check generated tests && sqlfluff lint generated
```

The pytest suite ([`tests/dags/test_generated_dags.py`](tests/dags/test_generated_dags.py)) checks that:
1. `py_compile` succeeds on every generated DAG
2. `DagBag` imports it with no import errors, under `DEPLOY_ENV` = dev, stg and prd
3. there are no cycles, and `dag_id` matches the file name
4. every templated field renders, with Variables supplied as `AIRFLOW_VAR_*`
5. the standards hold: labels, owner, retries, timeouts, failure callback, doc_md, tags, and sensor mode and timeout

## Add a scenario

1. Create `src/scenarios/<name>.ts` that exports a `Scenario` (see [`src/scenarios/types.ts`](src/scenarios/types.ts)):
   - `id`, `title`, `description`, `naming: { source, target }`
   - `sections` and `fields`. Common fields are added automatically, so don't repeat them. Mark env-specific fields with `envSpecific: true`.
   - `samples`: one or more named parameter sets that cover the variants. CI generates and tests every one.
   - `generate(params)`: create a `DagContext`, add imports, build the tasks with the helpers in [`src/lib/python.ts`](src/lib/python.ts), register SQL with `ctx.addSql()`, and return `ctx.assemble({...})`.
2. Register it in [`src/scenarios/registry.ts`](src/scenarios/registry.ts) and remove its entry from `planned.ts`.
3. Run `npm test && npm run generate`, then the DAG checks above.

Rules that also apply to scenarios: put user input into Python only through `pyStr` / `pyList` / `pyDict` / `pyCall` and the other helpers, and use `ctx.ref()` only in fields listed in the operator's `template_fields`. The full list is in [CLAUDE.md](CLAUDE.md).

## Naming conventions

All naming rules live in [`src/config/naming.ts`](src/config/naming.ts):

- `OFFICIAL`: GCP and Airflow rules. A violation is an **error** and blocks download.
- `TEAM`: your convention (patterns like `{org}-{env}-{domain}-{purpose}`, dataset layers, verbs, frequencies). A violation is a **warning**.

To adopt your team's convention, edit `TEAM`. Scenario code doesn't change. **Suggest names** fills dag_id, buckets and datasets from org, env, domain, source/target and the schedule.

## Deploy to GitHub Pages

1. Push the repo to GitHub.
2. Go to **Settings → Pages → Build and deployment → Source** and select **GitHub Actions**.
3. Push to `main`. [`deploy.yml`](.github/workflows/deploy.yml) runs the full test workflow first and deploys only if it passes.

Workflows:

- [`test.yml`](.github/workflows/test.yml) runs on every pull request. It runs typecheck, lint, unit tests, build and sample generation. It then installs Airflow and providers-google using the official constraints file (versions from `runtime.config.json`) and runs pytest, ruff and sqlfluff.
- [`deploy.yml`](.github/workflows/deploy.yml) runs on push to `main`. It runs `test.yml`, then builds with the Pages base path and deploys.

## Change the target runtime

Edit [`runtime.config.json`](runtime.config.json). The UI badge, the generated headers and CI's constraints URL all read from it. After bumping `airflowVersion`, set `googleProviderVersion` to the version pinned in the new constraints file. Otherwise CI's install fails, which is intentional.

## Project layout

```
runtime.config.json          target runtime (single source of truth)
src/config/                  naming.ts (all naming rules), runtime.ts, schedules.ts
src/lib/python.ts            safe Python literal and expression emitter (ruff-format compatible)
src/scenarios/               types, common (shared params, default_args, env config, assembly), scenarios
src/validation/              field validators and form validation
src/components/              React UI
scripts/generate-samples.ts  writes generated/ for the tests
tests/unit/                  vitest
tests/dags/                  pytest against generated DAGs
```
