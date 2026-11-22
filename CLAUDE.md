# CLAUDE.md — conventions for this repo

Airflow DAG Creator for GCP: a static React + Vite + TypeScript + Tailwind app (GitHub Pages)
that generates Cloud Composer DAGs in the browser. These rules are mandatory for every change.

## Target runtime
- Set in ONE place: `runtime.config.json`. Defaults are Airflow 2.10.5, Python 3.11 and apache-airflow-providers-google 12.0.0 (the version pinned by the 2.10.5 constraints file), for Composer 2 / 3.
- Use only `airflow.providers.google.*` imports. Never `airflow.contrib`. Use `schedule=`, never `schedule_interval=`.
- Before using an operator argument, check it against the provider source for the pinned version, especially which arguments are in `template_fields`. Example: `GCSToBigQueryOperator.labels` labels the table, not the job, and its `project_id` isn't templated. That's why scenario 1 uses `BigQueryInsertJobOperator` with a `load` configuration.

## Architecture
- One scenario per file: `src/scenarios/<name>.ts` exports a `Scenario` (`src/scenarios/types.ts`). Adding a scenario means one file plus one line in `src/scenarios/registry.ts`, and removing its entry from `planned.ts`.
- Common parameters, `default_args`, env config, labels, the failure callback, the doc_md/header and the bundle README all come from `src/scenarios/common.ts` (`COMMON_FIELDS`, `DagContext`). Never copy them into a scenario.
- Every scenario defines `samples` that cover its variants. `npm run generate` writes them to `generated/`, and CI tests every one.
- Naming rules live only in `src/config/naming.ts`. An `OFFICIAL` violation is an error (blocks download). A `TEAM` violation is a warning.

## Safe code generation (non-negotiable)
- User input reaches Python ONLY through `src/lib/python.ts` helpers (`pyStr`, `pyInt`, `pyBool`, `pyList`, `pyDict`, `pyCall`, `pyName`, `pyIndex`, `pyFString`, `pyFromJson`, `pyDocstring`, `comment`). Never use template literals or concatenation to build Python source from user values.
- Identifiers (task variables, kwargs, names) must pass `pyIdent`. Derive them with `toSnakeIdentifier`.
- Fields default to rejecting Jinja delimiters. Set `allowJinja: true` only where the user is meant to write templates (object paths, custom SQL, non-templated text like descriptions).
- `ctx.ref(key)` returns a Jinja expression. Use it only in arguments listed in that operator's `template_fields`. For non-templated arguments, use static values or module constants.
- `template_ext` trap: a templated string ending in one of the operator's `template_ext` (e.g. `.json` / `.sql` for `BigQueryInsertJobOperator`) is loaded as a template FILE. Pass such values through `guardTemplateExt()` (e.g. `gs://b/*.json` becomes `gs://b/*{{ '.json' }}`). The only strings that should end in `.sql` are intended SQL file paths.
- Generated files must pass `ruff check` and `ruff format --check` (config in `pyproject.toml`). The emitter keeps containers on one line when they fit in 88 columns, and otherwise explodes them one per line with a magic trailing comma. Use `{ explode: true }` for operator calls and dicts. Generated SQL must pass `sqlfluff lint` (bigquery dialect, `.sqlfluff`).

## Generated DAG standards
- Use `with DAG(...) as dag:`, a header comment (scenario id and generator version), clear `verb_object` task_ids, and explicit `>>` dependencies. Task variable names equal the task_ids (ruff AIR001).
- No top-level work: no `Variable.get()`, hooks, API or DB calls at parse time. Reading `os.environ` for `DEPLOY_ENV` is the only parse-time lookup. Values resolve through Jinja at run time.
- Env config: the user picks a per-env `ENV_CONFIG` dict (`user_defined_macros={"cfg": CONFIG}`) or an env-prefixed JSON Airflow Variable (`user_defined_macros={"cfg_var": CONFIG_VARIABLE}`). SQL files start with `ctx.sqlPrelude()` and use `{{ cfg.<key> }}` in both modes.
- Make tasks idempotent: write into partitions (`table$YYYYMMDD`) or MERGE. `WRITE_APPEND` only when the user chooses it, and it must show a warning.
- Use `deferrable=True` (`ctx.deferrable()`) where supported. A sensor that isn't deferrable uses `mode="reschedule"` plus a `timeout`.
- `default_args` set retries, `retry_delay`, `retry_exponential_backoff`, `execution_timeout` and `on_failure_callback`. The DAG sets `catchup=False` and `max_active_runs=1` by default, plus `dagrun_timeout`, tags, `doc_md`, owner and contact.
- Group related tasks with `TaskGroup` (not around a single task). Don't pass large data through XCom.
- No credentials or keys, ever: only `gcp_conn_id` and the optional `impersonation_chain`.
- Set `LABELS` (env, team, owner, cost_center, dag_id; lowercase) on every BigQuery job, Dataproc cluster/batch and Dataflow job.
- Never embed long SQL in Python. Each statement goes in `dags/sql/<dag_id>/NN_name.sql` via `ctx.addSql()`. Scripts (PySpark/Beam) go under `scripts/<dag_id>/` and are referenced by gs:// URI.
- A bundle with SQL, scripts or Variables downloads as a `.zip` in the Composer layout: `dags/`, `dags/sql/<dag_id>/`, `scripts/<dag_id>/`, `config/<dag_id>_variables.json`, `README.md`.
- Unique runtime resource names (Dataproc clusters, Dataflow / Cloud Run jobs) are lowercase with hyphens and include the dag_id and a run-date suffix.

## Docs
- `README.md` is the user guide for the hosted site (https://mkk96.github.io/airflow-dag-creator-gcp/). It covers using the app and deploying its output only, never local setup or code. When a scenario or field changes, update its section there.
- Developer docs (local setup, tests, adding scenarios, CI/deploy) go in `DEVELOPMENT.md`.

## Deploy to GCP (src/lib/deploy/)
- Static site, so deploys run in the browser with the user's Google token (GIS token client, cloud-platform scope, kept in memory only) or through a generated Cloud Shell script. Both use `buildPlan()`: Variable → scripts → SQL → DAG last.
- Never deploy when the environment's `DEPLOY_ENV` doesn't match the target env, unless the user opted to set it. Composer replaces the whole envVariables map, so always merge.
- The shell script embeds files as base64 and single-quotes every value (`shq`). No user text may reach the shell unquoted.
- The OAuth Client ID comes from the `GOOGLE_OAUTH_CLIENT_ID` repo variable (`VITE_GOOGLE_CLIENT_ID` at build time). Never a client secret.

## Provider gotchas found so far (providers-google 12.0.0)
- `BigQueryCheckOperator` / `BigQueryValueCheckOperator` drop `labels` on their deferrable path. Emit them with `deferrable=False`; labels are the standard.
- The Google provider's Dataflow and Dataproc modules import the apache-beam and openlineage providers. CI and `scripts/test-dags.sh` install both (Composer images ship them).
- `DataformWorkflowInvocationStateSensor` isn't deferrable, and only `workflow_invocation_id` is templated. The Dataform scenario emits a small subclass and uses reschedule mode.
- Pub/Sub message `data` must be bytes and isn't templated. Put run-specific values in attributes.
- `CloudSQLExportInstanceOperator.body` is templated without `template_ext`. Load the query with `{% include 'NN_x.sql' %}`.
- Sensors that wait a long time need `execution_timeout` > sensor timeout, because Airflow caps deferral at `execution_timeout`.

## Shared helpers that live in scenario files (move to common.ts when convenient)
- `gcsSensorLoadArchive.ts`: landing, format, destination and archive field factories, load-config builders, sensor/archive/check tasks, `>>` chain wrapping. Used by `cloudsqlToBigquery.ts` and `fullElt.ts`.
- `bigquerySqlTransform.ts`: SQL heuristics, `{{ cfg.x }}` / `{{ params.x }}` checks, the dependency-chain emitter. Used by the other BigQuery scenarios.
- `dataflowTemplate.ts`: `{{ cfg.x }}` resolution in free text, key=value parsing, region/subnetwork/machine-type validators. `dataprocEphemeral.ts`: Spark job fields and spec, PySpark starter script. Used by both Dataproc scenarios.

## Checks before you finish
```bash
npm run typecheck && npm run lint && npm test && npm run build && npm run generate
# Linux/macOS/WSL or CI (Airflow doesn't support native Windows):
bash scripts/test-dags.sh   # pytest (DagBag ×3 envs, cycles, rendering, standards) + ruff + sqlfluff
```
- CI: `.github/workflows/test.yml` runs on PRs. `deploy.yml` runs on `main`, calls test.yml and deploys to Pages only when it passes.
- `generated/` is build output (gitignored). Don't commit it.
