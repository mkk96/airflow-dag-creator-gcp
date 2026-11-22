# Airflow DAG Creator for GCP

**Open the app: https://mkk96.github.io/airflow-dag-creator-gcp/**

Build production-ready Airflow DAGs for **Cloud Composer 2 / 3** without writing them by hand. Pick a scenario, fill in a form, watch the Python DAG update as you type, and download a file you can drop straight into your Composer DAGs bucket.

- **Target:** Airflow 2.10, Python 3.11, `apache-airflow-providers-google` 12.0.0, the versions in Composer 2 / 3 images.
- **Private:** everything runs in your browser. Nothing you type is sent anywhere.
- **Tested:** every scenario is compiled, imported by Airflow and linted (ruff, sqlfluff) before each release.

---

## Quick start

1. Open the app and click **Change** on the scenario card (top left) to open the scenario gallery. Search or browse by category, then pick one.
2. Fill in the form sections (described below). Defaults are sensible, so you usually only change names, buckets and datasets.
3. In **Environment, naming & labels**, click **Suggest names** to fill in the DAG id, bucket and dataset names from your org, environment and domain.
4. Fix anything marked in **red**. **Amber** items are suggestions. The status pill above the code shows the count; click it to jump to the first issue.
5. Check the generated code in the preview on the right. Use the file tabs to see the DAG, any SQL files, the Variables file and the deployment README.
6. Click **.zip**, or **.py** for simple DAGs (the recommended one is highlighted), and deploy it (see [Deploy the DAG to Composer](#deploy-the-dag-to-composer)).

## Available scenarios

| Category | Scenario | What it builds |
|---|---|---|
| Ingestion | **GCS → BigQuery load** | Loads CSV, JSON, Parquet or Avro files from a bucket into a table, into the run's date partition, with an optional row-count check. |
| Ingestion | **GCS sensor → load → archive** | Waits for files to land, loads them into BigQuery, then moves (or copies) them to a date-stamped archive folder. |
| Ingestion | **Cloud SQL → GCS → BigQuery** | Exports a Postgres/MySQL query to CSV in GCS, then loads it into BigQuery. |
| BigQuery | **BigQuery SQL transformation** | Runs your SQL: a SELECT written into the run's partition, or a MERGE / DML / DDL statement. |
| BigQuery | **BigQuery → GCS export** | Exports a table/partition (extract job) or a query result (EXPORT DATA) to GCS as CSV, JSON, Avro or Parquet. |
| BigQuery | **BigQuery → BigQuery copy** | Copies tables or the run's partition between datasets or projects. |
| Processing | **Dataflow template job** | Launches a Google-provided or custom Classic / Flex template. |
| Processing | **Dataproc cluster job** | Creates a cluster, runs a PySpark or Spark job, and always deletes the cluster. |
| Processing | **Dataproc Serverless batch** | Submits a serverless PySpark or Spark batch. |
| Pipelines | **Dataform workflow** | Compiles a Dataform repository and runs all actions, selected tags or selected tables. |
| Pipelines | **Full ELT pipeline** | Sensor → load staging → MERGE into the final table → archive files → alert on failure. |
| Triggers & messaging | **Cloud Run job / Cloud Function** | Executes a Cloud Run job (with optional overrides) or invokes a Cloud Function. |
| Triggers & messaging | **Pub/Sub publish / pull sensor** | Publishes a message, waits for messages, or waits and then publishes. |
| Data quality | **BigQuery data quality checks** | Row count, not-null, uniqueness, freshness, metric-value and custom SQL checks on a table. |

## Filling in the form

Sections can be collapsed. Each section header shows a green dot when it's complete, or a count of errors (red) and warnings (amber). Fields tagged **ENV** are environment-specific.

### DAG
- **DAG id**: also the file name (`<dag_id>.py`). Suggested format: `{domain}_{source}_to_{target}_{frequency}`, e.g. `sales_gcs_to_bq_daily`.
- **Description, Owner, Alert emails, Tags**: appear in the Airflow UI and in the DAG's documentation panel. The owner also becomes a cost label.

### Environment, naming & labels
- **Environment config source**: where the DAG gets environment-specific values (project, buckets, datasets, service accounts):
  - **Per-env config dict** (default): the values for dev, stg and prd are written into the DAG. Nothing else to set up.
  - **Airflow Variables**: the values go in a JSON file that you import into Airflow, one Variable per environment. Choose this if you manage config outside the code.
- **Values below are for env**: the environment the values you type belong to. Values for the other environments are derived by swapping the env code, e.g. `acme-dev-sales-landing` becomes `acme-prd-sales-landing`. Check the **Environment values** table in the generated README and edit it if your environments don't follow that pattern.
- **Org code, Domain**: used by **Suggest names**.
- **Team, Cost center**: applied as GCP labels to every BigQuery job, Dataproc cluster/batch and Dataflow job, so you can track cost per team.

### Schedule
- A preset (`@daily`, `@hourly`, …), **None** for manual or externally triggered runs, or **Custom cron expression**.
- **Start date** and **Timezone**, plus **Catchup** (off by default), **Max active runs** (1 by default) and a **DAG run timeout**.

### Retries, timeouts & alerts
- Retries, retry delay with optional exponential backoff, a per-task execution timeout, and **Email on failure** (needs email configured in Composer).

### GCP connection
- **GCP connection id**: usually `google_cloud_default`.
- **Project id**: the project that runs the jobs and owns the destination table.
- **Location / region**: BigQuery location such as `US`, `EU` or `northamerica-northeast1`. Dataflow, Dataproc, Cloud Run, Cloud Functions and Dataform need a **region**, not `US`/`EU`; the form tells you if so.
- **Impersonation chain** (optional): service account(s) to run as. No keys are ever put in the DAG.
- **Use deferrable operators**: frees Airflow worker slots while long jobs run and sensors wait. Requires the **triggerer** to be enabled in your Composer environment. Untick it if yours doesn't have one (sensors then use reschedule mode). Short BigQuery check queries always run non-deferrable so their cost labels are kept.

### Errors vs warnings
- **Red errors** break an official GCP or Airflow rule, such as invalid bucket names, bad cron syntax or invalid project ids. Downloads are disabled until they're fixed.
- **Amber warnings** flag a naming convention or a risky choice, e.g. a non-standard name or an append that isn't idempotent. You can still download.

### Using environment values in free text
In SQL, job arguments and template parameters you can write `{{ cfg.<key> }}` (e.g. `{{ cfg.project_id }}`, `{{ cfg.dataset }}`, `{{ cfg.data_bucket }}`) to insert the value for whichever environment the DAG runs in. Airflow macros such as `{{ ds }}` and `{{ ds_nodash }}` also work. The field help lists the keys available.

## Scenario guide

### GCS → BigQuery load
- **Source bucket**: bucket name only, without `gs://`.
- **Source object paths**: one per line, inside the bucket. Use wildcards and date macros so each run reads only its own files, e.g. `landing/orders/{{ ds_nodash }}/*.csv`.
- **File format** and its options: header rows, delimiter, quote, encoding, null marker, bad-record tolerance.
- **Schema source** (CSV / JSON): **Autodetect**, **Schema file** (paste or upload the output of `bq show --schema --format=prettyjson`), or **Use the existing table's schema**. Parquet and Avro carry their own schema.
- **Dataset, Table, Partitioning**: with daily or monthly partitioning, each run loads into its own partition (`orders$20240301`). Re-running a day replaces only that day.
- **Write disposition**: `WRITE_TRUNCATE` (recommended, safe to re-run), `WRITE_APPEND` (re-runs create duplicates, so you'll see a warning) or `WRITE_EMPTY`. Plus clustering columns, create disposition and schema update options.
- **Post-load check**: row count > 0 for the loaded partition, a custom SQL check (one row whose values must all be true), or none.

### GCS sensor → load → archive
- **Landing bucket / Landing folder / File name pattern**: where files arrive. Put a date macro such as `{{ ds }}` in the folder; the pattern allows one `*` (e.g. `*.csv`).
- **Wait for files up to / Check for files every**: how long the sensor waits before failing the run, and how often it looks.
- **File format, options and schema**: as in GCS → BigQuery.
- **Destination**: dataset, table, and hourly / daily / monthly partitioning. Each run replaces its own partition; `WRITE_APPEND` shows a warning.
- **Post-load check**: row count > 0 for the loaded partition, or none.
- **Archive bucket / Archive folder / Archive action**: processed files are moved (or copied) to a date-stamped folder, only after a successful load. The archive folder can't be inside the landing folder.

### Cloud SQL → GCS → BigQuery
- **Database engine, Cloud SQL instance id, Instance project (optional), Database**: what to export from.
- **Export query**: one SELECT, saved as a .sql file. Use `{{ ds }}` for incremental exports or leave it out for a full snapshot. List the columns in schema order and leave off the trailing semicolon.
- **Serverless export (offload)**: runs the export on a temporary instance so the primary database isn't loaded (extra cost).
- **Export bucket / Export folder / Gzip**: where Cloud SQL writes `<table>.csv`. The instance's own service account needs write access to that bucket.
- **CSV load options and schema**: null marker and max bad records; a JSON schema or the existing table's schema (exports have no header row).
- **Destination and post-load check**: as in GCS → BigQuery.

### BigQuery SQL transformation
- **What does the SQL do?**: **SELECT → destination table** writes the result into the run's partition. **Statement** is for MERGE, INSERT, DELETE, CREATE or scripts that write their own target.
- **SQL**: paste or upload a .sql file; it's saved next to the DAG. Use `{{ cfg.project_id }}`, `{{ cfg.dataset }}`, `{{ cfg.source_dataset }}`, `{{ params.table }}` and `{{ ds }}`.
- **Source dataset** (optional) and **Destination dataset** are env-specific. **Destination table** takes no `$` decorator.
- **Partitioning** (daily, monthly or none), **Partition column**, **Clustering columns** (up to 4).
- **Write / Create disposition** and **Schema update options**. `WRITE_TRUNCATE` into a partition is idempotent; `WRITE_APPEND` shows a warning.

### BigQuery → GCS export
- **Export from**: a table or its run-date partition (extract job, no query cost), or a query result (your SELECT wrapped in `EXPORT DATA`).
- **Dataset** (env-specific) with **Table** and **What to export** (daily or monthly partition, or the whole table); or **Query** and **Export name** in query mode.
- **Destination bucket** (env-specific) and **Destination path**: exactly one `*`, and include the run date, e.g. `exports/orders/dt={{ ds }}/part-*.csv.gz`.
- **Delete this run's previous files first**: removes earlier files with the same prefix so re-runs leave no stale files.
- **File format** (CSV, JSON, Avro, Parquet), **Compression** (must suit the format), **Header row** and **Delimiter** (CSV), **Avro logical types**.

### BigQuery → BigQuery copy
- **Source project** (optional, defaults to the job project), **Source dataset** (env-specific), **Source tables** (one per line; several are combined).
- **Destination project** (optional), **Destination dataset** (env-specific), **Destination table**.
- **What to copy**: the run's daily partition (`table$YYYYMMDD` → `table$YYYYMMDD`), the monthly partition, or the whole table.
- **Write / Create disposition**: `WRITE_TRUNCATE` is idempotent; `WRITE_APPEND` shows a warning. Copy jobs scan no bytes.

### Dataflow template job
- **Template type**: Classic or Flex. Google-provided templates come in both; your own Flex templates are a container spec `.json` file.
- **Template bucket + path**: e.g. `dataflow-templates-<region>` + `latest/GCS_Text_to_BigQuery`, or your env-specific code bucket + `templates/flex/my_pipeline.json`. The region comes from **Location / region**.
- **Template parameters**: one `key=value` per line, as in the template's docs. Macros like `{{ ds_nodash }}` and `{{ cfg.data_bucket }}` work.
- **Completion**: wait until the job finishes (batch jobs), or submit only (streaming jobs, usually with schedule None).
- **Workers**: temp bucket (required, env-specific), optional data bucket, worker service account, machine type, max workers, network or subnetwork (full URL for Shared VPC), and internal IPs only.
- The job is named `<dag_id>-<run timestamp>` and carries the cost labels.

### Dataproc cluster job
- **Job type**: PySpark or Spark (main class + jar URIs). For PySpark a starter script is generated in `scripts/<dag_id>/`; replace its logic and upload it to the code bucket.
- **Arguments, extra Python files / jars, Spark properties**: one per line. Arguments can use `{{ ds }}` and `{{ cfg.data_bucket }}`. The starter script expects `--input`, `--output` and `--run-date`.
- **Cluster**: image version, master/worker machine types, primary workers (2 or more, or 0 for a single node), Spot secondary workers, boot disk size, optional autoscaling policy.
- **Idle auto-delete (minutes)**: a safety net that removes the cluster if the delete task never runs. The cluster is always deleted after the job, even when the job fails, and the DAG run still fails if the job failed.
- **Service account & network**: cluster service account, subnetwork (name or Shared VPC URL), internal IPs only, network tags. Buckets: code (required), data and staging (optional), all env-specific.

### Dataproc Serverless batch
- **Job type**: PySpark (starter script generated into `scripts/<dag_id>/`) or Spark (main class + jars), with the same argument, file and property fields as the cluster job.
- **Runtime version**: e.g. `2.2` (Spark 3.5). Spark properties such as `spark.executor.instances=4` set the resources.
- **Service account & network**: batch service account, subnetwork (needs Private Google Access), network tags, optional staging bucket.
- Each run and each retry gets its own batch, `<dag_id>-<run timestamp>-<try>`, labelled for cost tracking.

### Dataform workflow
- **Repository id / Compile from**: a branch, tag or commit SHA, or a development workspace (for testing). The repository is in **Project id** / **Location / region**.
- **Run as service account** (optional, env-specific): the identity for the workflow's BigQuery jobs.
- **Compilation overrides**: default project (env-specific), dataset suffix, table prefix, and variables (`name=value`, macros like `{{ ds }}` allowed).
- **Run**: all actions, actions with tags, or selected tables (`dataset.name`). Optionally include dependencies or dependents, and fully refresh incremental tables (costly, so it warns).
- **How to wait**: start the run and wait with a sensor (success / failure states, timeout and check interval), or start and wait inside one task. Dataform has no job labels; set them in SQLX `bigquery: { labels }`.

### Full ELT pipeline
- **Landing files, format and staging schema**: as in GCS sensor → load → archive.
- **Staging dataset / Staging table**: each run replaces its own daily partition; the table is created on the first run.
- **Final dataset / Final table / Merge key columns / Columns to merge**: the upsert target and its keys. Rows with a NULL key are skipped.
- **Latest-version column** (optional): keeps only the newest row per key within a run.
- **Create the final table if missing** (with optional partition column): created from the staging column types and clustered by the keys.
- **Check before MERGE** (rows present, no NULL keys), archive settings, and **When a task fails**: log only, or log and email the alert emails. Either way the run is marked failed.

### Cloud Run job / Cloud Function
- **Target**: execute a deployed Cloud Run job and wait for it, or invoke a 1st gen Cloud Function. Project and region come from **Project id** and **Location / region**.
- **Job name**: the deployed job's name. **Container name** is only needed for multi-container jobs.
- **Container arguments / Environment variable overrides**: replace the args and add or replace `NAME=value` variables for this run. Macros like `{{ ds }}` are allowed; never put secrets here.
- **Task count / Task timeout override**: 0 keeps the job's own settings.
- **Function name / Input data (JSON)**: the JSON is sent as the function's request data (macros allowed). The call API is meant for low traffic and only works with 1st gen functions; for 2nd gen, use a Cloud Run job.

### Pub/Sub publish / pull sensor
- **Mode**: publish a message, wait for messages, or wait and then publish.
- **Topic / Subscription**: IDs only, env-specific. **Suggest names** fills them as `{org}-{env}-{domain}-events[-composer]`.
- **Message body** (JSON or text): the same in every run, because Pub/Sub data isn't templated. Put run-specific values in **Message attributes** (`key=value`, macros allowed). The optional **Ordering key** enables ordered delivery.
- **Max messages / Acknowledge / Store in XCom**: the sensor succeeds on the first message and keeps up to N messages in XCom, so keep N small. Acknowledging stops redelivery.
- **Wait timeout / Check every**: the sensor fails after the timeout.

### BigQuery data quality checks
- **Dataset** (env-specific) and **Table**. **Rows to check**: the run date's rows (by **Date column**, or `_PARTITIONTIME` if blank) or the whole table.
- **Wait for the data first** and **Wait timeout**: start only once the run-date partition (daily-partitioned tables) or the table exists.
- **Checks to run**, each its own task, running in parallel:
  - row count (minimum rows)
  - not-null (columns)
  - uniqueness (key columns)
  - freshness (TIMESTAMP column and maximum age in hours, measured against the end of the run date)
  - metric value (SQL expression, expected value, tolerance %)
  - custom SQL (one SELECT returning one row where every value must be TRUE)
- Deferrable mode is off by default for this scenario, because deferrable checks lose their cost labels.

## Downloading

| Button | Gives you | Use when |
|---|---|---|
| **.zip** | The DAG, its SQL files, scripts, the Variables file (if used) and a deployment README, laid out like your Composer bucket | The DAG has SQL or script files or uses Airflow Variables. The app highlights this button when needed. |
| **.py** | Just `<dag_id>.py` | Simple DAGs with no extra files |
| **Copy** | The DAG code on your clipboard | Pasting into your own repo |

What's inside the `.zip`:

```
<dag_id>.zip
├── dags/
│   ├── <dag_id>.py
│   └── sql/<dag_id>/01_<step>.sql      (SQL used by the DAG)
├── scripts/<dag_id>/<job>.py          (PySpark starter script: upload to your code bucket)
├── config/<dag_id>_variables.json     (only with "Airflow Variables")
└── README.md                          (commands, environment values, IAM roles for this DAG)
```

## Deploy the DAG to Composer

Each download includes a README with these commands already filled in for your DAG.

```bash
COMPOSER_ENV="your-composer-env"
COMPOSER_REGION="your-region"
DAGS_PREFIX=$(gcloud composer environments describe "$COMPOSER_ENV" \
  --location "$COMPOSER_REGION" --format="value(config.dagGcsPrefix)")
```

1. **Tell each Composer environment which env it is.** Do this once per environment. The value is `dev`, `stg` or `prd`.
   ```bash
   gcloud composer environments update "$COMPOSER_ENV" --location "$COMPOSER_REGION" \
     --update-env-variables=DEPLOY_ENV=prd
   ```
2. **Airflow Variables mode only:** import the Variables file.
   ```bash
   gcloud storage cp config/<dag_id>_variables.json "${DAGS_PREFIX%/dags}/data/"
   gcloud composer environments run "$COMPOSER_ENV" --location "$COMPOSER_REGION" \
     variables import -- /home/airflow/gcs/data/<dag_id>_variables.json
   ```
3. **Upload the DAG** and its SQL folder. Run this from the unzipped folder:
   ```bash
   gcloud storage cp --recursive dags/* "$DAGS_PREFIX/"
   ```
4. **Dataproc scenarios only:** upload the script to your code bucket (the exact command, with your bucket, is in the generated README):
   ```bash
   gcloud storage cp --recursive scripts/<dag_id> gs://<your-code-bucket>/scripts/
   ```
5. **Grant IAM roles** to the Composer service account, or to the impersonated one. The generated README lists the exact roles for your scenario. For example, GCS → BigQuery needs:
   - `roles/bigquery.jobUser` on the project that runs the jobs
   - `roles/bigquery.dataEditor` on the destination dataset
   - `roles/storage.objectViewer` on the source bucket
   - with impersonation: `roles/iam.serviceAccountTokenCreator` on the impersonated service account

The DAG appears in the Airflow UI within a minute or two.

## Save and reuse your settings

- **Export** saves the whole form to a JSON file. Commit it next to your DAG to regenerate it later.
- **Import** loads a saved file back into the form.
- The app also remembers your last scenario and form in this browser. **Reset to defaults** at the bottom of the form clears it.

## What the generated DAGs follow

- Use `with DAG(...) as dag:`, with clear `verb_object` task ids, explicit dependencies and a documentation panel (owner, contact, pipeline steps).
- Default to `catchup=False` and `max_active_runs=1`, with retries plus exponential backoff, timeouts and a failure callback.
- Do no slow work when Airflow parses the file. Values are resolved when tasks run.
- Re-run safely: partition overwrite instead of blind appends.
- Label every BigQuery job (`env`, `team`, `owner`, `cost_center`, `dag_id`) for cost tracking.
- Keep SQL in separate `.sql` files, not inside the Python.
- Contain no credentials. They use only the Airflow connection and optional impersonation.

## Troubleshooting

| Symptom | Fix |
|---|---|
| Download buttons are disabled | A section has red errors. Look for the red badge. |
| `DEPLOY_ENV must be one of ('dev', 'stg', 'prd')` in Airflow | Set the `DEPLOY_ENV` environment variable on the Composer environment (step 1). |
| A deferrable task stays in the *deferred* state | Enable the triggerer in Composer, or regenerate with **Use deferrable operators** unticked. |
| `KeyError` / Variable not found when the task runs | Airflow Variables mode: import the Variables file (step 2). |
| `TemplateNotFound` for a `.sql` file | Upload the whole `dags/` folder from the zip, including `sql/<dag_id>/`. |
| No email alerts | Configure email (SendGrid / SMTP) in Composer, and add alert emails in the form. |
