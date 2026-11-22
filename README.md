# Airflow DAG Creator for GCP

**Open the app: https://mkk96.github.io/airflow-dag-creator-gcp/**

Build production-ready Airflow DAGs for **Cloud Composer 2 / 3** without writing them by hand. Pick a scenario, fill in a form, watch the Python DAG update as you type, and download a file you can drop straight into your Composer DAGs bucket.

- **Target:** Airflow 2.10, Python 3.11, `apache-airflow-providers-google` 12.0.0, the versions in Composer 2 / 3 images.
- **Private:** everything runs in your browser. Nothing you type is sent anywhere.
- **Tested:** every scenario is compiled, imported by Airflow and linted (ruff, sqlfluff) before each release.

---

## Quick start

1. Open the app and choose a **DAG scenario** from the dropdown at the top left.
2. Fill in the form sections (described below). Defaults are sensible, so you usually only change names, buckets and datasets.
3. In **Environment, naming & labels**, click **✨ Suggest names** to fill in the DAG id, bucket and dataset names from your org, environment and domain.
4. Fix anything marked in **red**. **Amber** items are suggestions.
5. Check the generated code in the preview on the right. Use the file tabs to see the DAG, any SQL files, the Variables file and the deployment README.
6. Click **⬇ .zip**, or **⬇ .py** for simple DAGs, and deploy it (see [Deploy the DAG to Composer](#deploy-the-dag-to-composer)).

## Available scenarios

| Scenario | What it builds |
|---|---|
| **GCS → BigQuery load** | Loads CSV, newline-delimited JSON, Parquet or Avro files from a bucket into a BigQuery table, idempotently into the run's date partition, with an optional data quality check. |

Coming soon (shown greyed out in the dropdown): BigQuery SQL transformation, BigQuery → GCS export, BigQuery table copy, GCS sensor → load → archive, Dataflow templates, Dataproc cluster jobs, Dataproc Serverless, Cloud SQL → BigQuery, Cloud Run / Cloud Functions, Pub/Sub, BigQuery data quality suites, Dataform, and a full ELT pipeline.

## Filling in the form

Sections can be collapsed. The badge on a section shows how many errors (red) or warnings (amber) it contains.

### DAG
- **DAG id**: also the file name (`<dag_id>.py`). Suggested format: `{domain}_{source}_to_{target}_{frequency}`, e.g. `sales_gcs_to_bq_daily`.
- **Description, Owner, Alert emails, Tags**: appear in the Airflow UI and in the DAG's documentation panel. The owner also becomes a cost label.

### Environment, naming & labels
- **Environment config source**: where the DAG gets environment-specific values (project, buckets, datasets, service accounts):
  - **Per-env config dict** (default): the values for dev, stg and prd are written into the DAG. Nothing else to set up.
  - **Airflow Variables**: the values go in a JSON file that you import into Airflow, one Variable per environment. Choose this if you manage config outside the code.
- **Values below are for env**: the environment the values you type belong to. Values for the other environments are derived by swapping the env code, e.g. `acme-dev-sales-landing` becomes `acme-prd-sales-landing`. Check the **Environment values** table in the generated README and edit it if your environments don't follow that pattern.
- **Org code, Domain**: used by **✨ Suggest names**.
- **Team, Cost center**: applied as GCP labels to every BigQuery job, so you can track cost per team.

### Schedule
- A preset (`@daily`, `@hourly`, …), **None** for manual or externally triggered runs, or **Custom cron expression**.
- **Start date** and **Timezone**, plus **Catchup** (off by default), **Max active runs** (1 by default) and a **DAG run timeout**.

### Retries, timeouts & alerts
- Retries, retry delay with optional exponential backoff, a per-task execution timeout, and **Email on failure** (needs email configured in Composer).

### GCP connection
- **GCP connection id**: usually `google_cloud_default`.
- **Project id**: the project that runs the jobs and owns the destination table.
- **Location**: BigQuery location such as `US`, `EU` or `northamerica-northeast1`.
- **Impersonation chain** (optional): service account(s) to run as. No keys are ever put in the DAG.
- **Use deferrable operators**: frees Airflow worker slots while BigQuery jobs run. Requires the **triggerer** to be enabled in your Composer environment. Untick it if yours doesn't have one.

### GCS → BigQuery fields
- **Source bucket**: bucket name only, without `gs://`.
- **Source object paths**: one per line, inside the bucket. Use wildcards and Airflow date macros so each run reads only its own files, e.g. `landing/orders/{{ ds_nodash }}/*.csv`.
- **File format** and its options: header rows, delimiter, quote, encoding, null marker, bad-record tolerance, and so on.
- **Schema source** (CSV / JSON): **Autodetect**, **Schema file** (paste or upload the output of `bq show --schema --format=prettyjson`), or **Use the existing table's schema**. Parquet and Avro carry their own schema.
- **Dataset, Table, Partitioning**: with daily or monthly partitioning, each run loads into its own partition (`orders$20240301`). Re-running a day replaces only that day.
- **Write disposition**: `WRITE_TRUNCATE` (recommended, safe to re-run), `WRITE_APPEND` (re-runs create duplicates, so you'll see a warning) or `WRITE_EMPTY`.
- **Clustering columns**, **Create disposition**, **Schema update options**.
- **Post-load check**: **Row count > 0** for the loaded partition, a **Custom SQL check** (any query returning one row whose values must all be true), or **None**.

### Errors vs warnings
- **Red errors** break an official GCP or Airflow rule, such as invalid bucket names, bad cron syntax or invalid project ids. Downloads are disabled until they're fixed.
- **Amber warnings** flag a naming convention or a risky choice, e.g. a non-standard name or an append that isn't idempotent. You can still download.

## Downloading

| Button | Gives you | Use when |
|---|---|---|
| **⬇ .zip** | The DAG, its SQL files, the Variables file (if used) and a deployment README, laid out like your Composer bucket | The DAG has SQL files or uses Airflow Variables. The app highlights this button when needed. |
| **⬇ .py** | Just `<dag_id>.py` | Simple DAGs with no extra files |
| **Copy** | The DAG code on your clipboard | Pasting into your own repo |

What's inside the `.zip`:

```
<dag_id>.zip
├── dags/
│   ├── <dag_id>.py
│   └── sql/<dag_id>/01_check_<table>_row_count.sql
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
4. **Grant IAM roles** to the Composer service account, or to the impersonated one:
   - `roles/bigquery.jobUser` on the project that runs the jobs
   - `roles/bigquery.dataEditor` on the destination dataset
   - `roles/storage.objectViewer` on the source bucket
   - with impersonation: `roles/iam.serviceAccountTokenCreator` on the impersonated service account

The DAG appears in the Airflow UI within a minute or two.

## Save and reuse your settings

- **Export JSON** saves the whole form to a file. Commit it next to your DAG to regenerate it later.
- **Import JSON** loads a saved file back into the form.
- The app also remembers your last form in this browser. **Reset to defaults** at the bottom of the form clears it.

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
