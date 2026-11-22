/**
 * Tooltip explanations for form parameters, keyed by field key.
 *
 * A field's tooltip shows (in order of preference) its own `tooltip`, the
 * entry here, then its `help` text — plus its options, default, and whether
 * it is env-specific or accepts Airflow macros. Keys shared across scenarios
 * (write_disposition, csv_*, partition_type, …) are explained once, here.
 * tests/unit/scenarios.test.ts fails if any field ends up with no explanation.
 */

export interface FieldDoc {
  /** What the parameter does and when to change it. */
  text: string;
  /** Optional example value. */
  example?: string;
}

export const FIELD_DOCS: Record<string, FieldDoc> = {
  // ---- DAG ---------------------------------------------------------------------
  dag_id: {
    text: "Unique name of the DAG in Airflow and the name of the generated file (<dag_id>.py). Letters, digits, '_', '.', '-' only. The team convention is {domain}_{source}_to_{target}_{frequency}.",
    example: "sales_gcs_to_bq_daily",
  },
  description: {
    text: "One-line summary shown next to the DAG in the Airflow UI and at the top of its documentation panel (doc_md).",
  },
  owner: {
    text: "The team that owns the DAG. Shown in the Airflow UI (default_args.owner) and applied, lowercased, as the `owner` GCP cost label.",
    example: "data-eng",
  },
  email: {
    text: "Addresses that receive failure alerts when 'Email on failure' is on. The first one is also linked as the owner contact in the Airflow UI.",
    example: "data-alerts@example.com",
  },
  tags: {
    text: "Labels used to filter DAGs in the Airflow UI. Use a few broad ones such as the domain and the systems involved.",
    example: "sales",
  },

  // ---- Environment, naming & labels -----------------------------------------------
  config_mode: {
    text: "Where env-specific values (projects, buckets, datasets, service accounts) live. 'Config dict' writes the dev/stg/prd values into the DAG file, with nothing else to set up. 'Airflow Variables' stores them in one JSON Variable per environment that you import into Airflow, which is handy when ops manages config. Either way the DAG picks dev/stg/prd from the DEPLOY_ENV environment variable and Jinja resolves the values when tasks run, never when the file is parsed.",
  },
  env: {
    text: "Which environment the values you type belong to. Values for the other two environments are derived by swapping this env code inside them (acme-dev-sales → acme-prd-sales). Check the 'Environment values' table in the generated README if your names don't follow that pattern.",
  },
  org: {
    text: "Short lowercase code for your organisation. Used by 'Suggest names' as the first part of bucket names.",
    example: "acme",
  },
  domain: {
    text: "Business domain the pipeline serves, e.g. sales, finance, marketing. Used by 'Suggest names' for the dag_id, bucket and dataset names.",
    example: "sales",
  },
  team: {
    text: "Owning team, applied as the `team` GCP label on every BigQuery job, Dataproc cluster/batch and Dataflow job so costs can be grouped by team in billing reports. Lowercase letters, digits, '_' and '-' only.",
    example: "data-eng",
  },
  cost_center: {
    text: "Finance cost center, applied as the `cost_center` GCP label on every job/resource for chargeback reporting. Lowercase letters, digits, '_' and '-' only.",
    example: "cc-1234",
  },

  // ---- Schedule ----------------------------------------------------------------------
  schedule_preset: {
    text: "How often Airflow starts a run. Presets: @hourly, @daily (midnight), @weekly, @monthly. 'None' means the DAG only runs when triggered manually or by another system; 'Custom' lets you enter a cron expression.",
  },
  schedule_cron: {
    text: "Five-field cron expression evaluated in the DAG timezone: minute hour day-of-month month day-of-week. Each run processes the interval that just ended.",
    example: "0 6 * * *  (every day at 06:00)",
  },
  start_date: {
    text: "First date the schedule applies from. With catchup off, Airflow only schedules the most recent interval, so a past date is fine. Never use a dynamic date such as 'today'.",
    example: "2025-01-01",
  },
  timezone: {
    text: "IANA timezone for the schedule and start date. Cron times are interpreted in this zone, including daylight-saving changes. Macros such as {{ ds }} are based on the run's logical date.",
    example: "America/Toronto",
  },
  catchup: {
    text: "If on, Airflow creates a run for every missed interval between start_date and now (a backfill). Keep it off unless you deliberately want history loaded; loads are idempotent, so backfills are safe but can be costly.",
  },
  max_active_runs: {
    text: "How many runs of this DAG may run at the same time. 1 (the standard) keeps runs in order and avoids two runs writing the same tables concurrently.",
  },
  dagrun_timeout_minutes: {
    text: "Maximum time a whole DAG run may take. When exceeded, the run is marked failed and its running tasks are stopped. Set it comfortably above the normal runtime.",
  },

  // ---- Retries, timeouts & alerts ---------------------------------------------------------
  retries: {
    text: "How many times a failed task is retried automatically before it is marked failed. Applied to every task via default_args. Tasks are idempotent, so retries are safe.",
  },
  retry_delay_minutes: {
    text: "Wait time before the first retry of a failed task.",
  },
  retry_exponential_backoff: {
    text: "Doubles the wait between successive retries (5, 10, 20 min…), up to the max retry delay. Helps with transient quota and rate-limit errors.",
  },
  max_retry_delay_minutes: {
    text: "Upper bound for the retry wait when exponential backoff is on.",
  },
  execution_timeout_minutes: {
    text: "Maximum time any single task may run before Airflow stops it and treats it as failed (then retried). Applied to every task via default_args; long sensors get their own longer timeout automatically.",
  },
  email_on_failure: {
    text: "Send an email to the alert addresses when a task fails after its last retry. Requires email (SendGrid or SMTP) to be configured in Composer. A failure callback also logs the failure details in every case.",
  },

  // ---- GCP connection -----------------------------------------------------------------------
  gcp_conn_id: {
    text: "Airflow connection used to call Google APIs. In Composer, 'google_cloud_default' uses the environment's service account. Only change it if your team defines a separate connection.",
    example: "google_cloud_default",
  },
  project_id: {
    text: "GCP project that runs the jobs and (usually) owns the destination data. Env-specific: the dev/stg/prd values are derived from what you type here.",
    example: "acme-prd-data",
  },
  location: {
    text: "Where the jobs run. For BigQuery a location such as US, EU or northamerica-northeast1 (must match your datasets). Dataflow, Dataproc, Cloud Run, Cloud Functions and Dataform need a region, not US/EU.",
    example: "northamerica-northeast1",
  },
  impersonation_chain: {
    text: "Optional service account(s) the tasks act as instead of the Composer service account. The Composer service account needs roles/iam.serviceAccountTokenCreator on it. With several entries, each impersonates the next and the last one is used. No keys are ever stored.",
    example: "etl-runner@acme-prd-data.iam.gserviceaccount.com",
  },
  deferrable: {
    text: "Deferrable operators and sensors hand their waiting to the Airflow triggerer, freeing worker slots while jobs run. Requires the triggerer to be enabled in Composer; turn it off if yours has none (sensors then use reschedule mode).",
  },

  // ---- Files & formats (shared by load scenarios) -----------------------------------------------
  source_format: {
    text: "Format of the files to load. CSV and newline-delimited JSON need a schema (autodetect, a schema file, or the existing table). Parquet and Avro carry their own schema.",
  },
  csv_skip_leading_rows: {
    text: "Number of header lines at the top of each CSV file to skip. Usually 1 for files with a header row, 0 for files without.",
  },
  csv_field_delimiter: {
    text: "Character that separates columns in the CSV files.",
  },
  field_delimiter: {
    text: "Character that separates columns in the exported CSV files.",
  },
  csv_quote: {
    text: "Character used to quote values that contain the delimiter or line breaks. Leave empty if values are never quoted.",
  },
  csv_encoding: {
    text: "Character encoding of the CSV files. UTF-8 is the most common; legacy exports are often ISO-8859-1.",
  },
  csv_null_marker: {
    text: "Text that represents a NULL value in the files, e.g. \\N or NULL. Empty means BigQuery's default (empty field).",
  },
  csv_allow_quoted_newlines: {
    text: "Allow quoted values to contain line breaks (multi-line text fields). Slightly slower to load, so only turn on if your data needs it.",
  },
  csv_allow_jagged_rows: {
    text: "Accept rows that are missing trailing columns; the missing values become NULL. Off means such rows count as bad records.",
  },
  max_bad_records: {
    text: "How many unparsable rows the load may skip before failing. 0 (strict) fails on the first bad row, which is the safest default.",
  },
  ignore_unknown_values: {
    text: "Ignore extra columns or JSON fields that are not in the table schema instead of failing the load.",
  },
  parquet_enable_list_inference: {
    text: "Load Parquet LIST columns as simple BigQuery ARRAYs instead of nested records. Recommended on.",
  },
  avro_use_logical_types: {
    text: "Convert Avro logical types (timestamp-millis, date, decimal…) to the matching BigQuery types instead of raw integers/bytes. Recommended on.",
  },

  // ---- Destination (shared) ------------------------------------------------------------------------
  partition_type: {
    text: "How the destination table is partitioned. Daily/monthly partitioning makes each run write only its own partition (table$YYYYMMDD / table$YYYYMM), so re-running a date replaces just that date — this is what makes the pipeline idempotent.",
  },
  write_disposition: {
    text: "What happens to existing data. WRITE_TRUNCATE replaces the target partition (or table) — safe to re-run. WRITE_APPEND adds rows, so re-runs create duplicates. WRITE_EMPTY only writes if the target is empty and fails otherwise.",
  },
  create_disposition: {
    text: "CREATE_IF_NEEDED creates the table on first run (needs a schema). CREATE_NEVER requires the table to exist already, which is useful when the table is managed elsewhere (e.g. Terraform).",
  },
  schema_update_options: {
    text: "Let the job change the table schema: add new nullable columns (field addition) and/or turn REQUIRED columns into NULLABLE (field relaxation). Only valid when appending, or truncating a single partition.",
  },
  target_partition_kind: {
    text: "Data type of the final table's partition column: DATE or TIMESTAMP. Must match the column's type in the staging data.",
  },

  // ---- Checks, sensors, archive (shared) ----------------------------------------------------------
  dq_check: {
    text: "Optional data quality task after the load. 'Row count' fails the run if the loaded partition is empty. 'Custom SQL' runs your own query, which must return one row whose values are all TRUE.",
  },
  min_row_count: {
    text: "The row-count check fails if the checked rows are fewer than this.",
  },
  sensor_poke_interval_seconds: {
    text: "How often the sensor checks whether the files have arrived. Shorter intervals react faster but make more API calls.",
  },
  poke_interval_seconds: {
    text: "How often the sensor checks the condition while waiting.",
  },
  archive_action: {
    text: "What to do with processed files after a successful load. 'Move' copies them to the archive folder and deletes the originals, so they are never loaded twice. 'Copy' keeps the originals in the landing folder.",
  },

  // ---- Scenario-specific without inline help -------------------------------------------------------
  print_header: {
    text: "Write the column names as the first line of each exported CSV file.",
  },
  job_type: {
    text: "PySpark runs a Python script; a starter script is generated into scripts/<dag_id>/ for you to replace and upload to the code bucket. Spark runs a JVM job from a main class and JAR files.",
  },
  master_machine_type: {
    text: "Compute Engine machine type of the cluster's master node, which coordinates the job. A standard 4-vCPU type is enough for most jobs.",
    example: "n2-standard-4",
  },
  worker_machine_type: {
    text: "Machine type of each worker node, which does the processing. Choose more memory (highmem) for large joins and aggregations.",
    example: "n2-standard-8",
  },
  pubsub_mode: {
    text: "Publish only: send a message (e.g. to notify downstream systems). Wait only: a sensor that succeeds when a message arrives on a subscription. Wait then publish: relay, i.e. wait for a message and then publish one.",
  },
  message_format: {
    text: "JSON sends the body as a JSON document; Text sends it as plain text. Either way the body is the same on every run. Put run-specific values in the message attributes.",
  },
  compile_source: {
    text: "Compile the Dataform code from a Git branch, tag or commit (recommended for scheduled runs) or from a development workspace (for testing only).",
  },
  invocation_scope: {
    text: "Which Dataform actions to run: everything in the repository, only actions with the given tags, or only the listed tables.",
  },
};
