import type { PlannedScenario } from "./types";

/** Roadmap. Remove an entry when its scenario file is registered in registry.ts. */
export const PLANNED_SCENARIOS: PlannedScenario[] = [
  { id: "bigquery_sql_transform", title: "BigQuery SQL transformation", description: "BigQueryInsertJobOperator running SQL from a .sql file." },
  { id: "bigquery_to_gcs", title: "BigQuery → GCS export", description: "Export a table or query result to GCS." },
  { id: "bigquery_table_copy", title: "BigQuery → BigQuery copy", description: "Copy tables between datasets or projects." },
  { id: "gcs_sensor_load_archive", title: "GCS sensor → load → archive", description: "Wait for files, load into BigQuery, move processed files to an archive bucket." },
  { id: "dataflow_template", title: "Dataflow template job", description: "Launch a Classic or Flex template." },
  { id: "dataproc_ephemeral", title: "Dataproc cluster job", description: "Create cluster → submit PySpark/Spark → delete cluster (all_done)." },
  { id: "dataproc_serverless", title: "Dataproc Serverless batch", description: "Submit a serverless Spark batch." },
  { id: "cloudsql_to_bigquery", title: "Cloud SQL → GCS → BigQuery", description: "Export Postgres/MySQL to GCS, then load into BigQuery." },
  { id: "cloud_run_function", title: "Cloud Run job / Cloud Function", description: "Trigger a Cloud Run job or invoke a Cloud Function." },
  { id: "pubsub", title: "Pub/Sub publish / pull sensor", description: "Publish a message or wait for messages." },
  { id: "bigquery_dq_checks", title: "BigQuery data quality checks", description: "BigQueryCheckOperator and BigQueryValueCheckOperator suites." },
  { id: "dataform_workflow", title: "Dataform workflow", description: "Compile and invoke a Dataform workflow." },
  { id: "full_elt", title: "Full ELT pipeline", description: "Sensor → load staging → MERGE → archive → notify on failure." },
];
