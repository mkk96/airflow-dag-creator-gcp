/**
 * Scenario — Cloud SQL (PostgreSQL / MySQL) → GCS → BigQuery.
 *
 *   export_<table>_to_gcs  CloudSQLExportInstanceOperator: CSV export of a SELECT
 *                          query to gs://<export_bucket>/<folder>/<table>.csv
 *   load_<table>_to_bq     BigQueryInsertJobOperator `load` job (partition decorator,
 *                          job labels), like gcs_to_bigquery
 *   check_<table>_row_count optional BigQueryCheckOperator
 *
 * Verified against apache-airflow-providers-google 12.0.0:
 *   - CloudSQLExportInstanceOperator.template_fields = project_id, instance, body,
 *     gcp_conn_id, api_version, impersonation_chain; it has no template_ext and
 *     supports deferrable=True.
 *   - CLOUD_SQL_EXPORT_VALIDATION accepts exportContext.fileType, uri, databases,
 *     csvExportOptions.selectQuery and `offload` (serverless export).
 *
 * The export query lives in its own .sql file. Because `body` is templated but has
 * no template_ext, it is pulled in with `{% include '<file>.sql' %}` (resolved via
 * template_searchpath), so the SQL file can itself use {{ ds }} and {{ cfg.* }}.
 * Its header is a Jinja comment, so the text sent to Cloud SQL is exactly the
 * user's SELECT.
 */
import { DATASET_LAYER_SETS, fillPattern, TEAM } from "../config/naming";
import { GENERATOR_NAME, RUNTIME } from "../config/runtime";
import { pyBool, pyCall, pyDict, pyInt, pyList, pyName, pyStr, toSnakeIdentifier, assign, type Py } from "../lib/python";
import * as v from "../validation/rules";
import { BQ_INSERT_JOB_TEMPLATE_EXT, bool, DagContext, guardTemplateExt, list, num, str } from "./common";
import {
  bqLoadTask,
  dependencyChain,
  destinationFields,
  destinationLoadOptions,
  isPartitioned,
  partitionDecorator,
  rowCountCheckTask,
  schemaConstants,
  taskGroupBlock,
  validateDestination,
} from "./gcsSensorLoadArchive";
import type { Params, Scenario } from "./types";

const isMysql = (p: Params) => p.database_engine === "MYSQL";
const ENGINE_LABEL: Record<string, string> = { POSTGRES: "PostgreSQL", MYSQL: "MySQL" };

/** Cloud SQL instance id: lowercase letters, digits, hyphens; starts with a letter. */
function instanceId(value: string): v.Issues {
  if (!value) return v.err("Instance id is required.");
  if (!/^[a-z]([a-z0-9-]*[a-z0-9])?$/.test(value) || value.length > 98) {
    return v.err("Instance ids use lowercase letters, digits and hyphens, start with a letter and do not end with a hyphen (no project prefix).");
  }
  return v.ok();
}

function databaseName(value: string): v.Issues {
  if (!value) return v.err("Database is required.");
  if (!/^[A-Za-z_][A-Za-z0-9_$-]{0,62}$/.test(value)) {
    return v.err("Use letters, digits, '_', '$' or '-' (max 63), starting with a letter or '_'.");
  }
  return v.ok();
}

/** Export query without a trailing semicolon. */
const exportQuery = (p: Params) =>
  String(p.export_query ?? "")
    .replace(/\r\n?/g, "\n")
    .trim()
    .replace(/;\s*$/, "");

/** Strip SQL comments so we can look at the first keyword. */
const stripSqlComments = (sql: string) => sql.replace(/--[^\n]*/g, "").replace(/\/\*[\s\S]*?\*\//g, "").trim();

const DEFAULT_QUERY = `SELECT
    invoice_id,
    customer_id,
    invoice_date,
    amount,
    currency,
    updated_at
FROM invoices
WHERE DATE(updated_at) = '{{ ds }}'
`;

const DEFAULT_SCHEMA = JSON.stringify(
  [
    { name: "invoice_id", type: "INT64", mode: "REQUIRED" },
    { name: "customer_id", type: "INT64", mode: "NULLABLE" },
    { name: "invoice_date", type: "DATE", mode: "NULLABLE" },
    { name: "amount", type: "NUMERIC", mode: "NULLABLE" },
    { name: "currency", type: "STRING", mode: "NULLABLE" },
    { name: "updated_at", type: "TIMESTAMP", mode: "NULLABLE" },
  ],
  null,
  2,
);

export const cloudsqlToBigquery: Scenario = {
  id: "cloudsql_to_bigquery",
  title: "Cloud SQL → GCS → BigQuery",
  description:
    "Export a PostgreSQL or MySQL query from Cloud SQL to a CSV file in GCS, then load it into a BigQuery table, idempotently into the run's partition.",
  naming: { source: TEAM.systemCodes.cloudsql, target: TEAM.systemCodes.bigquery },
  sections: [
    { id: "source", title: "Source (Cloud SQL)" },
    { id: "export", title: "Export file (GCS)", description: "Where Cloud SQL writes the CSV file that BigQuery loads." },
    { id: "format", title: "CSV load options" },
    { id: "schema", title: "Schema" },
    { id: "destination", title: "Destination (BigQuery)" },
    { id: "quality", title: "Data quality" },
  ],
  commonDefaults: {
    dag_id: "billing_sql_to_bq_daily",
    description: "Export changed invoices from Cloud SQL and load them into BigQuery.",
    domain: "billing",
    tags: ["billing", "cloudsql", "bigquery"],
  },
  fields: [
    // Source -----------------------------------------------------------------------
    {
      key: "database_engine", label: "Database engine", type: "select", section: "source", default: "POSTGRES",
      options: [
        { value: "POSTGRES", label: "PostgreSQL" },
        { value: "MYSQL", label: "MySQL" },
      ],
      help: "The export API is the same for both; the engine only changes the SQL dialect of your query and the CSV NULL handling.",
    },
    {
      key: "cloudsql_instance", label: "Cloud SQL instance id", type: "text", section: "source",
      default: "billing-dev-pg", required: true, envSpecific: true, validate: (x) => instanceId(String(x)),
      help: "Instance name only (not project:instance). Env-specific.",
    },
    {
      key: "cloudsql_project_id", label: "Instance project id (optional)", type: "text", section: "source",
      default: "", envSpecific: true, validate: (x) => v.bqProject(String(x)),
      help: "Project that owns the Cloud SQL instance. Empty = the Project id in GCP connection. Env-specific.",
    },
    {
      key: "database", label: "Database", type: "text", section: "source", default: "billing", required: true,
      validate: (x) => databaseName(String(x)), help: "Database the query runs in.",
    },
    {
      key: "export_query", label: "Export query", type: "code", language: "sql", accept: ".sql", section: "source",
      default: DEFAULT_QUERY, required: true, allowJinja: true,
      help: (p) =>
        `One ${ENGINE_LABEL[str(p, "database_engine")] ?? ""} SELECT, saved as its own .sql file. Use {{ ds }} (run date, YYYY-MM-DD) to export only that run's rows, ` +
        "or leave it out for a full snapshot. List the columns in the same order as the BigQuery schema; no trailing semicolon." +
        (isMysql(p) ? " MySQL writes NULL as \"N in CSV exports, which BigQuery rejects: wrap nullable columns in COALESCE(col, '')." : ""),
    },
    {
      key: "offload_export", label: "Serverless export (offload)", type: "boolean", section: "source", default: false,
      help: "Runs the export on a temporary instance so the primary database is not loaded. Costs extra and starts slower.",
    },
    // Export file ----------------------------------------------------------------
    {
      key: "export_bucket", label: "Export bucket", type: "text", section: "export",
      default: "acme-dev-billing-landing", required: true, envSpecific: true, suggestable: true,
      validate: (x) => v.gcsBucket(String(x)),
      help: "Bucket name only (no gs://). Env-specific. The Cloud SQL instance's own service account must be able to write here.",
    },
    {
      key: "export_prefix", label: "Export folder", type: "text", section: "export",
      default: "cloudsql/invoices/dt={{ ds }}/", required: true, allowJinja: true,
      validate: (x) => {
        const s = String(x);
        return v.merge(
          v.gcsObject(s),
          s.endsWith("/") ? v.ok() : v.err("End the folder path with '/'."),
          s.includes("*") ? v.err("Wildcards are not allowed here.") : v.ok(),
        );
      },
      help: "Folder inside the export bucket, ending with '/'. The file is named after the table. Include {{ ds }} so each run writes its own file.",
    },
    {
      key: "compress_export", label: "Gzip the export file", type: "boolean", section: "export", default: false,
      help: "Writes <table>.csv.gz: smaller files, but BigQuery loads compressed CSV more slowly (max 4 GB).",
    },
    // CSV load options -------------------------------------------------------------
    {
      key: "csv_null_marker", label: "Null marker (optional)", type: "text", section: "format", default: "",
      help: "Text that means NULL in the CSV. Empty = BigQuery default (empty field), which matches PostgreSQL exports.",
    },
    {
      key: "max_bad_records", label: "Max bad records", type: "number", section: "format", default: 0,
      min: 0, max: 1000000, help: "Rows that may fail to parse before the load fails. 0 = strict.",
    },
    // Schema -------------------------------------------------------------------------
    {
      key: "schema_mode", label: "Schema source", type: "select", section: "schema", default: "explicit",
      options: [
        { value: "explicit", label: "Schema file (BigQuery JSON schema)" },
        { value: "existing", label: "Use the existing table's schema" },
      ],
      help: "Cloud SQL CSV exports have no header row, so autodetect would invent column names; give the schema or load into an existing table.",
    },
    {
      key: "schema_json", label: "BigQuery JSON schema", type: "code", language: "json", accept: ".json",
      section: "schema", default: DEFAULT_SCHEMA, required: true, showIf: (p) => p.schema_mode === "explicit",
      validate: (x) => v.bqSchemaJson(String(x)),
      help: "Columns in the same order as the export query. Embedded in the DAG as SCHEMA_FIELDS.",
    },
    // Destination / quality --------------------------------------------------------------
    ...destinationFields({ dataset: "raw_billing", table: "invoices" }),
    {
      key: "dq_check", label: "Post-load check", type: "select", section: "quality", default: "row_count",
      options: [
        { value: "row_count", label: "Row count > 0 for the loaded partition" },
        { value: "none", label: "None" },
      ],
      help: "Choose None if a run may legitimately export no rows (e.g. no changes that day).",
    },
  ],

  samples: {
    postgres_incremental_daily: {
      dag_id: "billing_sql_to_bq_daily",
      email: ["billing-alerts@example.com"],
      cluster_fields: ["customer_id"],
    },
    mysql_snapshot_weekly_variables: {
      dag_id: "crm_sql_to_bq_weekly",
      description: "Weekly full snapshot of CRM accounts from Cloud SQL for MySQL.",
      domain: "crm",
      config_mode: "variables",
      database_engine: "MYSQL",
      cloudsql_instance: "crm-dev-mysql",
      cloudsql_project_id: "acme-dev-crm",
      database: "crm",
      export_query: [
        "SELECT",
        "    account_id,",
        "    account_name,",
        "    created_at,",
        "    COALESCE(segment, '') AS segment",
        "FROM accounts",
        "",
      ].join("\n"),
      offload_export: true,
      export_bucket: "acme-dev-crm-landing",
      export_prefix: "cloudsql/accounts/dt={{ ds }}/",
      compress_export: true,
      schema_mode: "existing",
      create_disposition: "CREATE_NEVER",
      dataset: "raw_crm",
      table: "accounts",
      partition_type: "NONE",
      schedule_preset: "@weekly",
      impersonation_chain: ["etl-runner@acme-dev-data.iam.gserviceaccount.com"],
      tags: ["crm", "cloudsql", "bigquery"],
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => {
      out[k] = out[k] ? v.merge(out[k], i) : i;
    };
    const query = exportQuery(p);
    if (query.includes(";")) add("export_query", v.err("Only a single SELECT statement is allowed."));
    if (query && !/^(SELECT|WITH)\b/i.test(stripSqlComments(query))) {
      add("export_query", v.warn("Cloud SQL CSV exports expect a SELECT (or WITH ... SELECT) query."));
    }
    if (isPartitioned(p) && query && !query.includes("{{")) {
      add("export_query", v.warn("No date macro: every run exports the whole result into its own partition. For a full snapshot choose 'Not partitioned'."));
    }
    if (!str(p, "export_prefix").includes("{{")) {
      add("export_prefix", v.warn("No date macro: every run overwrites the same export file."));
    }
    if (p.schema_mode === "existing" && p.create_disposition === "CREATE_IF_NEEDED") {
      add("schema_mode", v.warn("The table must already exist when using its existing schema (consider CREATE_NEVER)."));
    }
    validateDestination(p, add);
    return out;
  },

  suggest(ctx) {
    const layer = DATASET_LAYER_SETS[ctx.layerSet as keyof typeof DATASET_LAYER_SETS]?.[0] ?? "raw";
    const out: Partial<Params> = {};
    const bucket = fillPattern("gcsBucket", { ...ctx, purpose: TEAM.bucketPurposes.landing });
    const dataset = fillPattern("bqDataset", { layer, domain: ctx.domain });
    if (bucket) out.export_bucket = bucket;
    if (dataset) out.dataset = dataset;
    return out;
  },

  generate(p) {
    const ctx = new DagContext(cloudsqlToBigquery, p);
    ctx.importFrom("airflow.providers.google.cloud.operators.cloud_sql", "CloudSQLExportInstanceOperator");

    const engine = str(p, "database_engine");
    const table = str(p, "table");
    const obj = toSnakeIdentifier(table, "table");
    const partition = str(p, "partition_type");
    const exportId = `export_${obj}_to_gcs`;
    const loadId = `load_${obj}_to_bq`;
    const fileName = `${obj}.csv${bool(p, "compress_export") ? ".gz" : ""}`;
    const uri = `gs://${ctx.ref("export_bucket")}/${str(p, "export_prefix").replace(/^\/+/, "")}${fileName}`;

    // --- export query (.sql file, pulled into the templated body with include) ---------
    const sqlFile = ctx.addSql(
      exportId,
      [
        `{# ${ENGINE_LABEL[engine] ?? engine} export query for Cloud SQL (csvExportOptions.selectQuery). -#}`,
        `{# Generated by ${GENERATOR_NAME} v${RUNTIME.generatorVersion} for DAG ${ctx.dagId}. -#}`,
        ...ctx.sqlPrelude(),
        exportQuery(p),
      ].join("\n"),
    );

    const exportContext: Array<[string, Py]> = [
      ["fileType", pyStr("CSV")],
      ["uri", pyStr(uri)],
      ["databases", pyList([pyStr(str(p, "database"))])],
      ["csvExportOptions", pyDict([["selectQuery", pyStr(`{% include '${sqlFile}' %}`)]])],
    ];
    if (bool(p, "offload_export")) exportContext.push(["offload", pyBool(true)]);

    const instanceProject = str(p, "cloudsql_project_id") ? ctx.ref("cloudsql_project_id") : ctx.ref("project_id");
    const exportTask = pyCall(
      "CloudSQLExportInstanceOperator",
      [],
      [
        ["task_id", pyStr(exportId)],
        ["project_id", pyStr(instanceProject)],
        ["instance", pyStr(ctx.ref("cloudsql_instance"))],
        ["body", pyDict([["exportContext", pyDict(exportContext, { explode: true })]], { explode: true })],
        ["gcp_conn_id", pyName("GCP_CONN_ID")],
        ["impersonation_chain", ctx.impersonationChain()],
        ["deferrable", ctx.deferrable()],
      ],
      { explode: true },
    );

    // --- load ----------------------------------------------------------------------------
    const load: Array<[string, Py]> = [
      ["sourceUris", pyList([pyStr(guardTemplateExt(uri, BQ_INSERT_JOB_TEMPLATE_EXT))])],
      ...destinationLoadOptions(ctx, p),
      ["sourceFormat", pyStr("CSV")],
    ];
    if (str(p, "schema_mode") === "explicit") load.push(["schema", pyDict([["fields", pyName("SCHEMA_FIELDS")]])]);
    load.push(
      // Cloud SQL CSV exports have no header row and quote values that contain newlines.
      ["skipLeadingRows", pyInt(0)],
      ["allowQuotedNewlines", pyBool(true)],
    );
    if (String(p.csv_null_marker ?? "") !== "") load.push(["nullMarker", pyStr(String(p.csv_null_marker))]);
    load.push(["maxBadRecords", pyInt(num(p, "max_bad_records"))]);
    const loadTask = bqLoadTask(ctx, loadId, load);

    const checkId = str(p, "dq_check") === "row_count" ? `check_${obj}_row_count` : null;
    const body = [assign(exportId, exportTask, 1), ""];
    if (checkId) {
      body.push(
        ...taskGroupBlock(ctx, "load", "Load the export into BigQuery and check the result", [
          [loadId, loadTask],
          [checkId, rowCountCheckTask(ctx, p, checkId)],
        ]),
        "",
        dependencyChain([exportId, "load"], 1),
      );
    } else {
      body.push(assign(loadId, loadTask, 1), "", dependencyChain([exportId, loadId], 1));
    }

    const constants = str(p, "schema_mode") === "explicit" ? schemaConstants(String(p.schema_json ?? "[]")) : [];

    // --- docs ------------------------------------------------------------------------------
    const target = `\`<project_id>.<dataset>.${table}\``;
    const docSteps = [
      `\`${exportId}\`: run \`${sqlFile}\` on database \`${str(p, "database")}\` of the ${ENGINE_LABEL[engine] ?? engine} instance ` +
        `\`cloudsql_instance\` and write the result as CSV to \`${str(p, "export_prefix")}${fileName}\` in the \`export_bucket\`` +
        (bool(p, "offload_export") ? " (serverless export)." : "."),
      `\`${loadId}\`: load that file into ${target}` +
        (partition === "NONE" ? "" : ` partition \`${partitionDecorator(partition)}\``) +
        ` with ${str(p, "write_disposition")}.`,
      ...(checkId ? [`\`${checkId}\`: fail if the loaded ${partition === "NONE" ? "table" : "partition"} has no rows.`] : []),
    ];

    const iamRoles = [
      "`roles/cloudsql.editor` (or a custom role with `cloudsql.instances.get` and `cloudsql.instances.export`) on the Cloud SQL instance's project",
      "`roles/bigquery.jobUser` on the project that runs the jobs",
      "`roles/bigquery.dataEditor` on the destination dataset",
      "`roles/storage.objectViewer` on the export bucket",
      "The Cloud SQL instance's own service account (`gcloud sql instances describe <instance> --format='value(serviceAccountEmailAddress)'`) needs `roles/storage.objectAdmin` on the export bucket: Cloud SQL, not Composer, writes the file",
    ];
    if (list(p, "impersonation_chain").length) {
      iamRoles.push("`roles/iam.serviceAccountTokenCreator` for the Composer service account on the impersonated service account");
    }

    return ctx.assemble({
      docSteps,
      constants,
      body,
      iamRoles,
      readmeNotes: [
        `The export query is \`dags/sql/${ctx.dagId}/${sqlFile}\`; Airflow renders it (macros such as {{ ds }} work) and sends it as the export's selectQuery.`,
        "Re-running a date overwrites the same export file" +
          (str(p, "write_disposition") === "WRITE_TRUNCATE"
            ? `, and the WRITE_TRUNCATE load replaces ${partition === "NONE" ? "the whole table" : "that run's partition"}, so re-runs are idempotent.`
            : "; the load is not idempotent with the chosen write disposition."),
        "Cloud SQL runs one export or import per instance at a time; keep max_active_runs at 1 and avoid overlapping exports from other jobs.",
        ...(bool(p, "offload_export")
          ? ["Serverless export (offload) is on: the export runs on a temporary instance, so it does not load the primary but takes longer to start and is billed separately."]
          : ["Without serverless export the query runs on the instance itself; schedule large exports outside peak hours or enable offload."]),
        ...(engine === "MYSQL"
          ? ['Cloud SQL for MySQL writes NULL values as "N in CSV exports, which BigQuery cannot parse: wrap nullable columns in COALESCE() in the export query.']
          : []),
        "Export files are kept in the export bucket; add a lifecycle rule to delete them after a few days.",
        "Labels (env, team, owner, cost_center, dag_id) are set on every BigQuery job for cost tracking.",
        ...(bool(p, "deferrable")
          ? ["The export and BigQuery jobs run in deferrable mode: make sure the Composer environment has a triggerer (Composer 2: set triggerer count >= 1)."]
          : []),
      ],
    });
  },
};
