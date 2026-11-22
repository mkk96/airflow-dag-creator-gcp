/**
 * BigQuery → GCS export.
 *
 * Table mode: BigQueryInsertJobOperator with an `extract` job configuration
 * (providers-google 12.x). `configuration` is templated (env values resolve at
 * run time), `configuration.labels` labels the job, and deferrable=True is
 * supported. It is preferred over BigQueryToGCSOperator so both modes use the
 * same operator and the job body mirrors the BigQuery API one-to-one.
 *
 * Query mode: an `EXPORT DATA OPTIONS (...) AS SELECT ...` statement in its own
 * .sql file, run by BigQueryInsertJobOperator with a `query` configuration.
 *
 * Idempotency: the destination path is date-partitioned (e.g. dt={{ ds }}) and
 * an optional first task deletes the files a previous run of the same date
 * wrote, so a re-run never leaves stale shards behind.
 */
import { DATASET_LAYER_SETS, fillPattern, TEAM } from "../config/naming";
import {
  assign,
  pyBool,
  pyCall,
  pyDict,
  pyList,
  pyName,
  pyStr,
  toSnakeIdentifier,
  withHeader,
  type Py,
} from "../lib/python";
import * as v from "../validation/rules";
import {
  cleanUserSql,
  dependencyChain,
  firstSqlKeyword,
  sqlRefIssues,
  sqlStatementCount,
} from "./bigquerySqlTransform";
import { BQ_INSERT_JOB_TEMPLATE_EXT, bool, DagContext, guardTemplateExt, list, str } from "./common";
import type { Params, Scenario } from "./types";

const isTable = (p: Params) => p.source_mode === "table";
const isQuery = (p: Params) => p.source_mode === "query";
const isCsv = (p: Params) => p.destination_format === "CSV";

/** Allowed compression codecs per export format (extract jobs and EXPORT DATA). */
const COMPRESSION_BY_FORMAT: Record<string, string[]> = {
  CSV: ["NONE", "GZIP"],
  NEWLINE_DELIMITED_JSON: ["NONE", "GZIP"],
  AVRO: ["NONE", "DEFLATE", "SNAPPY"],
  PARQUET: ["NONE", "SNAPPY", "GZIP", "ZSTD"],
};
/** EXPORT DATA `format` option names. */
const EXPORT_DATA_FORMAT: Record<string, string> = {
  CSV: "CSV",
  NEWLINE_DELIMITED_JSON: "JSON",
  AVRO: "AVRO",
  PARQUET: "PARQUET",
};
/** Expected file extensions per format (compressed CSV/JSON add .gz). */
const EXTENSIONS: Record<string, string[]> = {
  CSV: [".csv", ".tsv", ".txt"],
  NEWLINE_DELIMITED_JSON: [".json", ".jsonl", ".ndjson"],
  AVRO: [".avro"],
  PARQUET: [".parquet"],
};
/** Field delimiter options → SQL string literal (fixed set, never user text). */
const DELIMITER_SQL: Record<string, string> = { ",": "','", "\t": "'\\t'", "|": "'|'", ";": "';'" };

/** Object path with Jinja expressions removed (for wildcard / quote checks). */
const withoutJinja = (s: string) => s.replace(/\{\{[\s\S]*?\}\}/g, "x").replace(/\{%[\s\S]*?%\}/g, "");

/** Prefix of the object path before the `*` wildcard: what the cleanup task deletes. */
const shardPrefix = (objectPath: string) => objectPath.replace(/^\/+/, "").split("*")[0];

/** Purpose token for the export bucket name; falls back if naming.ts has no `export` purpose. */
const EXPORT_PURPOSE = (TEAM.bucketPurposes as Record<string, string>).export ?? "export";

const DEFAULT_EXPORT_SQL = `-- Orders of the run date, exported as one file set per day.
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.amount
FROM \`{{ cfg.project_id }}.{{ cfg.dataset }}.orders\` AS o
WHERE o.order_date = DATE('{{ ds }}')
`;

const WEEKLY_EXPORT_SQL = `-- Weekly revenue per customer for the 7 days ending on the run date.
SELECT
    o.customer_id,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.amount) AS revenue
FROM \`{{ cfg.project_id }}.{{ cfg.dataset }}.orders\` AS o
WHERE o.order_date BETWEEN DATE_SUB(DATE('{{ ds }}'), INTERVAL 6 DAY) AND DATE('{{ ds }}')
GROUP BY o.customer_id
`;

export const bigqueryToGcs: Scenario = {
  id: "bigquery_to_gcs",
  title: "BigQuery → GCS export",
  description:
    "Export a BigQuery table, its run-date partition, or a query result to GCS as CSV, JSON, Avro or Parquet, into a date-partitioned path that re-runs overwrite.",
  naming: { source: TEAM.systemCodes.bigquery, target: TEAM.systemCodes.gcs },
  sections: [
    { id: "source", title: "Source (BigQuery)" },
    { id: "destination", title: "Destination (GCS)" },
    { id: "format", title: "File format options" },
  ],
  commonDefaults: {
    dag_id: "analytics_bq_to_gcs_daily",
    description: "Export the daily orders partition from BigQuery to GCS.",
    domain: "analytics",
    tags: ["analytics", "bigquery", "gcs", "export"],
  },
  fields: [
    // Source ------------------------------------------------------------------------------
    {
      key: "source_mode", label: "Export from", type: "select", section: "source", default: "table",
      options: [
        { value: "table", label: "A table or its run-date partition (extract job)" },
        { value: "query", label: "A query result (EXPORT DATA statement)" },
      ],
      help: "Extract jobs copy a table as-is at no query cost. Use a query to filter, join or reshape the data before exporting.",
    },
    {
      key: "dataset", label: "Dataset", type: "text", section: "source", default: "curated_analytics",
      required: true, envSpecific: true, suggestable: true, validate: (x) => v.bqDataset(String(x)),
      help: (p) =>
        isQuery(p)
          ? "Env-specific. Available in the query as {{ cfg.dataset }}."
          : "Env-specific. Dataset of the table to export (in the job's project).",
    },
    {
      key: "table", label: "Table", type: "text", section: "source", default: "orders", required: true,
      showIf: isTable, validate: (x) => v.bqTable(String(x)), help: "Table to export, without a $ decorator.",
    },
    {
      key: "partition_type", label: "What to export", type: "select", section: "source", default: "DAY",
      showIf: isTable,
      options: [
        { value: "DAY", label: "The run's daily partition (table$YYYYMMDD)" },
        { value: "MONTH", label: "The run's monthly partition (table$YYYYMM)" },
        { value: "NONE", label: "The whole table (snapshot)" },
      ],
      help: "Partition exports need a time-partitioned table.",
    },
    {
      key: "export_sql", label: "Query (SELECT)", type: "code", language: "sql", accept: ".sql", section: "source",
      default: DEFAULT_EXPORT_SQL, required: true, allowJinja: true, showIf: isQuery,
      help: "A single SELECT; it is wrapped in EXPORT DATA (with the destination and format options below) and saved as its own .sql file. Use {{ cfg.project_id }}, {{ cfg.dataset }} and {{ ds }} for the run date.",
    },
    {
      key: "export_name", label: "Export name", type: "text", section: "source", default: "orders",
      required: true, showIf: isQuery,
      validate: (x) => (/^[a-z][a-z0-9_]*$/.test(String(x)) ? v.ok() : v.err("Use snake_case, e.g. weekly_revenue.")),
      help: "Short snake_case name of what is exported; used in the task ids (export_<name>_to_gcs).",
    },
    // Destination ------------------------------------------------------------------------
    {
      key: "export_bucket", label: "Destination bucket", type: "text", section: "destination",
      default: "acme-dev-analytics-export", required: true, envSpecific: true, suggestable: true,
      validate: (x) => v.gcsBucket(String(x)), help: "Bucket name only (no gs://). Env-specific.",
    },
    {
      key: "export_object", label: "Destination path", type: "text", section: "destination",
      default: "exports/orders/dt={{ ds }}/part-*.csv.gz", required: true, allowJinja: true,
      validate: (x) => v.gcsObject(String(x)),
      help: "Path inside the bucket with exactly one * (BigQuery numbers the files there: part-000000000000…). Include the run date, e.g. dt={{ ds }}, so each run writes its own folder and a re-run overwrites it.",
    },
    {
      key: "clear_previous", label: "Delete this run's previous files first", type: "boolean", section: "destination",
      default: true,
      help: "Before exporting, delete the objects that start with the path before the * (e.g. exports/orders/dt=2024-01-15/part-). Re-runs then never leave stale files behind.",
    },
    // Format -------------------------------------------------------------------------------
    {
      key: "destination_format", label: "File format", type: "select", section: "format", default: "CSV",
      options: [
        { value: "CSV", label: "CSV" },
        { value: "NEWLINE_DELIMITED_JSON", label: "JSON (newline-delimited)" },
        { value: "AVRO", label: "Avro" },
        { value: "PARQUET", label: "Parquet" },
      ],
      help: "CSV can't hold nested or repeated (RECORD/ARRAY) columns; use JSON, Avro or Parquet for those.",
    },
    {
      key: "compression", label: "Compression", type: "select", section: "format", default: "GZIP",
      options: [
        { value: "NONE", label: "None" },
        { value: "GZIP", label: "GZIP (CSV, JSON, Parquet)" },
        { value: "DEFLATE", label: "DEFLATE (Avro)" },
        { value: "SNAPPY", label: "SNAPPY (Avro, Parquet)" },
        { value: "ZSTD", label: "ZSTD (Parquet)" },
      ],
      help: "Compressed CSV/JSON files should end in .gz (e.g. part-*.csv.gz).",
    },
    {
      key: "print_header", label: "Write a header row", type: "boolean", section: "format", default: true,
      showIf: isCsv,
    },
    {
      key: "field_delimiter", label: "Field delimiter", type: "select", section: "format", default: ",",
      showIf: isCsv,
      options: [
        { value: ",", label: "Comma ( , )" },
        { value: "\t", label: "Tab" },
        { value: "|", label: "Pipe ( | )" },
        { value: ";", label: "Semicolon ( ; )" },
      ],
    },
    {
      key: "use_avro_logical_types", label: "Use Avro logical types", type: "boolean", section: "format",
      default: true, showIf: (p) => p.destination_format === "AVRO",
      help: "Write DATE, TIME, TIMESTAMP and DATETIME as Avro logical types instead of strings/integers.",
    },
  ],

  samples: {
    daily_partition_csv: {
      dag_id: "analytics_bq_to_gcs_daily",
      email: ["analytics-data@example.com"],
    },
    weekly_query_parquet_variables: {
      dag_id: "reporting_bq_to_gcs_weekly",
      description: "Weekly customer revenue extract for the reporting partner feed.",
      domain: "reporting",
      config_mode: "variables",
      env: "stg",
      project_id: "acme-stg-data",
      source_mode: "query",
      dataset: "curated_reporting",
      export_sql: WEEKLY_EXPORT_SQL,
      export_name: "weekly_customer_revenue",
      export_bucket: "acme-stg-reporting-export",
      export_object: "partner/weekly_revenue/week_ending={{ ds }}/revenue-*.parquet",
      destination_format: "PARQUET",
      compression: "SNAPPY",
      schedule_preset: "@weekly",
      impersonation_chain: ["export-runner@acme-stg-data.iam.gserviceaccount.com"],
      tags: ["reporting", "bigquery", "gcs", "export"],
    },
    monthly_json_snapshot: {
      dag_id: "revenue_bq_to_gcs_monthly",
      description: "Monthly snapshot of the revenue table as JSON.",
      domain: "revenue",
      dataset: "curated_revenue",
      table: "monthly_product_revenue",
      partition_type: "MONTH",
      export_bucket: "acme-dev-revenue-export",
      export_object: "snapshots/monthly_product_revenue/month={{ logical_date.strftime('%Y-%m') }}/part-*.json",
      destination_format: "NEWLINE_DELIMITED_JSON",
      compression: "NONE",
      schedule_preset: "@monthly",
      deferrable: false,
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => {
      out[k] = out[k] ? v.merge(out[k], i) : i;
    };
    const objectPath = str(p, "export_object");
    const plain = withoutJinja(objectPath);
    const stars = (plain.match(/\*/g) ?? []).length;
    if (stars !== 1) {
      add("export_object", v.err("Use exactly one * wildcard, e.g. part-*.csv. BigQuery replaces it with a file number and needs it for exports over 1 GB."));
    }
    if (!objectPath.includes("{{")) {
      add("export_object", v.warn("No run-date macro (e.g. dt={{ ds }}) in the path: every run overwrites the same files."));
    }
    if (isQuery(p) && /['\\]/.test(plain)) {
      add("export_object", v.err("Quotes and backslashes are not allowed in the path of a query export."));
    }
    if (bool(p, "clear_previous") && stars === 1 && withoutJinja(shardPrefix(objectPath)).trim() === "") {
      add("clear_previous", v.err("The path must have a fixed prefix before the * (e.g. exports/orders/dt={{ ds }}/part-*), or the cleanup would delete the whole bucket."));
    }
    if (bool(p, "clear_previous") && stars === 1 && !shardPrefix(objectPath).includes("{{")) {
      add("clear_previous", v.warn("The prefix before the * has no run-date macro: every run deletes the previous export before writing a new one."));
    }

    const format = str(p, "destination_format");
    const compression = str(p, "compression");
    if (!(COMPRESSION_BY_FORMAT[format] ?? []).includes(compression)) {
      add("compression", v.err(`${compression} is not supported for ${format}. Choose ${(COMPRESSION_BY_FORMAT[format] ?? []).join(", ")}.`));
    }
    const lower = plain.toLowerCase();
    const gz = compression === "GZIP" && (format === "CSV" || format === "NEWLINE_DELIMITED_JSON");
    const exts = (EXTENSIONS[format] ?? []).map((e) => (gz ? `${e}.gz` : e));
    if (stars === 1 && !exts.some((e) => lower.endsWith(e))) {
      add("export_object", v.warn(`File names usually end in ${exts.join(" or ")} for this format and compression.`));
    }

    if (isQuery(p)) {
      const sql = String(p.export_sql ?? "");
      const keyword = firstSqlKeyword(sql);
      if (keyword !== "SELECT" && keyword !== "WITH") {
        add("export_sql", v.err("Enter a SELECT (or WITH … SELECT); the EXPORT DATA statement is generated around it."));
      } else if (sqlStatementCount(sql) > 1) {
        add("export_sql", v.err("Only a single SELECT statement is allowed."));
      }
      add("export_sql", sqlRefIssues(sql, bigqueryToGcs, p, []));
    }
    return out;
  },

  suggest(ctx) {
    const layers = DATASET_LAYER_SETS[ctx.layerSet as keyof typeof DATASET_LAYER_SETS] ?? DATASET_LAYER_SETS.classic;
    const out: Partial<Params> = {};
    const dataset = fillPattern("bqDataset", { layer: layers[2], domain: ctx.domain });
    const bucket = fillPattern("gcsBucket", { ...ctx, purpose: EXPORT_PURPOSE });
    if (dataset) out.dataset = dataset;
    if (bucket) out.export_bucket = bucket;
    return out;
  },

  generate(p) {
    const ctx = new DagContext(bigqueryToGcs, p);
    ctx.importFrom("airflow.providers.google.cloud.operators.bigquery", "BigQueryInsertJobOperator");

    const table = isTable(p);
    const format = str(p, "destination_format");
    const compression = str(p, "compression");
    const objectPath = str(p, "export_object").replace(/^\/+/, "");
    const name = table ? str(p, "table") : str(p, "export_name");
    const obj = toSnakeIdentifier(name, "export");
    const partition = table ? str(p, "partition_type") : "NONE";
    const decorator =
      partition === "DAY" ? "${{ ds_nodash }}" : partition === "MONTH" ? "${{ logical_date.strftime('%Y%m') }}" : "";
    const exportTaskId = `export_${obj}_to_gcs`;
    const deleteTaskId = `delete_${obj}_previous_files`;
    const clear = bool(p, "clear_previous");
    const avro = format === "AVRO";
    const csv = format === "CSV";

    // --- export job configuration -------------------------------------------------------
    let jobConfig: Array<[string, Py]>;
    let sqlName: string | null = null;
    if (table) {
      const extract: Array<[string, Py]> = [
        [
          "sourceTable",
          pyDict(
            [
              ["projectId", pyStr(ctx.ref("project_id"))],
              ["datasetId", pyStr(ctx.ref("dataset"))],
              ["tableId", pyStr(name + decorator)],
            ],
            { explode: true },
          ),
        ],
        [
          "destinationUris",
          pyList([pyStr(guardTemplateExt(`gs://${ctx.ref("export_bucket")}/${objectPath}`, BQ_INSERT_JOB_TEMPLATE_EXT))]),
        ],
        ["destinationFormat", pyStr(format)],
      ];
      if (compression !== "NONE") extract.push(["compression", pyStr(compression)]);
      if (csv) {
        extract.push(
          ["printHeader", pyBool(bool(p, "print_header"))],
          ["fieldDelimiter", pyStr(String(p.field_delimiter ?? ","))],
        );
      }
      if (avro) extract.push(["useAvroLogicalTypes", pyBool(bool(p, "use_avro_logical_types"))]);
      jobConfig = [["extract", pyDict(extract, { explode: true })]];
    } else {
      // EXPORT DATA options come from fixed form choices; only the object path
      // (validated: no quotes/backslashes outside Jinja) is user text.
      const options = [
        `uri = 'gs://${ctx.sqlRef("export_bucket")}/${objectPath}'`,
        `format = '${EXPORT_DATA_FORMAT[format] ?? "CSV"}'`,
        "overwrite = TRUE",
      ];
      if (csv) {
        options.push(`header = ${bool(p, "print_header") ? "TRUE" : "FALSE"}`);
        options.push(`field_delimiter = ${DELIMITER_SQL[String(p.field_delimiter ?? ",")] ?? "','"}`);
      }
      if (avro) options.push(`use_avro_logical_types = ${bool(p, "use_avro_logical_types") ? "TRUE" : "FALSE"}`);
      if (compression !== "NONE") options.push(`compression = '${compression}'`);
      const sql = [
        ...ctx.sqlHeader(`Export: query result to ${format} files in GCS (EXPORT DATA, overwrite = TRUE).`),
        "EXPORT DATA",
        "OPTIONS (",
        options.map((o) => `    ${o}`).join(",\n"),
        ")",
        "AS",
        cleanUserSql(String(p.export_sql ?? "")),
      ].join("\n");
      sqlName = ctx.addSql(exportTaskId, sql);
      jobConfig = [
        [
          "query",
          pyDict(
            [
              ["query", pyStr(sqlName)],
              ["useLegacySql", pyBool(false)],
            ],
            { explode: true },
          ),
        ],
      ];
    }
    jobConfig.push(["labels", pyName("LABELS")]);

    const exportTask = pyCall(
      "BigQueryInsertJobOperator",
      [],
      [
        ["task_id", pyStr(exportTaskId)],
        ["project_id", pyStr(ctx.ref("project_id"))],
        ["location", pyName("LOCATION")],
        ["configuration", pyDict(jobConfig, { explode: true })],
        ["gcp_conn_id", pyName("GCP_CONN_ID")],
        ["impersonation_chain", ctx.impersonationChain()],
        ["deferrable", ctx.deferrable()],
      ],
      { explode: true },
    );

    // --- optional cleanup of a previous run's files -------------------------------------------
    // GCSDeleteObjectsOperator: bucket_name and prefix are templated; no template_ext.
    const prefix = shardPrefix(objectPath);
    let deleteTask: Py | null = null;
    if (clear) {
      ctx.importFrom("airflow.providers.google.cloud.operators.gcs", "GCSDeleteObjectsOperator");
      deleteTask = pyCall(
        "GCSDeleteObjectsOperator",
        [],
        [
          ["task_id", pyStr(deleteTaskId)],
          ["bucket_name", pyStr(ctx.ref("export_bucket"))],
          ["prefix", pyStr(prefix)],
          ["gcp_conn_id", pyName("GCP_CONN_ID")],
          ["impersonation_chain", ctx.impersonationChain()],
        ],
        { explode: true },
      );
    }

    // --- DAG body ----------------------------------------------------------------------------
    const body: string[] = [];
    if (deleteTask) {
      ctx.importFrom("airflow.utils.task_group", "TaskGroup");
      body.push(
        withHeader(
          pyCall("TaskGroup", [], [
            ["group_id", pyStr("export")],
            ["tooltip", pyStr("Delete the previous files of this run, then export from BigQuery to GCS")],
          ]),
          null,
          1,
        ),
        assign(deleteTaskId, deleteTask, 2),
        "",
        assign(exportTaskId, exportTask, 2),
        "",
        dependencyChain([deleteTaskId, exportTaskId]),
      );
    } else {
      body.push(assign(exportTaskId, exportTask, 1));
    }

    // --- docs --------------------------------------------------------------------------------
    const fmtLabel: Record<string, string> = {
      CSV: "CSV",
      NEWLINE_DELIMITED_JSON: "newline-delimited JSON",
      AVRO: "Avro",
      PARQUET: "Parquet",
    };
    const dest = `\`gs://<export_bucket>/${objectPath}\``;
    const docSteps: string[] = [];
    if (clear) docSteps.push(`\`${deleteTaskId}\`: delete objects under \`gs://<export_bucket>/${prefix}\` left by a previous run.`);
    docSteps.push(
      table
        ? `\`${exportTaskId}\`: extract \`<project_id>.<dataset>.${name}\`` +
            (partition === "NONE" ? "" : ` partition \`${decorator}\``) +
            ` to ${fmtLabel[format] ?? format} files ${dest}` +
            (compression === "NONE" ? "." : ` (${compression}).`)
        : `\`${exportTaskId}\`: run \`sql/${ctx.dagId}/${sqlName}\` (EXPORT DATA) and write ${fmtLabel[format] ?? format} files to ${dest}.`,
    );

    const iamRoles = [
      "`roles/bigquery.jobUser` on the project that runs the jobs",
      `\`roles/bigquery.dataViewer\` on the ${table ? "source dataset" : "datasets the query reads"}`,
      "`roles/storage.objectAdmin` on the destination bucket (create, overwrite and delete export files)",
    ];
    if (list(p, "impersonation_chain").length) {
      iamRoles.push("`roles/iam.serviceAccountTokenCreator` for the Composer service account on the impersonated service account");
    }

    return ctx.assemble({
      docSteps,
      body,
      iamRoles,
      readmeNotes: [
        "Each run writes into its own date-partitioned path; BigQuery shards the output into numbered files at the * wildcard.",
        clear
          ? "A re-run first deletes the files a previous run of the same date wrote (objects starting with the path before the *), so no stale shards are left behind."
          : "Re-runs overwrite files with the same names, but if a re-run produces fewer files, extra shards from the earlier run remain. Enable the cleanup option to avoid this.",
        ...(table
          ? []
          : [`The EXPORT DATA statement lives in \`dags/sql/${ctx.dagId}/${sqlName}\`; edit the SELECT there and re-upload.`]),
        "The dataset and the bucket must be in compatible locations (a multi-region dataset can export to a bucket in that multi-region or any region inside it).",
        "Labels (env, team, owner, cost_center, dag_id) are set on every BigQuery job for cost tracking.",
        ...(bool(p, "deferrable")
          ? ["Operators run in deferrable mode: make sure the Composer environment has a triggerer (Composer 2: set triggerer count >= 1)."]
          : []),
      ],
    });
  },
};
