/**
 * Scenario 1 — GCS → BigQuery load.
 *
 * Uses BigQueryInsertJobOperator with a `load` job configuration (not
 * GCSToBigQueryOperator) because, in apache-airflow-providers-google 12.x:
 *   - `configuration` and `project_id` are templated, so env-specific values
 *     can be resolved by Jinja at run time (Variables or config dict);
 *   - `configuration.labels` sets labels on the BigQuery *job* (GCSToBigQuery's
 *     `labels` only labels the destination table);
 *   - it supports deferrable=True.
 */
import { DATASET_LAYER_SETS, fillPattern, TEAM } from "../config/naming";
import {
  assign,
  comment,
  pyBool,
  pyCall,
  pyDict,
  pyFromJson,
  pyIdent,
  pyInt,
  pyList,
  pyName,
  pyStr,
  toSnakeIdentifier,
  withHeader,
  type Py,
} from "../lib/python";
import * as v from "../validation/rules";
import { BQ_INSERT_JOB_TEMPLATE_EXT, bool, DagContext, guardTemplateExt, list, num, str } from "./common";
import type { Params, Scenario } from "./types";

const isCsv = (p: Params) => p.source_format === "CSV";
const isCsvOrJson = (p: Params) => p.source_format === "CSV" || p.source_format === "NEWLINE_DELIMITED_JSON";
const isPartitioned = (p: Params) => p.partition_type !== "NONE";

/** Minimal set of BigQuery reserved words that must be quoted as column names. */
const BQ_RESERVED = new Set(
  (
    "ALL AND ANY ARRAY AS ASC ASSERT_ROWS_MODIFIED AT BETWEEN BY CASE CAST COLLATE CONTAINS CREATE " +
    "CROSS CUBE CURRENT DEFAULT DEFINE DESC DISTINCT ELSE END ENUM ESCAPE EXCEPT EXCLUDE EXISTS " +
    "EXTRACT FALSE FETCH FOLLOWING FOR FROM FULL GROUP GROUPING GROUPS HASH HAVING IF IGNORE IN " +
    "INNER INTERSECT INTERVAL INTO IS JOIN LATERAL LEFT LIKE LIMIT LOOKUP MERGE NATURAL NEW NO NOT " +
    "NULL NULLS OF ON OR ORDER OUTER OVER PARTITION PRECEDING PROTO QUALIFY RANGE RECURSIVE RESPECT " +
    "RIGHT ROLLUP ROWS SELECT SET SOME STRUCT TABLESAMPLE THEN TO TREAT TRUE UNBOUNDED UNION UNNEST " +
    "USING WHEN WHERE WINDOW WITH WITHIN"
  ).split(" "),
);
const sqlColumn = (c: string) => (BQ_RESERVED.has(c.toUpperCase()) ? `\`${c}\`` : c);

/** Parsed schema fields, or [] when the JSON is invalid (validation reports it). */
function parseSchema(text: string): unknown[] {
  try {
    const parsed: unknown = JSON.parse(text);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

const DEFAULT_SCHEMA = JSON.stringify(
  [
    { name: "order_id", type: "STRING", mode: "REQUIRED" },
    { name: "customer_id", type: "STRING", mode: "NULLABLE" },
    { name: "order_date", type: "DATE", mode: "REQUIRED" },
    { name: "amount", type: "NUMERIC", mode: "NULLABLE" },
  ],
  null,
  2,
);

const DEFAULT_CUSTOM_SQL = `-- Every column of the single result row must be TRUE for the check to pass.
SELECT
    COUNT(*) > 0 AS has_rows,
    COUNTIF(order_id IS NULL) = 0 AS no_null_keys
FROM \`{{ cfg.project_id }}.{{ cfg.dataset }}.{{ params.table }}\`
WHERE DATE(_PARTITIONTIME) = DATE('{{ ds }}')
`;

export const gcsToBigquery: Scenario = {
  id: "gcs_to_bigquery",
  title: "GCS → BigQuery load",
  description:
    "Load CSV, JSON, Parquet or Avro files from a GCS bucket into a BigQuery table, idempotently into the run's date partition, with an optional data quality check.",
  naming: { source: TEAM.systemCodes.gcs, target: TEAM.systemCodes.bigquery },
  sections: [
    { id: "source", title: "Source (GCS)" },
    { id: "format", title: "File format options" },
    { id: "schema", title: "Schema" },
    { id: "destination", title: "Destination (BigQuery)" },
    { id: "quality", title: "Data quality" },
  ],
  commonDefaults: {
    dag_id: "sales_gcs_to_bq_daily",
    description: "Load daily sales order files from GCS into BigQuery.",
    tags: ["sales", "gcs", "bigquery"],
  },
  fields: [
    // Source ------------------------------------------------------------------------
    {
      key: "source_bucket", label: "Source bucket", type: "text", section: "source",
      default: "acme-dev-sales-landing", required: true, envSpecific: true, suggestable: true,
      validate: (x) => v.gcsBucket(String(x)), help: "Bucket name only (no gs://). Env-specific.",
    },
    {
      key: "source_objects", label: "Source object paths", type: "list", section: "source",
      default: ["landing/orders/{{ ds_nodash }}/*.csv"], required: true, allowJinja: true,
      validateItem: (x) => v.gcsObject(x),
      help: "One per line, inside the bucket. Wildcards (*) and Airflow macros such as {{ ds_nodash }} are allowed, so each run reads only its own files.",
    },
    {
      key: "source_format", label: "File format", type: "select", section: "source", default: "CSV",
      options: [
        { value: "CSV", label: "CSV" },
        { value: "NEWLINE_DELIMITED_JSON", label: "JSON (newline-delimited)" },
        { value: "PARQUET", label: "Parquet" },
        { value: "AVRO", label: "Avro" },
      ],
    },
    // Format options --------------------------------------------------------------------
    {
      key: "csv_skip_leading_rows", label: "Header rows to skip", type: "number", section: "format",
      default: 1, min: 0, max: 1000, showIf: isCsv,
    },
    {
      key: "csv_field_delimiter", label: "Field delimiter", type: "select", section: "format", default: ",",
      showIf: isCsv,
      options: [
        { value: ",", label: "Comma ( , )" },
        { value: "\t", label: "Tab" },
        { value: "|", label: "Pipe ( | )" },
        { value: ";", label: "Semicolon ( ; )" },
      ],
    },
    {
      key: "csv_quote", label: "Quote character", type: "text", section: "format", default: '"',
      showIf: isCsv, help: "Single character; leave empty if fields are never quoted.",
      validate: (x) => ([...String(x)].length > 1 ? v.err("Use a single character.") : v.ok()),
    },
    {
      key: "csv_encoding", label: "Encoding", type: "select", section: "format", default: "UTF-8", showIf: isCsv,
      options: ["UTF-8", "ISO-8859-1", "UTF-16BE", "UTF-16LE", "UTF-32BE", "UTF-32LE"].map((e) => ({ value: e, label: e })),
    },
    {
      key: "csv_null_marker", label: "Null marker (optional)", type: "text", section: "format", default: "",
      showIf: isCsv, help: 'String that represents NULL, e.g. "\\N". Empty = BigQuery default.',
    },
    {
      key: "csv_allow_quoted_newlines", label: "Allow quoted newlines", type: "boolean", section: "format",
      default: false, showIf: isCsv,
    },
    {
      key: "csv_allow_jagged_rows", label: "Allow jagged rows", type: "boolean", section: "format",
      default: false, showIf: isCsv,
    },
    {
      key: "max_bad_records", label: "Max bad records", type: "number", section: "format", default: 0,
      min: 0, max: 1000000, showIf: isCsvOrJson,
    },
    {
      key: "ignore_unknown_values", label: "Ignore unknown values", type: "boolean", section: "format",
      default: false, showIf: isCsvOrJson,
    },
    {
      key: "parquet_enable_list_inference", label: "Enable list inference", type: "boolean", section: "format",
      default: true, showIf: (p) => p.source_format === "PARQUET",
    },
    {
      key: "avro_use_logical_types", label: "Use Avro logical types", type: "boolean", section: "format",
      default: true, showIf: (p) => p.source_format === "AVRO",
    },
    // Schema ---------------------------------------------------------------------------------
    {
      key: "schema_mode", label: "Schema source", type: "select", section: "schema", default: "autodetect",
      showIf: isCsvOrJson,
      options: [
        { value: "autodetect", label: "Autodetect" },
        { value: "explicit", label: "Schema file (BigQuery JSON schema)" },
        { value: "existing", label: "Use the existing table's schema" },
      ],
      help: "Parquet and Avro are self-describing; their schema comes from the files.",
    },
    {
      key: "schema_json", label: "BigQuery JSON schema", type: "code", language: "json", accept: ".json",
      section: "schema", default: DEFAULT_SCHEMA, required: true,
      showIf: (p) => isCsvOrJson(p) && p.schema_mode === "explicit",
      validate: (x) => v.bqSchemaJson(String(x)),
      help: "Paste or upload the output of `bq show --schema --format=prettyjson`. Embedded in the DAG as SCHEMA_FIELDS.",
    },
    // Destination --------------------------------------------------------------------------
    {
      key: "dataset", label: "Dataset", type: "text", section: "destination", default: "raw_sales",
      required: true, envSpecific: true, suggestable: true, validate: (x) => v.bqDataset(String(x)),
      help: "Env-specific. Convention: {layer}_{domain}.",
    },
    {
      key: "table", label: "Table", type: "text", section: "destination", default: "orders", required: true,
      validate: (x) => v.bqTable(String(x)), help: "snake_case noun, e.g. orders.",
    },
    {
      key: "partition_type", label: "Partitioning", type: "select", section: "destination", default: "DAY",
      options: [
        { value: "DAY", label: "Daily — load into table$YYYYMMDD (idempotent)" },
        { value: "MONTH", label: "Monthly — load into table$YYYYMM (idempotent)" },
        { value: "NONE", label: "Not partitioned" },
      ],
    },
    {
      key: "partition_field", label: "Partition column (optional)", type: "text", section: "destination",
      default: "", showIf: isPartitioned, validate: (x) => v.bqColumn(String(x)),
      help: "Empty = ingestion-time partitioning. If set (DATE/TIMESTAMP column), every row loaded by a run must fall in that run's partition.",
    },
    {
      key: "cluster_fields", label: "Clustering columns", type: "list", section: "destination", default: [],
      validateItem: (x) => v.bqColumn(x), help: "Up to 4, one per line.",
    },
    {
      key: "write_disposition", label: "Write disposition", type: "select", section: "destination",
      default: "WRITE_TRUNCATE",
      options: [
        { value: "WRITE_TRUNCATE", label: "WRITE_TRUNCATE — replace (partition or table)" },
        { value: "WRITE_APPEND", label: "WRITE_APPEND — append (not idempotent)" },
        { value: "WRITE_EMPTY", label: "WRITE_EMPTY — fail if data exists" },
      ],
    },
    {
      key: "create_disposition", label: "Create disposition", type: "select", section: "destination",
      default: "CREATE_IF_NEEDED",
      options: [
        { value: "CREATE_IF_NEEDED", label: "CREATE_IF_NEEDED" },
        { value: "CREATE_NEVER", label: "CREATE_NEVER" },
      ],
    },
    {
      key: "schema_update_options", label: "Schema update options", type: "multiselect", section: "destination",
      default: [],
      options: [
        { value: "ALLOW_FIELD_ADDITION", label: "Allow field addition" },
        { value: "ALLOW_FIELD_RELAXATION", label: "Allow field relaxation (REQUIRED → NULLABLE)" },
      ],
      showIf: (p) =>
        p.write_disposition === "WRITE_APPEND" || (p.write_disposition === "WRITE_TRUNCATE" && isPartitioned(p)),
    },
    // Quality -------------------------------------------------------------------------------
    {
      key: "dq_check", label: "Post-load check", type: "select", section: "quality", default: "row_count",
      options: [
        { value: "row_count", label: "Row count > 0 for the loaded partition" },
        { value: "custom", label: "Custom SQL check (BigQueryCheckOperator)" },
        { value: "none", label: "None" },
      ],
    },
    {
      key: "dq_custom_sql", label: "Check SQL", type: "code", language: "sql", accept: ".sql", section: "quality",
      default: DEFAULT_CUSTOM_SQL, required: true, allowJinja: true, showIf: (p) => p.dq_check === "custom",
      help: "Saved as its own .sql file. Returns one row; every value must be truthy. Use {{ cfg.<key> }} for env values (project_id, dataset, source_bucket), {{ params.table }} and {{ ds }}.",
    },
  ],

  samples: {
    csv_daily_partitioned: {
      dag_id: "sales_gcs_to_bq_daily",
      email: ["data-eng-alerts@example.com"],
      partition_field: "order_date",
      cluster_fields: ["customer_id"],
    },
    json_explicit_schema_variables: {
      dag_id: "events_gcs_to_bq_monthly",
      description: 'Customer "events" from O\'Brien\'s feed — path C:\\temp\\x, {{ not jinja }} ünïcødé',
      domain: "events",
      config_mode: "variables",
      env: "prd",
      project_id: "acme-prd-data",
      source_bucket: "acme-prd-events-landing",
      dataset: "raw_events",
      table: "customer_events",
      impersonation_chain: ["etl-runner@acme-prd-data.iam.gserviceaccount.com"],
      source_objects: ["events/{{ ds_nodash[:6] }}/*.json", "events/late/{{ ds_nodash[:6] }}/*.json"],
      source_format: "NEWLINE_DELIMITED_JSON",
      schema_mode: "explicit",
      partition_type: "MONTH",
      schedule_preset: "custom",
      schedule_cron: "30 2 1 * *",
      timezone: "America/Toronto",
      dq_check: "custom",
      tags: ["events", "gcs", "bigquery"],
    },
    parquet_full_refresh: {
      dag_id: "finance_gcs_to_bq_adhoc",
      description: "Full refresh of the finance ledger snapshot.",
      domain: "finance",
      source_bucket: "acme-dev-finance-landing",
      source_objects: ["ledger/snapshot/*.parquet"],
      source_format: "PARQUET",
      dataset: "raw_finance",
      table: "ledger_snapshot",
      partition_type: "NONE",
      cluster_fields: ["account_id", "posting_date"],
      dq_check: "none",
      schedule_preset: "none",
      retry_exponential_backoff: false,
    },
    avro_append_existing: {
      dag_id: "marketing_gcs_to_bq_daily",
      domain: "marketing",
      source_bucket: "acme-dev-marketing-landing",
      source_objects: ["clicks/dt={{ ds }}/*.avro"],
      source_format: "AVRO",
      dataset: "raw_marketing",
      table: "clicks",
      write_disposition: "WRITE_APPEND",
      schema_update_options: ["ALLOW_FIELD_ADDITION"],
      catchup: true,
      email: ["mkt@example.com", "oncall@example.com"],
      email_on_failure: true,
    },
    csv_tab_existing_schema: {
      dag_id: "ops_gcs_to_bq_hourly",
      domain: "ops",
      config_mode: "variables",
      source_bucket: "acme-dev-ops-landing",
      source_objects: ["metrics/{{ ds_nodash }}/{{ logical_date.strftime('%H') }}/*.tsv.gz"],
      csv_field_delimiter: "\t",
      csv_null_marker: "\\N",
      csv_allow_quoted_newlines: true,
      schema_mode: "existing",
      create_disposition: "CREATE_NEVER",
      dataset: "raw_ops",
      table: "host_metrics",
      partition_field: "event_ts",
      schedule_preset: "@hourly",
      dagrun_timeout_minutes: 50,
      execution_timeout_minutes: 30,
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => {
      out[k] = out[k] ? v.merge(out[k], i) : i;
    };
    const wd = str(p, "write_disposition");
    if (wd === "WRITE_APPEND") {
      add("write_disposition", v.warn("Not idempotent: re-running a date appends duplicate rows. Prefer WRITE_TRUNCATE into a partition."));
    } else if (wd === "WRITE_EMPTY") {
      add("write_disposition", v.warn("Re-runs fail once the partition/table already has data."));
    } else if (!isPartitioned(p)) {
      add("write_disposition", v.warn("Not partitioned: every run replaces the whole table."));
    }
    if (isPartitioned(p) && !list(p, "source_objects").some((o) => o.includes("{{"))) {
      add("source_objects", v.warn("No date macro in the paths: every run loads the same files into its own partition."));
    }
    if (list(p, "cluster_fields").length > 4) add("cluster_fields", v.err("BigQuery allows at most 4 clustering columns."));
    if (isCsvOrJson(p) && p.schema_mode === "existing" && p.create_disposition === "CREATE_IF_NEEDED") {
      add("schema_mode", v.warn("The table must already exist when using its existing schema (consider CREATE_NEVER)."));
    }
    if (p.dq_check === "custom") {
      const sql = str(p, "dq_custom_sql").replace(/;\s*$/, "");
      if (sql.includes(";")) add("dq_custom_sql", v.err("Only a single SELECT statement is allowed."));
    }
    return out;
  },

  suggest(ctx) {
    const layer = DATASET_LAYER_SETS[ctx.layerSet as keyof typeof DATASET_LAYER_SETS]?.[0] ?? "raw";
    const out: Partial<Params> = {};
    const bucket = fillPattern("gcsBucket", { ...ctx, purpose: TEAM.bucketPurposes.landing });
    const dataset = fillPattern("bqDataset", { layer, domain: ctx.domain });
    if (bucket) out.source_bucket = bucket;
    if (dataset) out.dataset = dataset;
    return out;
  },

  generate(p) {
    const ctx = new DagContext(gcsToBigquery, p);
    ctx.importFrom("airflow.providers.google.cloud.operators.bigquery", "BigQueryInsertJobOperator");

    const format = str(p, "source_format");
    const table = str(p, "table");
    const obj = toSnakeIdentifier(table, "table");
    const partition = str(p, "partition_type");
    const partitionField = str(p, "partition_field");
    const schemaMode = isCsvOrJson(p) ? str(p, "schema_mode") : "self_describing";
    const decorator =
      partition === "DAY" ? "${{ ds_nodash }}" : partition === "MONTH" ? "${{ logical_date.strftime('%Y%m') }}" : "";

    // --- load configuration -------------------------------------------------------
    const load: Array<[string, Py]> = [
      [
        "sourceUris",
        pyList(
          list(p, "source_objects").map((o) =>
            pyStr(
              guardTemplateExt(`gs://${ctx.ref("source_bucket")}/${o.replace(/^\/+/, "")}`, BQ_INSERT_JOB_TEMPLATE_EXT),
            ),
          ),
        ),
      ],
      [
        "destinationTable",
        pyDict(
          [
            ["projectId", pyStr(ctx.ref("project_id"))],
            ["datasetId", pyStr(ctx.ref("dataset"))],
            ["tableId", pyStr(table + decorator)],
          ],
          { explode: true },
        ),
      ],
      ["sourceFormat", pyStr(format)],
      ["writeDisposition", pyStr(str(p, "write_disposition"))],
      ["createDisposition", pyStr(str(p, "create_disposition"))],
    ];
    if (schemaMode === "autodetect") load.push(["autodetect", pyBool(true)]);
    if (schemaMode === "explicit") load.push(["schema", pyDict([["fields", pyName("SCHEMA_FIELDS")]])]);
    if (partition !== "NONE") {
      const tp: Array<[string, Py]> = [["type", pyStr(partition)]];
      if (partitionField) tp.push(["field", pyStr(partitionField)]);
      load.push(["timePartitioning", pyDict(tp)]);
    }
    const clusters = list(p, "cluster_fields");
    if (clusters.length) load.push(["clustering", pyDict([["fields", pyList(clusters.map(pyStr))]])]);
    const sizeOpts = list(p, "schema_update_options");
    const showSuo =
      str(p, "write_disposition") === "WRITE_APPEND" || (str(p, "write_disposition") === "WRITE_TRUNCATE" && partition !== "NONE");
    if (showSuo && sizeOpts.length) load.push(["schemaUpdateOptions", pyList(sizeOpts.map(pyStr))]);
    if (format === "CSV") {
      load.push(
        ["skipLeadingRows", pyInt(num(p, "csv_skip_leading_rows"))],
        ["fieldDelimiter", pyStr(String(p.csv_field_delimiter ?? ","))],
        ["quote", pyStr(String(p.csv_quote ?? ""))],
        ["encoding", pyStr(str(p, "csv_encoding"))],
        ["allowQuotedNewlines", pyBool(bool(p, "csv_allow_quoted_newlines"))],
        ["allowJaggedRows", pyBool(bool(p, "csv_allow_jagged_rows"))],
      );
      if (String(p.csv_null_marker ?? "") !== "") load.push(["nullMarker", pyStr(String(p.csv_null_marker))]);
    }
    if (isCsvOrJson(p)) {
      load.push(
        ["maxBadRecords", pyInt(num(p, "max_bad_records"))],
        ["ignoreUnknownValues", pyBool(bool(p, "ignore_unknown_values"))],
      );
    }
    if (format === "PARQUET") {
      load.push(["parquetOptions", pyDict([["enableListInference", pyBool(bool(p, "parquet_enable_list_inference"))]])]);
    }
    if (format === "AVRO") load.push(["useAvroLogicalTypes", pyBool(bool(p, "avro_use_logical_types"))]);

    const loadTaskId = `load_${obj}_to_bq`;
    const loadTask = pyCall(
      "BigQueryInsertJobOperator",
      [],
      [
        ["task_id", pyStr(loadTaskId)],
        ["project_id", pyStr(ctx.ref("project_id"))],
        ["location", pyName("LOCATION")],
        [
          "configuration",
          pyDict(
            [
              ["load", pyDict(load, { explode: true })],
              ["labels", pyName("LABELS")],
            ],
            { explode: true },
          ),
        ],
        ["gcp_conn_id", pyName("GCP_CONN_ID")],
        ["impersonation_chain", ctx.impersonationChain()],
        ["deferrable", ctx.deferrable()],
      ],
      { explode: true },
    );

    // --- optional data quality check ----------------------------------------------------
    const dq = str(p, "dq_check");
    let checkTaskId: string | null = null;
    let checkTask: Py | null = null;
    if (dq === "row_count" || dq === "custom") {
      ctx.importFrom("airflow.providers.google.cloud.operators.bigquery", "BigQueryCheckOperator");
      let sqlName: string;
      if (dq === "row_count") {
        checkTaskId = `check_${obj}_row_count`;
        const col = partitionField ? `DATE(${sqlColumn(partitionField)})` : "DATE(_PARTITIONTIME)";
        const where =
          partition === "DAY"
            ? `WHERE ${col} = DATE('{{ ds }}')`
            : partition === "MONTH"
              ? `WHERE DATE_TRUNC(${col}, MONTH) = DATE_TRUNC(DATE('{{ ds }}'), MONTH)`
              : null;
        const sql = [
          ...ctx.sqlHeader(
            partition === "NONE"
              ? "Data quality: fail when the table is empty after the load."
              : "Data quality: fail when the partition loaded by this run is empty.",
          ),
          "SELECT COUNT(*) > 0 AS has_rows",
          `FROM \`${ctx.sqlRef("project_id")}.${ctx.sqlRef("dataset")}.{{ params.table }}\``,
          ...(where ? [where] : []),
        ].join("\n");
        sqlName = ctx.addSql(checkTaskId, sql);
      } else {
        checkTaskId = `check_${obj}_quality`;
        const userSql = String(p.dq_custom_sql ?? "").replace(/\r\n?/g, "\n").trim();
        const sql = [...ctx.sqlHeader("Data quality: custom check (every value in the result row must be TRUE)."), userSql].join("\n");
        sqlName = ctx.addSql(checkTaskId, sql);
      }
      checkTask = pyCall(
        "BigQueryCheckOperator",
        [],
        [
          ["task_id", pyStr(checkTaskId)],
          ["sql", pyStr(sqlName)],
          ["params", pyDict([["table", pyStr(table)]])],
          ["use_legacy_sql", pyBool(false)],
          ["location", pyName("LOCATION")],
          ["labels", pyName("LABELS")],
          ["gcp_conn_id", pyName("GCP_CONN_ID")],
          ["impersonation_chain", ctx.impersonationChain()],
          ["deferrable", ctx.deferrable()],
        ],
        { explode: true },
      );
    }

    // --- module constants ------------------------------------------------------------
    const constants: string[] = [];
    if (schemaMode === "explicit") {
      const fields = parseSchema(String(p.schema_json ?? "[]"));
      constants.push(
        comment("BigQuery schema for the destination table (from the uploaded schema file)."),
        assign("SCHEMA_FIELDS", pyList(fields.map((f) => pyFromJson(f)), { explode: true })),
      );
    }

    // --- DAG body ------------------------------------------------------------------------
    // Related tasks are grouped in a TaskGroup; a lone task needs no group.
    const body: string[] = [];
    if (checkTask && checkTaskId) {
      ctx.importFrom("airflow.utils.task_group", "TaskGroup");
      body.push(
        withHeader(
          pyCall("TaskGroup", [], [
            ["group_id", pyStr("ingest")],
            ["tooltip", pyStr("Load files from GCS into BigQuery and check the result")],
          ]),
          null,
          1,
        ),
        assign(loadTaskId, loadTask, 2),
        "",
        assign(checkTaskId, checkTask, 2),
        "",
        `    ${pyIdent(loadTaskId)} >> ${pyIdent(checkTaskId)}`,
      );
    } else {
      body.push(assign(loadTaskId, loadTask, 1));
    }

    // --- docs --------------------------------------------------------------------------------
    const fmtLabel: Record<string, string> = {
      CSV: "CSV",
      NEWLINE_DELIMITED_JSON: "newline-delimited JSON",
      PARQUET: "Parquet",
      AVRO: "Avro",
    };
    const target = `\`<project_id>.<dataset>.${table}\``;
    const docSteps = [
      `\`${loadTaskId}\`: load ${fmtLabel[format] ?? format} files (${list(p, "source_objects")
        .map((o) => `\`${o}\``)
        .join(", ")}) from the \`source_bucket\` into ${target}` +
        (partition === "NONE" ? "" : ` partition \`${decorator}\``) +
        ` with ${str(p, "write_disposition")}.`,
    ];
    if (checkTaskId) {
      docSteps.push(
        dq === "row_count"
          ? `\`${checkTaskId}\`: fail if the loaded ${partition === "NONE" ? "table" : "partition"} has no rows.`
          : `\`${checkTaskId}\`: custom SQL data quality check.`,
      );
    }

    const iamRoles = [
      "`roles/bigquery.jobUser` on the project that runs the jobs",
      "`roles/bigquery.dataEditor` on the destination dataset",
      "`roles/storage.objectViewer` on the source bucket",
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
        "The load job is idempotent when it writes with WRITE_TRUNCATE into the run's partition decorator; re-running a date replaces that partition only.",
        "Labels (env, team, owner, cost_center, dag_id) are set on every BigQuery job for cost tracking.",
        ...(bool(p, "deferrable")
          ? ["Operators run in deferrable mode: make sure the Composer environment has a triggerer (Composer 2: set triggerer count >= 1)."]
          : []),
      ],
    });
  },
};
