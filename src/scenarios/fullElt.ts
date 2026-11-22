/**
 * Scenario — Full ELT pipeline:
 *   sensor → load staging → [check] → [create final table] → MERGE → archive → notify on failure.
 *
 *   ingest     wait_for_<table>_files      GCSObjectExistenceSensor (use_glob=True)
 *              load_<table>_to_staging     BigQueryInsertJobOperator load, WRITE_TRUNCATE into the
 *                                          staging table's run-date partition (staging$YYYYMMDD)
 *   transform  check_<table>_staging       optional BigQueryCheckOperator (rows present, no NULL keys)
 *              create_<table>_if_missing   optional CREATE TABLE IF NOT EXISTS ... AS SELECT ... WHERE FALSE
 *              merge_<table>_from_staging  BigQueryInsertJobOperator query job running a MERGE (.sql file)
 *   finalize   archive_<table>_files       GCSToGCSOperator(move_object=True)
 *              notify_pipeline_failure     PythonOperator, trigger_rule=ONE_FAILED: logs (and emails)
 *                                          the failed tasks, then fails so the DAG run is marked failed
 *
 * Why a PythonOperator instead of EmailOperator for the failure alert: a task with
 * trigger_rule=ONE_FAILED that *succeeds* becomes the DAG's only leaf, so Airflow
 * would mark a failed pipeline run as "success". The alert task therefore sends the
 * email itself (airflow.utils.email.send_email, the same backend EmailOperator uses)
 * and then raises AirflowFailException. When everything succeeds it is skipped.
 *
 * Landing/archive/load helpers are shared with gcsSensorLoadArchive.ts.
 */
import { DATASET_LAYER_SETS, fillPattern, TEAM } from "../config/naming";
import {
  assign,
  comment,
  pyBool,
  pyCall,
  pyDict,
  pyIdent,
  pyInt,
  pyList,
  pyName,
  pyStr,
  toSnakeIdentifier,
  type Py,
} from "../lib/python";
import * as v from "../validation/rules";
import { bool, DagContext, list, str } from "./common";
import {
  archiveFields,
  archiveTask,
  bqLoadTask,
  dependencyChain,
  FORMAT_LABEL,
  formatFields,
  formatLoadOptions,
  isCsvOrJson,
  landingFields,
  landingSourceUri,
  schemaColumnNames,
  schemaConstants,
  schemaModeOf,
  sensorTask,
  sqlColumn,
  taskGroupBlock,
  validateLanding,
} from "./gcsSensorLoadArchive";
import type { Params, Scenario } from "./types";

/** Max line length enforced by sqlfluff (.sqlfluff), minus a small margin. */
const SQL_WIDTH = 116;

const DEFAULT_SCHEMA = JSON.stringify(
  [
    { name: "order_id", type: "STRING", mode: "REQUIRED" },
    { name: "customer_id", type: "STRING", mode: "NULLABLE" },
    { name: "order_date", type: "DATE", mode: "NULLABLE" },
    { name: "order_status", type: "STRING", mode: "NULLABLE" },
    { name: "amount", type: "NUMERIC", mode: "NULLABLE" },
    { name: "updated_at", type: "TIMESTAMP", mode: "NULLABLE" },
  ],
  null,
  2,
);

/** `(a, b)`-style list on one line, or exploded one item per line when too long. */
function sqlList(open: string, items: string[], indent: string): string[] {
  const one = `${indent}${open}(${items.join(", ")})`;
  if (one.length <= SQL_WIDTH) return [one];
  return [`${indent}${open}(`, ...items.map((x, i) => `${indent}    ${x}${i < items.length - 1 ? "," : ""}`), `${indent})`];
}

/** `[a, b, c] >> watcher` at DAG level, with the list exploded (as ruff formats it) when too long. */
function watcherDependency(upstream: string[], watcher: string): string[] {
  const one = dependencyChain([upstream, watcher], 1);
  if (!one.includes("\n")) return [one];
  return ["    [", ...upstream.map((id) => `        ${pyIdent(id)},`), `    ] >> ${pyIdent(watcher)}`];
}

/** Python source of the failure-alert callable (static code, no user values). */
function alertFunction(withEmail: boolean): string[] {
  return [
    withEmail ? "def alert_pipeline_failure(alert_emails, **context):" : "def alert_pipeline_failure(**context):",
    `    """Log${withEmail ? " and email" : ""} the failed tasks, then fail so the DAG run is marked failed."""`,
    '    dag_run = context["dag_run"]',
    "    failed_tis = dag_run.get_task_instances(state=[TaskInstanceState.FAILED])",
    '    failed = ", ".join(sorted(ti.task_id for ti in failed_tis)) or "unknown"',
    '    message = f"{DAG_ID} ({ENV}) run {dag_run.run_id} failed. Failed tasks: {failed}"',
    "    log.error(message)",
    ...(withEmail
      ? [
          "    if alert_emails:",
          "        send_email(",
          "            to=alert_emails,",
          '            subject=f"[{ENV}] Airflow DAG {DAG_ID} failed",',
          "            html_content=message,",
          "        )",
        ]
      : []),
    "    raise AirflowFailException(message)",
  ];
}

export const fullElt: Scenario = {
  id: "full_elt",
  title: "Full ELT pipeline",
  description:
    "Wait for files in GCS, load them into a BigQuery staging partition, MERGE them into the final table, archive the processed files and alert if any step fails.",
  naming: { source: TEAM.systemCodes.gcs, target: TEAM.systemCodes.bigquery },
  sections: [
    { id: "source", title: "Landing files (GCS)", description: "Where files arrive and how long to wait for them." },
    { id: "format", title: "File format options" },
    { id: "schema", title: "Staging schema" },
    { id: "staging", title: "Staging table (BigQuery)", description: "Each run replaces its own daily partition of the staging table." },
    { id: "merge", title: "Final table (MERGE)", description: "How staged rows are upserted into the final table." },
    { id: "quality", title: "Data quality" },
    { id: "archive", title: "Archive", description: "Where processed files go after a successful MERGE." },
    { id: "alerts", title: "Failure alert" },
  ],
  commonDefaults: {
    dag_id: "orders_gcs_to_bq_daily",
    description: "Daily orders ELT: load files to staging, MERGE into the curated table, archive the files.",
    domain: "orders",
    tags: ["orders", "elt", "bigquery"],
  },
  fields: [
    ...landingFields({ bucket: "acme-dev-orders-landing", prefix: "incoming/orders/dt={{ ds }}/", pattern: "*.csv" }),
    ...formatFields(DEFAULT_SCHEMA),
    // Staging ------------------------------------------------------------------------
    {
      key: "staging_dataset", label: "Staging dataset", type: "text", section: "staging", default: "stg_orders",
      required: true, envSpecific: true, suggestable: true, validate: (x) => v.bqDataset(String(x)),
      help: "Env-specific. Convention: {layer}_{domain}, e.g. stg_orders.",
    },
    {
      key: "staging_table", label: "Staging table", type: "text", section: "staging", default: "orders_staging",
      required: true, validate: (x) => v.bqTable(String(x)),
      help: "Created on the first run as a daily, ingestion-time partitioned table. Each run loads its files into staging$YYYYMMDD with WRITE_TRUNCATE.",
    },
    // Merge ----------------------------------------------------------------------------
    {
      key: "dataset", label: "Final dataset", type: "text", section: "merge", default: "curated_orders",
      required: true, envSpecific: true, suggestable: true, validate: (x) => v.bqDataset(String(x)),
      help: "Env-specific. Convention: {layer}_{domain}, e.g. curated_orders.",
    },
    {
      key: "table", label: "Final table", type: "text", section: "merge", default: "orders", required: true,
      validate: (x) => v.bqTable(String(x)), help: "snake_case noun, e.g. orders.",
    },
    {
      key: "merge_keys", label: "Merge key columns", type: "list", section: "merge", default: ["order_id"],
      required: true, validateItem: (x) => v.bqColumn(x),
      help: "One per line. Together they identify a row: matching rows are updated, new keys are inserted. Staged rows with a NULL key are skipped.",
    },
    {
      key: "merge_columns", label: "Columns to merge", type: "list", section: "merge",
      default: ["order_id", "customer_id", "order_date", "order_status", "amount", "updated_at"],
      required: true, validateItem: (x) => v.bqColumn(x),
      help: "One per line, including the key columns. Copied from staging into the final table; non-key columns are updated on a match.",
    },
    {
      key: "dedupe_column", label: "Latest-version column (optional)", type: "text", section: "merge",
      default: "updated_at", validate: (x) => v.bqColumn(String(x)),
      help: "If a run's files contain the same key more than once, keep the row with the highest value of this column (e.g. updated_at). Empty = no de-duplication; the MERGE then fails on duplicate keys.",
    },
    {
      key: "create_target_if_missing", label: "Create the final table if missing", type: "boolean", section: "merge",
      default: true,
      help: "Adds a CREATE TABLE IF NOT EXISTS step that copies the column types from the staging table, clustered by the merge keys.",
    },
    {
      key: "target_partition_field", label: "Final table partition column (optional)", type: "text", section: "merge",
      default: "order_date", showIf: (p) => p.create_target_if_missing === true, validate: (x) => v.bqColumn(String(x)),
      help: "Used only when the table is created. Must be one of the merged columns. Empty = not partitioned.",
    },
    {
      key: "target_partition_kind", label: "Partition column type", type: "select", section: "merge", default: "DATE",
      showIf: (p) => p.create_target_if_missing === true && str(p, "target_partition_field") !== "",
      options: [
        { value: "DATE", label: "DATE column — PARTITION BY column" },
        { value: "TIMESTAMP", label: "TIMESTAMP / DATETIME column — PARTITION BY DATE(column)" },
      ],
    },
    // Quality ------------------------------------------------------------------------------
    {
      key: "dq_check", label: "Check before MERGE", type: "select", section: "quality", default: "staging",
      options: [
        { value: "staging", label: "Staged partition has rows and no NULL merge keys" },
        { value: "none", label: "None" },
      ],
      help: "Stops the run before the final table is touched if the check fails.",
    },
    // Archive / alerts -----------------------------------------------------------------------
    ...archiveFields({ bucket: "acme-dev-orders-archive", prefix: "orders/dt={{ ds }}/" }),
    {
      key: "failure_alert", label: "When a task fails", type: "select", section: "alerts", default: "email",
      options: [
        { value: "email", label: "Log the failed tasks and email them to the alert emails" },
        { value: "log", label: "Log the failed tasks only" },
      ],
      help: "A final notify_pipeline_failure task runs only if a task fails. Email needs SMTP / SendGrid configured in Composer and uses the Alert emails (DAG section).",
    },
  ],

  samples: {
    csv_daily_explicit_schema: {
      dag_id: "orders_gcs_to_bq_daily",
      email: ["orders-oncall@example.com"],
      schema_mode: "explicit",
    },
    json_weekly_composite_key_variables: {
      dag_id: "inventory_gcs_to_bq_weekly",
      description: "Weekly warehouse stock counts upserted by warehouse and SKU.",
      domain: "inventory",
      config_mode: "variables",
      env: "prd",
      project_id: "acme-prd-data",
      source_bucket: "acme-prd-inventory-landing",
      source_prefix: "counts/week={{ ds_nodash }}/",
      file_pattern: "*.json",
      source_format: "NEWLINE_DELIMITED_JSON",
      staging_dataset: "stg_inventory",
      staging_table: "stock_counts_staging",
      dataset: "curated_inventory",
      table: "stock_counts",
      merge_keys: ["warehouse_id", "sku"],
      merge_columns: ["warehouse_id", "sku", "quantity", "counted_at"],
      dedupe_column: "",
      create_target_if_missing: false,
      dq_check: "none",
      archive_bucket: "acme-prd-inventory-archive",
      archive_prefix: "counts/week={{ ds_nodash }}/",
      failure_alert: "log",
      email_on_failure: false,
      schedule_preset: "@weekly",
      tags: ["inventory", "elt", "bigquery"],
    },
    parquet_hourly_wide: {
      dag_id: "billing_gcs_to_bq_hourly",
      description: "Hourly usage events: stage, MERGE into the curated usage table, archive.",
      domain: "billing",
      deferrable: false,
      source_bucket: "acme-dev-billing-landing",
      source_prefix: "usage/{{ ds_nodash }}/{{ logical_date.strftime('%H') }}/",
      file_pattern: "usage_*.parquet",
      source_format: "PARQUET",
      sensor_timeout_minutes: 40,
      sensor_poke_interval_seconds: 120,
      staging_dataset: "stg_billing",
      staging_table: "usage_events_staging",
      dataset: "curated_billing",
      table: "usage_events",
      merge_keys: ["account_id", "meter_id", "event_id"],
      merge_columns: [
        "account_id",
        "meter_id",
        "event_id",
        "usage_quantity",
        "unit_of_measure",
        "unit_price",
        "currency_code",
        "event_ts",
        "ingested_at",
        "source_system",
        "region",
      ],
      dedupe_column: "ingested_at",
      target_partition_field: "event_ts",
      target_partition_kind: "TIMESTAMP",
      archive_bucket: "acme-dev-billing-archive",
      archive_prefix: "usage/{{ ds_nodash }}/{{ logical_date.strftime('%H') }}/",
      email: ["billing-oncall@example.com", "data-eng-alerts@example.com"],
      schedule_preset: "@hourly",
      dagrun_timeout_minutes: 55,
      tags: ["billing", "elt", "bigquery"],
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => {
      out[k] = out[k] ? v.merge(out[k], i) : i;
    };
    validateLanding(p, add);

    const keys = list(p, "merge_keys");
    const cols = list(p, "merge_columns");
    const lc = (xs: string[]) => xs.map((x) => x.toLowerCase());
    const colSet = new Set(lc(cols));
    const dupes = (xs: string[]) => lc(xs).filter((x, i, a) => a.indexOf(x) !== i);
    if (dupes(keys).length) add("merge_keys", v.err(`Duplicate key column(s): ${[...new Set(dupes(keys))].join(", ")}.`));
    if (dupes(cols).length) add("merge_columns", v.err(`Duplicate column(s): ${[...new Set(dupes(cols))].join(", ")}.`));
    const missingKeys = keys.filter((k) => !colSet.has(k.toLowerCase()));
    if (missingKeys.length) add("merge_keys", v.err(`Also list ${missingKeys.join(", ")} in "Columns to merge".`));

    const partitionField = str(p, "target_partition_field");
    if (bool(p, "create_target_if_missing") && partitionField && !colSet.has(partitionField.toLowerCase())) {
      add("target_partition_field", v.err("Must be one of the columns to merge."));
    }

    if (schemaModeOf(p) === "explicit") {
      const schemaCols = schemaColumnNames(String(p.schema_json ?? ""));
      if (schemaCols.size) {
        const notInSchema = cols.filter((c) => !schemaCols.has(c.toLowerCase()));
        if (notInSchema.length) add("merge_columns", v.err(`Not in the staging schema: ${notInSchema.join(", ")}.`));
        const dedupe = str(p, "dedupe_column");
        if (dedupe && !schemaCols.has(dedupe.toLowerCase())) add("dedupe_column", v.err("Not in the staging schema."));
      }
    }
    if (isCsvOrJson(p) && p.schema_mode === "existing") {
      add("schema_mode", v.warn("The staging table must already exist and be ingestion-time partitioned by day."));
    }
    if (str(p, "failure_alert") === "email" && list(p, "email").length === 0) {
      add("failure_alert", v.warn("No alert emails are set (DAG section): the failure task will only log."));
    }
    return out;
  },

  suggest(ctx) {
    const layers = DATASET_LAYER_SETS[ctx.layerSet as keyof typeof DATASET_LAYER_SETS] ?? DATASET_LAYER_SETS.classic;
    const out: Partial<Params> = {};
    const landing = fillPattern("gcsBucket", { ...ctx, purpose: TEAM.bucketPurposes.landing });
    const archive = fillPattern("gcsBucket", { ...ctx, purpose: TEAM.bucketPurposes.archive });
    const staging = fillPattern("bqDataset", { layer: layers[1], domain: ctx.domain });
    const final = fillPattern("bqDataset", { layer: layers[2], domain: ctx.domain });
    if (landing) out.source_bucket = landing;
    if (archive) out.archive_bucket = archive;
    if (staging) out.staging_dataset = staging;
    if (final) out.dataset = final;
    return out;
  },

  generate(p) {
    const ctx = new DagContext(fullElt, p);
    const table = str(p, "table");
    const stagingTable = str(p, "staging_table");
    const obj = toSnakeIdentifier(table, "table");
    const keys = list(p, "merge_keys");
    const cols = list(p, "merge_columns");
    const keySet = new Set(keys.map((k) => k.toLowerCase()));
    const nonKeys = cols.filter((c) => !keySet.has(c.toLowerCase()));
    const dedupe = str(p, "dedupe_column");
    const emails = list(p, "email");
    const withEmail = str(p, "failure_alert") === "email" && emails.length > 0;

    const ids = {
      wait: `wait_for_${obj}_files`,
      load: `load_${obj}_to_staging`,
      check: `check_${obj}_staging`,
      create: `create_${obj}_if_missing`,
      merge: `merge_${obj}_from_staging`,
      archive: `archive_${obj}_files`,
      notify: "notify_pipeline_failure",
    };
    // Table names are validated (no Jinja delimiters or backticks) and written straight
    // into the back-quoted references; env-specific project/datasets come from cfg.
    const target = `\`${ctx.sqlRef("project_id")}.${ctx.sqlRef("dataset")}.${table}\``;
    const staging = `\`${ctx.sqlRef("project_id")}.${ctx.sqlRef("staging_dataset")}.${stagingTable}\``;
    const runPartition = "_PARTITIONTIME = TIMESTAMP('{{ ds }}')";

    const queryJob = (taskId: string, sqlFile: string): Py => {
      ctx.importFrom("airflow.providers.google.cloud.operators.bigquery", "BigQueryInsertJobOperator");
      return pyCall(
        "BigQueryInsertJobOperator",
        [],
        [
          ["task_id", pyStr(taskId)],
          ["project_id", pyStr(ctx.ref("project_id"))],
          ["location", pyName("LOCATION")],
          [
            "configuration",
            pyDict(
              [
                ["query", pyDict([["query", pyStr(sqlFile)], ["useLegacySql", pyBool(false)]])],
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
    };

    // --- ingest -------------------------------------------------------------------------
    const load: Array<[string, Py]> = [
      ["sourceUris", pyList([pyStr(landingSourceUri(ctx, p))])],
      [
        "destinationTable",
        pyDict(
          [
            ["projectId", pyStr(ctx.ref("project_id"))],
            ["datasetId", pyStr(ctx.ref("staging_dataset"))],
            ["tableId", pyStr(`${stagingTable}\${{ ds_nodash }}`)],
          ],
          { explode: true },
        ),
      ],
      ["writeDisposition", pyStr("WRITE_TRUNCATE")],
      ["createDisposition", pyStr(isCsvOrJson(p) && p.schema_mode === "existing" ? "CREATE_NEVER" : "CREATE_IF_NEEDED")],
      ["timePartitioning", pyDict([["type", pyStr("DAY")]])],
      ...formatLoadOptions(p),
    ];
    const ingest: Array<[string, Py]> = [
      [ids.wait, sensorTask(ctx, p, ids.wait)],
      [ids.load, bqLoadTask(ctx, ids.load, load)],
    ];

    // --- transform ------------------------------------------------------------------------
    const transform: Array<[string, Py]> = [];
    if (str(p, "dq_check") === "staging") {
      ctx.importFrom("airflow.providers.google.cloud.operators.bigquery", "BigQueryCheckOperator");
      const nullConds = keys.map((k) => `${sqlColumn(k)} IS NULL`);
      const nullOne = `    COUNTIF(${nullConds.join(" OR ")}) = 0 AS no_null_keys`;
      const nullCheck =
        nullOne.length <= SQL_WIDTH
          ? [nullOne]
          : ["    COUNTIF(", ...nullConds.map((c, i) => `        ${i ? "OR " : ""}${c}`), "    ) = 0 AS no_null_keys"];
      const sql = [
        ...ctx.sqlHeader("Data quality: the staged partition of this run has rows and no NULL merge keys."),
        "SELECT",
        "    COUNT(*) > 0 AS has_rows,",
        ...nullCheck,
        `FROM ${staging}`,
        `WHERE ${runPartition}`,
      ].join("\n");
      transform.push([
        ids.check,
        pyCall(
          "BigQueryCheckOperator",
          [],
          [
            ["task_id", pyStr(ids.check)],
            ["sql", pyStr(ctx.addSql(ids.check, sql))],
            ["use_legacy_sql", pyBool(false)],
            ["location", pyName("LOCATION")],
            ["labels", pyName("LABELS")],
            ["gcp_conn_id", pyName("GCP_CONN_ID")],
            ["impersonation_chain", ctx.impersonationChain()],
            // Not deferrable: the deferrable path of BigQueryCheckOperator drops labels (providers-google 12.0.0).
            ["deferrable", pyBool(false)],
          ],
          { explode: true },
        ),
      ]);
    }

    // SELECT list: one column stays on the SELECT line (sqlfluff LT09), otherwise one per line.
    const selectCols =
      cols.length === 1
        ? [`SELECT ${sqlColumn(cols[0])}`]
        : ["SELECT", ...cols.map((c, i) => `    ${sqlColumn(c)}${i < cols.length - 1 ? "," : ""}`)];
    const createTarget = bool(p, "create_target_if_missing");
    if (createTarget) {
      const partitionField = str(p, "target_partition_field");
      const partitionBy = partitionField
        ? str(p, "target_partition_kind") === "TIMESTAMP"
          ? [`PARTITION BY DATE(${sqlColumn(partitionField)})`]
          : [`PARTITION BY ${sqlColumn(partitionField)}`]
        : [];
      const sql = [
        ...ctx.sqlHeader("Create the final table on the first run, with the staging table's column types. No-op afterwards."),
        `CREATE TABLE IF NOT EXISTS ${target}`,
        ...partitionBy,
        `CLUSTER BY ${keys.slice(0, 4).map(sqlColumn).join(", ")}`,
        "AS",
        ...selectCols,
        `FROM ${staging}`,
        "WHERE FALSE",
      ].join("\n");
      transform.push([ids.create, queryJob(ids.create, ctx.addSql(ids.create, sql))]);
    }

    const on = keys.map((k) => `t.${sqlColumn(k)} = s.${sqlColumn(k)}`);
    const onLines =
      `    ON ${on.join(" AND ")}`.length <= SQL_WIDTH
        ? [`    ON ${on.join(" AND ")}`]
        : ["    ON", ...on.map((c, i) => `        ${i ? "AND " : ""}${c}`)];
    const partitionByKeys = `PARTITION BY ${keys.map(sqlColumn).join(", ")}`;
    const orderBy = `ORDER BY ${sqlColumn(dedupe)} DESC`;
    const qualifyOne = `    QUALIFY ROW_NUMBER() OVER (${partitionByKeys} ${orderBy}) = 1`;
    const qualify = !dedupe
      ? []
      : qualifyOne.length <= SQL_WIDTH
        ? [qualifyOne]
        : ["    QUALIFY ROW_NUMBER() OVER (", `        ${partitionByKeys}`, `        ${orderBy}`, "    ) = 1"];
    const mergeSql = [
      ...ctx.sqlHeader("Upsert this run's staged rows into the final table (idempotent: re-runs update the same rows)."),
      `MERGE ${target} AS t`,
      "USING (",
      ...selectCols.map((l) => `    ${l}`),
      `    FROM ${staging}`,
      "    WHERE",
      `        ${runPartition}`,
      ...keys.map((k) => `        AND ${sqlColumn(k)} IS NOT NULL`),
      ...qualify,
      ") AS s",
      ...onLines,
      ...(nonKeys.length
        ? [
            "WHEN MATCHED THEN",
            "    UPDATE",
            "        SET",
            ...nonKeys.map((c, i) => `            ${sqlColumn(c)} = s.${sqlColumn(c)}${i < nonKeys.length - 1 ? "," : ""}`),
          ]
        : []),
      "WHEN NOT MATCHED THEN",
      ...sqlList("INSERT ", cols.map(sqlColumn), "    "),
      ...sqlList("VALUES ", cols.map((c) => `s.${sqlColumn(c)}`), "    "),
    ].join("\n");
    transform.push([ids.merge, queryJob(ids.merge, ctx.addSql(ids.merge, mergeSql))]);

    // --- finalize ---------------------------------------------------------------------------
    ctx.importFrom("airflow.operators.python", "PythonOperator");
    ctx.importFrom("airflow.utils.trigger_rule", "TriggerRule");
    ctx.importFrom("airflow.exceptions", "AirflowFailException");
    ctx.importFrom("airflow.utils.state", "TaskInstanceState");
    if (withEmail) ctx.importFrom("airflow.utils.email", "send_email");
    const notifyTask = pyCall(
      "PythonOperator",
      [],
      [
        ["task_id", pyStr(ids.notify)],
        ["python_callable", pyName("alert_pipeline_failure")],
        ["op_kwargs", withEmail ? pyDict([["alert_emails", pyList(emails.map(pyStr))]]) : null],
        ["trigger_rule", pyName("TriggerRule.ONE_FAILED")],
        ["retries", pyInt(0)],
        ["email_on_failure", pyBool(false)],
      ],
      { explode: true },
    );

    // --- DAG body -----------------------------------------------------------------------------
    const body = [...taskGroupBlock(ctx, "ingest", "Wait for the landing files and load them into staging", ingest), ""];
    let transformNode: string = ids.merge;
    if (transform.length > 1) {
      transformNode = "transform";
      body.push(...taskGroupBlock(ctx, "transform", "Check the staged rows and MERGE them into the final table", transform), "");
    } else {
      body.push(assign(ids.merge, transform[0][1], 1), "");
    }
    body.push(
      ...taskGroupBlock(
        ctx,
        "finalize",
        "Archive the processed files; alert if any task failed",
        [
          [ids.archive, archiveTask(ctx, p, ids.archive)],
          [ids.notify, notifyTask],
        ],
        [],
      ),
      "",
      comment("Happy path, then the failure alert watching every task.", 1),
      dependencyChain(["ingest", transformNode, ids.archive], 1),
      ...watcherDependency([...ingest, ...transform].map(([id]) => id).concat(ids.archive), ids.notify),
    );

    const constants = [
      ...(schemaModeOf(p) === "explicit" ? [...schemaConstants(String(p.schema_json ?? "[]")), ""] : []),
      "",
      ...alertFunction(withEmail),
    ];

    // --- docs ---------------------------------------------------------------------------------
    const files = `\`${str(p, "source_prefix")}${str(p, "file_pattern")}\``;
    const docSteps = [
      `\`${ids.wait}\`: wait for a file matching ${files} in the \`source_bucket\`.`,
      `\`${ids.load}\`: load the ${FORMAT_LABEL[str(p, "source_format")] ?? ""} files into \`<staging_dataset>.${stagingTable}\` partition \`\${{ ds_nodash }}\` with WRITE_TRUNCATE.`,
      ...(transform.some(([id]) => id === ids.check)
        ? [`\`${ids.check}\`: fail if the staged partition is empty or has NULL merge keys.`]
        : []),
      ...(createTarget ? [`\`${ids.create}\`: create \`<dataset>.${table}\` if it does not exist yet.`] : []),
      `\`${ids.merge}\`: MERGE the staged rows into \`<dataset>.${table}\` on ${keys.map((k) => `\`${k}\``).join(", ")}` +
        (dedupe ? `, keeping the latest \`${dedupe}\` per key.` : "."),
      `\`${ids.archive}\`: ${str(p, "archive_action") === "copy" ? "copy" : "move"} the processed files to \`${str(p, "archive_prefix")}\` in the \`archive_bucket\`.`,
      `\`${ids.notify}\`: runs only if a task fails; logs${withEmail ? " and emails" : ""} the failed tasks and marks the run failed.`,
    ];

    const move = str(p, "archive_action") !== "copy";
    const iamRoles = [
      "`roles/bigquery.jobUser` on the project that runs the jobs",
      "`roles/bigquery.dataEditor` on the staging and final datasets",
      move
        ? "`roles/storage.objectUser` on the landing bucket (read, list and delete the processed files)"
        : "`roles/storage.objectViewer` on the landing bucket",
      "`roles/storage.objectUser` on the archive bucket (create / overwrite archived files)",
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
        "Idempotent: the staging load replaces the run's partition, and the MERGE updates rows that already exist, so re-running a date gives the same result.",
        "Files are archived only after the MERGE succeeds. Once archived, re-running the date finds no files: move them back from the archive folder to reprocess.",
        "The staging table keeps one partition per run date. Set a partition expiration on it (bq update --time_partitioning_expiration) to limit storage.",
        "Each run replaces its day's staging partition, so keep max_active_runs at 1 for schedules more frequent than daily.",
        withEmail
          ? "Failure alert: notify_pipeline_failure emails the alert emails via Composer's email backend (SMTP / SendGrid must be configured), then fails so the run shows as failed."
          : "Failure alert: notify_pipeline_failure logs the failed tasks and fails so the run shows as failed. Extend alert_pipeline_failure() to post to Slack / Google Chat / PagerDuty.",
        "Labels (env, team, owner, cost_center, dag_id) are set on every BigQuery job for cost tracking.",
        ...(bool(p, "deferrable")
          ? ["The sensor and BigQuery jobs run in deferrable mode: make sure the Composer environment has a triggerer (Composer 2: set triggerer count >= 1)."]
          : ["The sensor runs in reschedule mode, so it frees its worker slot between checks."]),
      ],
    });
  },
};
