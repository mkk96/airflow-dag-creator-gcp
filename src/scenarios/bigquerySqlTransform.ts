/**
 * BigQuery SQL transformation.
 *
 * Runs the user's SQL with BigQueryInsertJobOperator and a `query` job
 * configuration (providers-google 12.x):
 *   - the SQL always lives in its own file (dags/sql/<dag_id>/NN_*.sql) and is
 *     referenced by name in `configuration.query.query`; Airflow loads it via
 *     template_searchpath because the string ends in `.sql` (a template_ext);
 *   - `configuration` and `project_id` are templated, so env-specific values
 *     resolve through Jinja at run time;
 *   - `configuration.labels` labels the BigQuery job; deferrable=True is supported.
 *
 * Two modes: a SELECT written to a destination table (partition decorator +
 * WRITE_TRUNCATE = idempotent), or a statement (MERGE / DML / DDL / script)
 * that writes its own target, so no destination table is set.
 *
 * This file also exports small SQL-text helpers reused by the other BigQuery
 * scenarios (export, copy, data quality).
 */
import { DATASET_LAYER_SETS, fillPattern, TEAM } from "../config/naming";
import {
  assign,
  LINE_LENGTH,
  pyBool,
  pyCall,
  pyDict,
  pyIdent,
  pyList,
  pyName,
  pyStr,
  toSnakeIdentifier,
  type Py,
} from "../lib/python";
import * as v from "../validation/rules";
import { allFields, isVisible, list, DagContext, str } from "./common";
import type { Params, Scenario } from "./types";

// ---------------------------------------------------------------------------
// SQL text helpers (heuristics for validation only; never used to build SQL)
// ---------------------------------------------------------------------------

/**
 * The SQL with comments, Jinja tags/comments and string literals removed, and
 * Jinja expressions replaced by a placeholder. Backtick identifiers are kept.
 */
export function sqlSkeleton(sql: string): string {
  const src = sql.replace(/\{#[\s\S]*?#\}/g, " ").replace(/\{%[\s\S]*?%\}/g, " ").replace(/\{\{[\s\S]*?\}\}/g, "x");
  let out = "";
  let i = 0;
  while (i < src.length) {
    const c = src[i];
    const two = src.slice(i, i + 2);
    if (two === "--" || c === "#") {
      while (i < src.length && src[i] !== "\n") i++;
    } else if (two === "/*") {
      const end = src.indexOf("*/", i + 2);
      i = end < 0 ? src.length : end + 2;
      out += " ";
    } else if (c === "'" || c === '"') {
      i++;
      while (i < src.length && src[i] !== c) i += src[i] === "\\" ? 2 : 1;
      i++;
      out += " '' ";
    } else {
      out += c;
      i++;
    }
  }
  return out;
}

/** First SQL keyword of the text (upper case), ignoring comments and Jinja tags. */
export function firstSqlKeyword(sql: string): string {
  const m = /[A-Za-z_]+/.exec(sqlSkeleton(sql).replace(/^[\s(]+/, ""));
  return m ? m[0].toUpperCase() : "";
}

/** Number of statements (semicolon-separated, a trailing semicolon is ignored). */
export function sqlStatementCount(sql: string): number {
  const parts = sqlSkeleton(sql)
    .split(";")
    .map((x) => x.trim());
  return parts.filter(Boolean).length;
}

/** User SQL normalised for a .sql file: LF line endings, trimmed, no trailing semicolon. */
export function cleanUserSql(sql: string, dropTrailingSemicolon = true): string {
  const s = sql.replace(/\r\n?/g, "\n").trim();
  return dropTrailingSemicolon ? s.replace(/;\s*$/, "") : s;
}

/**
 * Error when SQL uses `{{ cfg.<key> }}` for a key that is not an env-specific
 * field with a value, or `{{ params.<key> }}` for a param the task doesn't pass.
 */
export function sqlRefIssues(sql: string, scenario: Scenario, p: Params, allowedParams: string[]): v.Issues {
  const cfgKeys = allFields(scenario)
    .filter((f) => f.envSpecific && isVisible(f, p))
    .filter((f) => (Array.isArray(p[f.key]) ? list(p, f.key).length > 0 : str(p, f.key) !== ""))
    .map((f) => f.key);
  const errors: string[] = [];
  const unknownCfg = [...new Set([...sql.matchAll(/\bcfg\.([A-Za-z_]\w*)/g)].map((m) => m[1]))].filter(
    (k) => !cfgKeys.includes(k),
  );
  if (unknownCfg.length) {
    errors.push(
      `Unknown env value(s): ${unknownCfg.map((k) => `cfg.${k}`).join(", ")}. Available: ${cfgKeys.map((k) => `cfg.${k}`).join(", ")}.`,
    );
  }
  const unknownParams = [...new Set([...sql.matchAll(/\bparams\.([A-Za-z_]\w*)/g)].map((m) => m[1]))].filter(
    (k) => !allowedParams.includes(k),
  );
  if (unknownParams.length) {
    errors.push(
      `Unknown param(s): ${unknownParams.map((k) => `params.${k}`).join(", ")}. Available: ${allowedParams.map((k) => `params.${k}`).join(", ")}.`,
    );
  }
  return errors.length ? { errors, warnings: [] } : v.ok();
}

/**
 * `a >> b` dependency statement laid out like ruff format: one line when it fits
 * in 88 columns, otherwise parenthesised with one operand per line.
 */
export function dependencyChain(taskIds: string[], level = 1): string {
  const pad = "    ".repeat(level);
  const ids = taskIds.map(pyIdent);
  const one = `${pad}${ids.join(" >> ")}`;
  if (one.length <= LINE_LENGTH) return one;
  return [`${pad}(`, `${pad}    ${ids[0]}`, ...ids.slice(1).map((id) => `${pad}    >> ${id}`), `${pad})`].join("\n");
}

const DML_DDL = new Set([
  "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP", "ALTER", "TRUNCATE", "DECLARE", "BEGIN",
  "SET", "EXECUTE", "CALL", "EXPORT", "LOAD", "GRANT", "REVOKE", "IF", "FOR", "LOOP", "WHILE", "ASSERT",
]);

// ---------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------

const isDestination = (p: Params) => p.output_mode === "destination";
const isPartitioned = (p: Params) => isDestination(p) && p.partition_type !== "NONE";

const DEFAULT_SQL = `-- Daily revenue per customer for the run date ({{ ds }}).
-- Every row belongs to the run's partition, so WRITE_TRUNCATE into
-- table$YYYYMMDD replaces only that day on a re-run.
SELECT
    o.order_date,
    o.customer_id,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.amount) AS revenue
FROM \`{{ cfg.project_id }}.{{ cfg.source_dataset }}.orders\` AS o
WHERE o.order_date = DATE('{{ ds }}')
GROUP BY o.order_date, o.customer_id
`;

const MERGE_SAMPLE_SQL = `-- Upsert today's customer changes into the customer dimension.
-- MERGE is idempotent: re-running a date applies the same changes again.
MERGE \`{{ cfg.project_id }}.{{ cfg.dataset }}.{{ params.table }}\` AS t
USING (
    SELECT
        customer_id,
        customer_name,
        segment,
        updated_at
    FROM \`{{ cfg.project_id }}.{{ cfg.source_dataset }}.customer_changes\`
    WHERE DATE(updated_at) = DATE('{{ ds }}')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1
) AS s
    ON t.customer_id = s.customer_id
WHEN MATCHED THEN
    UPDATE SET
        customer_name = s.customer_name,
        segment = s.segment,
        updated_at = s.updated_at
WHEN NOT MATCHED THEN
    INSERT (customer_id, customer_name, segment, updated_at)
    VALUES (s.customer_id, s.customer_name, s.segment, s.updated_at)
`;

const MONTHLY_SQL = `-- Monthly revenue per product for the run's month.
SELECT
    o.product_id,
    DATE_TRUNC(o.order_date, MONTH) AS revenue_month,
    SUM(o.amount) AS revenue
FROM \`{{ cfg.project_id }}.{{ cfg.source_dataset }}.orders\` AS o
WHERE DATE_TRUNC(o.order_date, MONTH) = DATE_TRUNC(DATE('{{ ds }}'), MONTH)
GROUP BY o.product_id, revenue_month
`;

export const bigquerySqlTransform: Scenario = {
  id: "bigquery_sql_transform",
  title: "BigQuery SQL transformation",
  description:
    "Run a SQL transformation in BigQuery from a .sql file: write a SELECT into the run's date partition, or run a MERGE / DML / DDL statement.",
  naming: { source: TEAM.systemCodes.bigquery, target: TEAM.systemCodes.bigquery },
  sections: [
    { id: "sql", title: "SQL" },
    { id: "destination", title: "Datasets & destination (BigQuery)" },
  ],
  commonDefaults: {
    dag_id: "revenue_bq_to_bq_daily",
    description: "Build the daily customer revenue table from orders.",
    domain: "revenue",
    tags: ["revenue", "bigquery", "transform"],
  },
  fields: [
    // SQL -----------------------------------------------------------------------------
    {
      key: "output_mode", label: "What does the SQL do?", type: "select", section: "sql", default: "destination",
      options: [
        { value: "destination", label: "SELECT → write the result into a destination table (partition)" },
        { value: "statement", label: "Statement → MERGE / INSERT / DELETE / CREATE … writes its own target" },
      ],
      help: "Use a destination table for a plain SELECT. Use statement mode for MERGE, DML, DDL or multi-statement scripts; BigQuery does not allow a destination table for those.",
    },
    {
      key: "transform_sql", label: "SQL", type: "code", language: "sql", accept: ".sql", section: "sql",
      default: DEFAULT_SQL, required: true, allowJinja: true,
      help: (p) =>
        "Paste or upload your .sql file; it is saved as its own file next to the DAG. Use {{ cfg.project_id }}, {{ cfg.dataset }}, {{ cfg.source_dataset }} for env-specific values, {{ params.table }} for the table name and {{ ds }} / {{ ds_nodash }} for the run date. " +
        (isDestination(p)
          ? "Write a single SELECT; every row must fall in the run's partition."
          : "Make it idempotent: MERGE, or DELETE the run's date before INSERT."),
    },
    {
      key: "source_dataset", label: "Source dataset (optional)", type: "text", section: "destination",
      default: "stg_revenue", envSpecific: true, suggestable: true, validate: (x) => v.bqDataset(String(x)),
      help: "Env-specific. Dataset the SQL reads from; use it in the SQL as {{ cfg.source_dataset }}. Leave empty if the SQL doesn't need it.",
    },
    {
      key: "dataset", label: "Destination dataset", type: "text", section: "destination", default: "curated_revenue",
      required: true, envSpecific: true, suggestable: true, validate: (x) => v.bqDataset(String(x)),
      help: "Env-specific. Dataset of the table this job writes; available in the SQL as {{ cfg.dataset }}.",
    },
    {
      key: "table", label: "Destination table", type: "text", section: "destination", default: "daily_customer_revenue",
      required: true, validate: (x) => v.bqTable(String(x)),
      help: (p) =>
        isDestination(p)
          ? "Table the SELECT result is written to (no $ decorator; it is added from the partitioning option). Also {{ params.table }} in the SQL."
          : "Table the statement writes; used for the task id and available in the SQL as {{ params.table }}.",
    },
    {
      key: "partition_type", label: "Partitioning", type: "select", section: "destination", default: "DAY",
      showIf: isDestination,
      options: [
        { value: "DAY", label: "Daily — write into table$YYYYMMDD (idempotent)" },
        { value: "MONTH", label: "Monthly — write into table$YYYYMM (idempotent)" },
        { value: "NONE", label: "Not partitioned — replace the whole table" },
      ],
    },
    {
      key: "partition_field", label: "Partition column (optional)", type: "text", section: "destination",
      default: "order_date", showIf: isPartitioned, validate: (x) => v.bqColumn(String(x)),
      help: "DATE/TIMESTAMP column the table is partitioned on (used when the table is created). Empty = ingestion-time partitioning. All rows returned by a run must fall in that run's partition.",
    },
    {
      key: "cluster_fields", label: "Clustering columns", type: "list", section: "destination", default: ["customer_id"],
      showIf: isDestination, validateItem: (x) => v.bqColumn(x),
      help: "Up to 4, one per line. Applied when the table is created.",
    },
    {
      key: "write_disposition", label: "Write disposition", type: "select", section: "destination",
      default: "WRITE_TRUNCATE", showIf: isDestination,
      options: [
        { value: "WRITE_TRUNCATE", label: "WRITE_TRUNCATE — replace (partition or table)" },
        { value: "WRITE_APPEND", label: "WRITE_APPEND — append (not idempotent)" },
        { value: "WRITE_EMPTY", label: "WRITE_EMPTY — fail if data exists" },
      ],
    },
    {
      key: "create_disposition", label: "Create disposition", type: "select", section: "destination",
      default: "CREATE_IF_NEEDED", showIf: isDestination,
      options: [
        { value: "CREATE_IF_NEEDED", label: "CREATE_IF_NEEDED — create the table if missing" },
        { value: "CREATE_NEVER", label: "CREATE_NEVER — the table must exist" },
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
        isDestination(p) &&
        (p.write_disposition === "WRITE_APPEND" || (p.write_disposition === "WRITE_TRUNCATE" && isPartitioned(p))),
      help: "Lets the query add or relax columns when writing into an existing table.",
    },
  ],

  samples: {
    daily_partition_summary: {
      dag_id: "revenue_bq_to_bq_daily",
      email: ["revenue-data@example.com"],
    },
    merge_statement_variables: {
      dag_id: "analytics_bq_to_bq_daily",
      description: "Upsert daily customer changes into the customer dimension (MERGE).",
      domain: "analytics",
      config_mode: "variables",
      env: "prd",
      project_id: "acme-prd-data",
      output_mode: "statement",
      transform_sql: MERGE_SAMPLE_SQL,
      source_dataset: "stg_analytics",
      dataset: "curated_analytics",
      table: "dim_customer",
      impersonation_chain: ["etl-runner@acme-prd-data.iam.gserviceaccount.com"],
      schedule_preset: "custom",
      schedule_cron: "15 3 * * *",
      timezone: "America/Toronto",
      tags: ["analytics", "bigquery", "merge"],
    },
    monthly_partition: {
      dag_id: "revenue_bq_to_bq_monthly",
      description: "Monthly revenue per product.",
      transform_sql: MONTHLY_SQL,
      table: "monthly_product_revenue",
      partition_type: "MONTH",
      partition_field: "revenue_month",
      cluster_fields: ["product_id"],
      schedule_preset: "@monthly",
      deferrable: false,
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => {
      out[k] = out[k] ? v.merge(out[k], i) : i;
    };
    const sql = String(p.transform_sql ?? "");
    const keyword = firstSqlKeyword(sql);
    const statements = sqlStatementCount(sql);
    add("transform_sql", sqlRefIssues(sql, bigquerySqlTransform, p, ["table"]));
    if (isDestination(p)) {
      if (DML_DDL.has(keyword)) {
        add("transform_sql", v.err(`${keyword} statements can't write to a destination table. Choose "Statement" mode.`));
      } else if (statements > 1) {
        add("transform_sql", v.err('A destination table needs a single SELECT. Use "Statement" mode for scripts.'));
      }
      const wd = str(p, "write_disposition");
      if (wd === "WRITE_APPEND") {
        add("write_disposition", v.warn("Not idempotent: re-running a date appends duplicate rows. Prefer WRITE_TRUNCATE into a partition."));
      } else if (wd === "WRITE_EMPTY") {
        add("write_disposition", v.warn("Re-runs fail once the partition/table already has data."));
      } else if (!isPartitioned(p)) {
        add("write_disposition", v.warn("Not partitioned: every run replaces the whole table."));
      }
      if (isPartitioned(p) && !sql.includes("{{")) {
        add("transform_sql", v.warn("No run-date macro (e.g. {{ ds }}) in the SQL: every run writes the same rows into its own partition."));
      }
      if (list(p, "cluster_fields").length > 4) add("cluster_fields", v.err("BigQuery allows at most 4 clustering columns."));
    } else {
      if (keyword === "SELECT" || keyword === "WITH") {
        add("transform_sql", v.warn('A plain SELECT in "Statement" mode writes nothing. Choose a destination table, or use MERGE / INSERT.'));
      }
      const skeleton = sqlSkeleton(sql).toUpperCase();
      if (/\bINSERT\b/.test(skeleton) && !/\bMERGE\b|\bDELETE\b|\bTRUNCATE\b|\bCREATE\s+OR\s+REPLACE\b/.test(skeleton)) {
        add("transform_sql", v.warn("INSERT without DELETE/MERGE is not idempotent: re-running a date duplicates rows."));
      }
    }
    return out;
  },

  suggest(ctx) {
    const layers = DATASET_LAYER_SETS[ctx.layerSet as keyof typeof DATASET_LAYER_SETS] ?? DATASET_LAYER_SETS.classic;
    const out: Partial<Params> = {};
    const source = fillPattern("bqDataset", { layer: layers[1], domain: ctx.domain });
    const dest = fillPattern("bqDataset", { layer: layers[2], domain: ctx.domain });
    if (source) out.source_dataset = source;
    if (dest) out.dataset = dest;
    return out;
  },

  generate(p) {
    const ctx = new DagContext(bigquerySqlTransform, p);
    ctx.importFrom("airflow.providers.google.cloud.operators.bigquery", "BigQueryInsertJobOperator");

    const destination = isDestination(p);
    const table = str(p, "table");
    const obj = toSnakeIdentifier(table, "table");
    const partition = destination ? str(p, "partition_type") : "NONE";
    const partitionField = str(p, "partition_field");
    const decorator =
      partition === "DAY" ? "${{ ds_nodash }}" : partition === "MONTH" ? "${{ logical_date.strftime('%Y%m') }}" : "";
    const taskId = `transform_${obj}`;

    // --- SQL file ----------------------------------------------------------------------
    const sqlName = ctx.addSql(
      taskId,
      [
        ...ctx.sqlHeader(
          destination
            ? `Transformation: SELECT written to ${table}${partition === "NONE" ? "" : " (run partition)"} with ${str(p, "write_disposition")}.`
            : `Transformation: statement that writes ${table}.`,
        ),
        cleanUserSql(String(p.transform_sql ?? ""), destination),
      ].join("\n"),
    );

    // --- query configuration ----------------------------------------------------------
    const query: Array<[string, Py]> = [
      ["query", pyStr(sqlName)],
      ["useLegacySql", pyBool(false)],
    ];
    if (destination) {
      query.push(
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
        ["writeDisposition", pyStr(str(p, "write_disposition"))],
        ["createDisposition", pyStr(str(p, "create_disposition"))],
      );
      if (partition !== "NONE") {
        const tp: Array<[string, Py]> = [["type", pyStr(partition)]];
        if (partitionField) tp.push(["field", pyStr(partitionField)]);
        query.push(["timePartitioning", pyDict(tp)]);
      }
      const clusters = list(p, "cluster_fields");
      if (clusters.length) query.push(["clustering", pyDict([["fields", pyList(clusters.map(pyStr))]])]);
      const suo = list(p, "schema_update_options");
      const suoVisible =
        str(p, "write_disposition") === "WRITE_APPEND" || (str(p, "write_disposition") === "WRITE_TRUNCATE" && partition !== "NONE");
      if (suoVisible && suo.length) query.push(["schemaUpdateOptions", pyList(suo.map(pyStr))]);
    }

    const task = pyCall(
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
              ["query", pyDict(query, { explode: true })],
              ["labels", pyName("LABELS")],
            ],
            { explode: true },
          ),
        ],
        ["params", pyDict([["table", pyStr(table)]])],
        ["gcp_conn_id", pyName("GCP_CONN_ID")],
        ["impersonation_chain", ctx.impersonationChain()],
        ["deferrable", ctx.deferrable()],
      ],
      { explode: true },
    );

    // --- docs --------------------------------------------------------------------------
    const target = `\`<project_id>.<dataset>.${table}\``;
    const docSteps = [
      destination
        ? `\`${taskId}\`: run \`sql/${ctx.dagId}/${sqlName}\` and write the result into ${target}` +
          (partition === "NONE" ? "" : ` partition \`${decorator}\``) +
          ` with ${str(p, "write_disposition")}.`
        : `\`${taskId}\`: run the statement in \`sql/${ctx.dagId}/${sqlName}\` (writes ${target}).`,
    ];

    const iamRoles = [
      "`roles/bigquery.jobUser` on the project that runs the jobs",
      "`roles/bigquery.dataEditor` on the destination dataset",
      "`roles/bigquery.dataViewer` on every dataset the SQL reads",
    ];
    if (list(p, "impersonation_chain").length) {
      iamRoles.push("`roles/iam.serviceAccountTokenCreator` for the Composer service account on the impersonated service account");
    }

    return ctx.assemble({
      docSteps,
      body: [assign(taskId, task, 1)],
      iamRoles,
      readmeNotes: [
        destination
          ? partition === "NONE"
            ? "The query replaces the whole destination table on every run (WRITE_TRUNCATE)."
            : "The query writes into the run's partition decorator; with WRITE_TRUNCATE a re-run replaces that partition only. Every row it returns must belong to that partition."
          : "Statement mode: the SQL writes its own target. Keep it idempotent (MERGE, or DELETE the run's date before INSERT) so re-runs don't duplicate data.",
        `The SQL lives in \`dags/sql/${ctx.dagId}/${sqlName}\`; edit it there and re-upload. Jinja (\`{{ cfg.<key> }}\`, \`{{ ds }}\`) is rendered by Airflow at run time.`,
        "Labels (env, team, owner, cost_center, dag_id) are set on every BigQuery job for cost tracking.",
        ...(p.deferrable === true
          ? ["Operators run in deferrable mode: make sure the Composer environment has a triggerer (Composer 2: set triggerer count >= 1)."]
          : []),
      ],
    });
  },
};
