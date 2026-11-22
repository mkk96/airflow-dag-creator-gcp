/**
 * BigQuery data quality checks.
 *
 * A suite of checks on one table, grouped in a TaskGroup and run in parallel:
 *   - BigQueryCheckOperator: row count, not-null, uniqueness, freshness and an
 *     optional custom SQL check (the single result row must be all TRUE);
 *   - BigQueryValueCheckOperator: a metric compared with an expected value,
 *     with an optional tolerance.
 * Every check's SQL lives in its own .sql file (template_ext ".sql" of both
 * operators loads it via template_searchpath).
 *
 * providers-google 12.0.0: `labels` is honoured when the check runs on the
 * worker (passed to BigQueryHook), but the deferrable code path submits the
 * query job without labels. Deferrable mode is therefore off by default here
 * (checks are short queries) and turning it on shows a warning.
 */
import { DATASET_LAYER_SETS, fillPattern, TEAM } from "../config/naming";
import {
  assign,
  pyBool,
  pyCall,
  pyDict,
  pyFromJson,
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
import { bool, DagContext, list, num, str } from "./common";
import type { Params, Scenario } from "./types";

type CheckKind = "row_count" | "not_null" | "unique" | "freshness" | "value" | "custom";

const CHECK_OPTIONS: Array<{ value: CheckKind; label: string }> = [
  { value: "row_count", label: "Row count — at least N rows" },
  { value: "not_null", label: "Not null — columns have no NULLs" },
  { value: "unique", label: "Uniqueness — key columns have no duplicates" },
  { value: "freshness", label: "Freshness — newest timestamp is recent enough" },
  { value: "value", label: "Metric value — equals an expected value (± tolerance)" },
  { value: "custom", label: "Custom SQL check" },
];

const has = (p: Params, kind: CheckKind) => Array.isArray(p.checks) && (p.checks as string[]).includes(kind);
const isRunFiltered = (p: Params) => p.run_filter === "run_date";
const isNumeric = (s: string) => /^-?\d+(\.\d+)?$/.test(s.trim());

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

const DEFAULT_CUSTOM_SQL = `-- Every column of the single result row must be TRUE for the check to pass.
SELECT
    COUNTIF(amount < 0) = 0 AS no_negative_amounts,
    COUNTIF(order_date > CURRENT_DATE()) = 0 AS no_future_orders
FROM \`{{ cfg.project_id }}.{{ cfg.dataset }}.{{ params.table }}\`
WHERE order_date = DATE('{{ ds }}')
`;

export const bigqueryDqChecks: Scenario = {
  id: "bigquery_dq_checks",
  title: "BigQuery data quality checks",
  description:
    "Run a suite of data quality checks on a BigQuery table (row count, not-null, uniqueness, freshness, metric value, custom SQL) and fail the DAG when one fails.",
  naming: { source: TEAM.systemCodes.bigquery, target: TEAM.systemCodes.bigquery },
  sections: [
    { id: "target", title: "Table to check" },
    { id: "checks", title: "Checks" },
  ],
  commonDefaults: {
    dag_id: "quality_bq_to_bq_daily",
    description: "Daily data quality checks on the orders table.",
    domain: "quality",
    tags: ["quality", "bigquery", "data-quality"],
    deferrable: false,
  },
  fields: [
    // Target ----------------------------------------------------------------------------
    {
      key: "dataset", label: "Dataset", type: "text", section: "target", default: "curated_sales", required: true,
      envSpecific: true, suggestable: true, validate: (x) => v.bqDataset(String(x)),
      help: "Env-specific. Dataset of the table to check (in the job's project); {{ cfg.dataset }} in custom SQL.",
    },
    {
      key: "table", label: "Table", type: "text", section: "target", default: "orders", required: true,
      validate: (x) => v.bqTable(String(x)), help: "Table to check; {{ params.table }} in custom SQL.",
    },
    {
      key: "run_filter", label: "Rows to check", type: "select", section: "target", default: "run_date",
      options: [
        { value: "run_date", label: "Only the run date's rows (partition filter, cheaper)" },
        { value: "none", label: "The whole table" },
      ],
      help: "Applies to the generated checks. Filtering on the partition column keeps scans small and makes re-runs check the same data.",
    },
    {
      key: "date_column", label: "Date column (optional)", type: "text", section: "target", default: "order_date",
      showIf: isRunFiltered, validate: (x) => v.bqColumn(String(x)),
      help: "DATE or TIMESTAMP column compared with the run date ({{ ds }}). Empty = ingestion-time partitioning (_PARTITIONTIME).",
    },
    {
      key: "wait_for_data", label: "Wait for the data first", type: "boolean", section: "target", default: true,
      help: (p) =>
        isRunFiltered(p)
          ? "Start the checks only once the run date's partition (table$YYYYMMDD) exists. Requires a table partitioned by day."
          : "Start the checks only once the table exists.",
    },
    {
      key: "wait_timeout_minutes", label: "Wait timeout (minutes)", type: "number", section: "target", default: 120,
      min: 1, max: 1440, showIf: (p) => p.wait_for_data === true,
      help: "The wait task fails if the data hasn't appeared by then.",
    },
    // Checks ------------------------------------------------------------------------------
    {
      key: "checks", label: "Checks to run", type: "multiselect", section: "checks",
      default: ["row_count", "not_null", "unique"], options: CHECK_OPTIONS, required: true,
      help: "Each check is its own task (and .sql file); they run in parallel and the DAG run fails if any check fails.",
    },
    {
      key: "min_row_count", label: "Minimum row count", type: "number", section: "checks", default: 1, min: 1,
      max: 2_000_000_000, showIf: (p) => has(p, "row_count"),
    },
    {
      key: "not_null_columns", label: "Not-null columns", type: "list", section: "checks",
      default: ["order_id", "customer_id"], required: true, showIf: (p) => has(p, "not_null"),
      validateItem: (x) => v.bqColumn(x), help: "One column per line; the check fails if any of them has a NULL.",
    },
    {
      key: "unique_columns", label: "Unique key columns", type: "list", section: "checks", default: ["order_id"],
      required: true, showIf: (p) => has(p, "unique"), validateItem: (x) => v.bqColumn(x),
      help: "One column per line; together they must identify a row (composite key). The check fails on any duplicate.",
    },
    {
      key: "freshness_column", label: "Freshness column", type: "text", section: "checks", default: "updated_at",
      required: true, showIf: (p) => has(p, "freshness"), validate: (x) => v.bqColumn(String(x)),
      help: "TIMESTAMP column with the time each row was created or updated.",
    },
    {
      key: "freshness_max_hours", label: "Maximum age (hours)", type: "number", section: "checks", default: 26,
      min: 1, max: 8760, showIf: (p) => has(p, "freshness"),
      help: "Fails when the newest value is more than this many hours before the end of the run date (midnight UTC after {{ ds }}). Measured against the run date, not the wall clock, so re-runs give the same result.",
    },
    {
      key: "value_metric", label: "Metric (SQL expression)", type: "text", section: "checks", default: "COUNT(*)",
      required: true, showIf: (p) => has(p, "value"),
      validate: (x) => (/;/.test(String(x)) ? v.err("Enter one SQL expression, without ';'.") : v.ok()),
      help: "Aggregate computed over the checked rows, e.g. COUNT(*), SUM(amount), COUNT(DISTINCT currency).",
    },
    {
      key: "value_pass_value", label: "Expected value", type: "text", section: "checks", default: "10000",
      required: true, showIf: (p) => has(p, "value"),
      help: "Number (compared with the tolerance below) or exact text.",
    },
    {
      key: "value_tolerance_pct", label: "Tolerance (%)", type: "number", section: "checks", default: 20, min: 0,
      max: 100, showIf: (p) => has(p, "value"),
      help: "Allowed deviation from a numeric expected value, e.g. 20 = ±20%. 0 = must match exactly.",
    },
    {
      key: "custom_check_sql", label: "Custom check SQL", type: "code", language: "sql", accept: ".sql",
      section: "checks", default: DEFAULT_CUSTOM_SQL, required: true, allowJinja: true,
      showIf: (p) => has(p, "custom"),
      help: "One SELECT returning one row; every value must be TRUE (or non-zero). Saved as its own .sql file. Use {{ cfg.project_id }}, {{ cfg.dataset }}, {{ params.table }} and {{ ds }}.",
    },
  ],

  samples: {
    daily_core_checks: {
      dag_id: "quality_bq_to_bq_daily",
      email: ["data-quality@example.com"],
    },
    full_suite_variables: {
      dag_id: "audit_bq_to_bq_daily",
      description: "Full data quality suite on the payments table, after the morning load.",
      domain: "audit",
      config_mode: "variables",
      env: "prd",
      project_id: "acme-prd-data",
      dataset: "curated_audit",
      table: "payments",
      date_column: "",
      wait_for_data: false,
      checks: ["row_count", "not_null", "unique", "freshness", "value", "custom"],
      min_row_count: 100,
      not_null_columns: ["payment_id", "order_id", "amount"],
      unique_columns: ["payment_id", "order_id"],
      freshness_column: "updated_at",
      freshness_max_hours: 30,
      value_metric: "COUNT(DISTINCT currency)",
      value_pass_value: "3",
      value_tolerance_pct: 0,
      custom_check_sql: `-- Every column of the single result row must be TRUE for the check to pass.
SELECT
    COUNTIF(amount <= 0) = 0 AS amounts_positive,
    COUNTIF(currency NOT IN ('CAD', 'USD', 'EUR')) = 0 AS known_currencies
FROM \`{{ cfg.project_id }}.{{ cfg.dataset }}.{{ params.table }}\`
WHERE DATE(_PARTITIONTIME) = DATE('{{ ds }}')
`,
      schedule_preset: "custom",
      schedule_cron: "0 7 * * *",
      impersonation_chain: ["dq-runner@acme-prd-data.iam.gserviceaccount.com"],
      tags: ["audit", "bigquery", "data-quality"],
    },
    weekly_whole_table: {
      dag_id: "quality_bq_to_bq_weekly",
      description: "Weekly uniqueness check on the customer dimension.",
      dataset: "curated_sales",
      table: "dim_customer",
      run_filter: "none",
      checks: ["unique"],
      unique_columns: ["customer_id"],
      schedule_preset: "@weekly",
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => {
      out[k] = out[k] ? v.merge(out[k], i) : i;
    };
    if (bool(p, "deferrable")) {
      add("deferrable", v.warn("providers-google 12.0.0: deferrable BigQuery check operators submit their job without labels. Turn this off to keep cost labels on the checks."));
    }
    if (has(p, "value")) {
      const pass = str(p, "value_pass_value");
      if (pass && !isNumeric(pass) && num(p, "value_tolerance_pct") > 0) {
        add("value_tolerance_pct", v.warn("The tolerance only applies to numeric expected values; text is compared exactly."));
      }
    }
    if (has(p, "custom")) {
      const sql = String(p.custom_check_sql ?? "");
      const keyword = firstSqlKeyword(sql);
      if (keyword !== "SELECT" && keyword !== "WITH") {
        add("custom_check_sql", v.err("Enter a SELECT (or WITH … SELECT) that returns one row."));
      } else if (sqlStatementCount(sql) > 1) {
        add("custom_check_sql", v.err("Only a single SELECT statement is allowed."));
      }
      add("custom_check_sql", sqlRefIssues(sql, bigqueryDqChecks, p, ["table"]));
    }
    return out;
  },

  suggest(ctx) {
    const layers = DATASET_LAYER_SETS[ctx.layerSet as keyof typeof DATASET_LAYER_SETS] ?? DATASET_LAYER_SETS.classic;
    const out: Partial<Params> = {};
    const dataset = fillPattern("bqDataset", { layer: layers[2], domain: ctx.domain });
    if (dataset) out.dataset = dataset;
    return out;
  },

  generate(p) {
    const ctx = new DagContext(bigqueryDqChecks, p);
    const table = str(p, "table");
    const obj = toSnakeIdentifier(table, "table");
    const fromLine = `FROM \`${ctx.sqlRef("project_id")}.${ctx.sqlRef("dataset")}.{{ params.table }}\``;
    const dateCol = str(p, "date_column");
    const where = isRunFiltered(p)
      ? [`WHERE ${dateCol ? `DATE(${sqlColumn(dateCol)})` : "DATE(_PARTITIONTIME)"} = DATE('{{ ds }}')`]
      : [];
    const scope = isRunFiltered(p) ? "rows of the run date" : "the whole table";
    const selected = CHECK_OPTIONS.map((o) => o.value).filter((k) => has(p, k));
    const wait = bool(p, "wait_for_data");
    const grouped = selected.length > 1;
    // A `with TaskGroup` that is the only statement of `with DAG` trips ruff SIM117,
    // so without a wait task the group is created as a variable and passed to tasks.
    const explicitGroup = grouped && !wait;
    const groupVar = "data_quality";

    const tasks: Array<{ id: string; call: Py; doc: string }> = [];
    const common = (): Array<[string, Py | null]> => [
      ["use_legacy_sql", pyBool(false)],
      ["location", pyName("LOCATION")],
      ["labels", pyName("LABELS")],
      ["gcp_conn_id", pyName("GCP_CONN_ID")],
      ["impersonation_chain", ctx.impersonationChain()],
      ["deferrable", ctx.deferrable()],
      ["task_group", explicitGroup ? pyName(groupVar) : null],
    ];
    const checkTask = (id: string, purpose: string, sqlLines: string[], doc: string) => {
      ctx.importFrom("airflow.providers.google.cloud.operators.bigquery", "BigQueryCheckOperator");
      const sqlName = ctx.addSql(id, [...ctx.sqlHeader(purpose), ...sqlLines].join("\n"));
      tasks.push({
        id,
        doc,
        call: pyCall(
          "BigQueryCheckOperator",
          [],
          [
            ["task_id", pyStr(id)],
            ["sql", pyStr(sqlName)],
            ["params", pyDict([["table", pyStr(table)]])],
            ...common(),
          ],
          { explode: true },
        ),
      });
    };

    for (const kind of selected) {
      switch (kind) {
        case "row_count": {
          const min = num(p, "min_row_count");
          checkTask(
            `check_${obj}_row_count`,
            `Data quality: at least ${min} row(s) in ${scope}.`,
            [`SELECT COUNT(*) >= ${min} AS has_min_rows`, fromLine, ...where],
            `at least ${min} row(s) in ${scope}.`,
          );
          break;
        }
        case "not_null": {
          const cols = list(p, "not_null_columns");
          checkTask(
            `check_${obj}_not_null`,
            `Data quality: no NULLs in ${cols.join(", ")} (${scope}).`,
            [
              "SELECT",
              cols.map((c) => `    COUNTIF(${sqlColumn(c)} IS NULL) = 0 AS ${c}_not_null`).join(",\n"),
              fromLine,
              ...where,
            ],
            `no NULLs in ${cols.map((c) => `\`${c}\``).join(", ")}.`,
          );
          break;
        }
        case "unique": {
          const cols = list(p, "unique_columns").map(sqlColumn);
          checkTask(
            `check_${obj}_unique_keys`,
            `Data quality: no duplicate (${cols.join(", ")}) in ${scope}.`,
            [
              "SELECT COUNT(*) = 0 AS keys_are_unique",
              "FROM (",
              ...(cols.length === 1 ? [`    SELECT ${cols[0]}`] : ["    SELECT", cols.map((c) => `        ${c}`).join(",\n")]),
              `    ${fromLine}`,
              ...where.map((w) => `    ${w}`),
              `    GROUP BY ${cols.join(", ")}`,
              "    HAVING COUNT(*) > 1",
              ") AS duplicate_keys",
            ],
            `no duplicate keys (${list(p, "unique_columns").map((c) => `\`${c}\``).join(", ")}).`,
          );
          break;
        }
        case "freshness": {
          const col = sqlColumn(str(p, "freshness_column"));
          const hours = num(p, "freshness_max_hours");
          checkTask(
            `check_${obj}_freshness`,
            `Data quality: newest ${str(p, "freshness_column")} at most ${hours}h before the end of the run date.`,
            [
              "SELECT",
              "    TIMESTAMP_DIFF(",
              "        TIMESTAMP_ADD(TIMESTAMP('{{ ds }}'), INTERVAL 1 DAY),",
              `        MAX(${col}),`,
              "        HOUR",
              `    ) <= ${hours} AS is_fresh`,
              fromLine,
              ...where,
            ],
            `newest \`${str(p, "freshness_column")}\` is at most ${hours} hours before the end of the run date.`,
          );
          break;
        }
        case "value": {
          ctx.importFrom("airflow.providers.google.cloud.operators.bigquery", "BigQueryValueCheckOperator");
          const id = `check_${obj}_metric_value`;
          const metric = str(p, "value_metric");
          const pass = str(p, "value_pass_value");
          const tolPct = num(p, "value_tolerance_pct");
          const numeric = isNumeric(pass);
          const tolerance = numeric && tolPct > 0 ? tolPct / 100 : null;
          const sqlName = ctx.addSql(
            id,
            [
              ...ctx.sqlHeader(`Data quality: metric value compared with ${pass}${tolerance ? ` (±${tolPct}%)` : ""}.`),
              `SELECT ${metric} AS metric_value`,
              fromLine,
              ...where,
            ].join("\n"),
          );
          tasks.push({
            id,
            doc: `\`${metric}\` = ${pass}${tolerance ? ` ±${tolPct}%` : ""}.`,
            call: pyCall(
              "BigQueryValueCheckOperator",
              [],
              [
                ["task_id", pyStr(id)],
                ["sql", pyStr(sqlName)],
                ["pass_value", numeric ? pyFromJson(Number(pass)) : pyStr(pass)],
                ["tolerance", tolerance === null ? null : pyFromJson(tolerance)],
                ["params", pyDict([["table", pyStr(table)]])],
                ...common(),
              ],
              { explode: true },
            ),
          });
          break;
        }
        case "custom": {
          checkTask(
            `check_${obj}_custom`,
            "Data quality: custom check (every value in the result row must be TRUE).",
            [cleanUserSql(String(p.custom_check_sql ?? ""))],
            "custom SQL check.",
          );
          break;
        }
      }
    }

    // --- optional wait for the data -------------------------------------------------------------
    // Sensors: project_id / dataset_id / table_id / partition_id are templated.
    const waitTaskId = isRunFiltered(p) ? `wait_for_${obj}_partition` : `wait_for_${obj}`;
    let waitTask: Py | null = null;
    if (wait) {
      const sensor = isRunFiltered(p) ? "BigQueryTablePartitionExistenceSensor" : "BigQueryTableExistenceSensor";
      ctx.importFrom("airflow.providers.google.cloud.sensors.bigquery", sensor);
      waitTask = pyCall(
        sensor,
        [],
        [
          ["task_id", pyStr(waitTaskId)],
          ["project_id", pyStr(ctx.ref("project_id"))],
          ["dataset_id", pyStr(ctx.ref("dataset"))],
          ["table_id", pyStr(table)],
          ["partition_id", isRunFiltered(p) ? pyStr("{{ ds_nodash }}") : null],
          ["gcp_conn_id", pyName("GCP_CONN_ID")],
          ["impersonation_chain", ctx.impersonationChain()],
          ...ctx.sensorKwargs(num(p, "wait_timeout_minutes"), 300),
        ],
        { explode: true },
      );
    }

    // --- DAG body: parallel checks, grouped when there is more than one ----------------------
    const body: string[] = [];
    if (waitTask) body.push(assign(waitTaskId, waitTask, 1), "");
    if (grouped) {
      ctx.importFrom("airflow.utils.task_group", "TaskGroup");
      const group = pyCall("TaskGroup", [], [
        ["group_id", pyStr(groupVar)],
        ["tooltip", pyStr(`Data quality checks on ${table}`)],
      ]);
      if (explicitGroup) {
        body.push(assign(groupVar, group, 1));
        tasks.forEach((t) => body.push("", assign(t.id, t.call, 1)));
      } else {
        body.push(withHeader(group, groupVar, 1));
        tasks.forEach((t, i) => {
          if (i) body.push("");
          body.push(assign(t.id, t.call, 2));
        });
        body.push("", dependencyChain([waitTaskId, groupVar]));
      }
    } else if (tasks.length === 1) {
      body.push(assign(tasks[0].id, tasks[0].call, 1));
      if (wait) body.push("", dependencyChain([waitTaskId, tasks[0].id]));
    }

    const iamRoles = [
      "`roles/bigquery.jobUser` on the project of the GCP connection (check jobs run there)",
      "`roles/bigquery.dataViewer` on the checked dataset",
    ];
    if (list(p, "impersonation_chain").length) {
      iamRoles.push("`roles/iam.serviceAccountTokenCreator` for the Composer service account on the impersonated service account");
    }

    return ctx.assemble({
      docSteps: [
        ...(wait
          ? [
              `\`${waitTaskId}\`: wait until ${isRunFiltered(p) ? "the run date's partition of " : ""}\`<project_id>.<dataset>.${table}\` exists (timeout ${num(p, "wait_timeout_minutes")} min).`,
            ]
          : []),
        ...tasks.map((t) => `\`${t.id}\`: ${t.doc}`),
      ],
      constants: [],
      body,
      iamRoles,
      readmeNotes: [
        `Checks run in parallel on ${scope} of \`<project_id>.<dataset>.${table}\`; the DAG run fails if any check fails. Re-running a date checks the same data.`,
        ...(wait
          ? [
              isRunFiltered(p)
                ? "The checks wait for the run date's partition (table$YYYYMMDD) to exist; the table must be partitioned by day."
                : "The checks wait for the table to exist.",
            ]
          : []),
        "Each check's SQL is its own file under `dags/sql/<dag_id>/`; edit thresholds there or regenerate.",
        "BigQueryCheckOperator fails when any value of the first result row is FALSE, 0, NULL or empty; BigQueryValueCheckOperator compares each value with the expected value (± tolerance).",
        "Check jobs run in the GCP connection's default project and are labelled (env, team, owner, cost_center, dag_id) for cost tracking.",
        ...(bool(p, "deferrable")
          ? [
              "Deferrable mode is on: in providers-google 12.0.0 deferrable check operators submit their job without labels. Make sure the Composer environment has a triggerer.",
            ]
          : []),
      ],
    });
  },
};
