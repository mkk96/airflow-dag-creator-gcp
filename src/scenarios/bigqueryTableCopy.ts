/**
 * BigQuery → BigQuery table copy.
 *
 * BigQueryInsertJobOperator with a `copy` job configuration (providers-google
 * 12.x). BigQueryToBigQueryOperator is not used: it has no deferrable mode in
 * 12.0.0. Here:
 *   - `configuration` and `project_id` are templated, so source and
 *     destination projects/datasets resolve per env at run time (cross-project
 *     and cross-dataset copies);
 *   - `configuration.labels` labels the copy job; deferrable=True is supported;
 *   - partition copies (table$YYYYMMDD → table$YYYYMMDD) with WRITE_TRUNCATE are
 *     idempotent: a re-run replaces only that partition.
 */
import { DATASET_LAYER_SETS, fillPattern, TEAM } from "../config/naming";
import { assign, pyCall, pyDict, pyList, pyName, pyStr, toSnakeIdentifier, type Py } from "../lib/python";
import * as v from "../validation/rules";
import { DagContext, list, str } from "./common";
import type { Params, Scenario } from "./types";

const isPartitionCopy = (p: Params) => p.partition_copy !== "NONE";

export const bigqueryTableCopy: Scenario = {
  id: "bigquery_table_copy",
  title: "BigQuery → BigQuery copy",
  description:
    "Copy BigQuery tables, or just the run's date partition, between datasets or projects with a copy job (no query cost).",
  naming: { source: TEAM.systemCodes.bigquery, target: TEAM.systemCodes.bigquery },
  sections: [
    { id: "source", title: "Source (BigQuery)" },
    { id: "destination", title: "Destination (BigQuery)" },
  ],
  commonDefaults: {
    dag_id: "reporting_bq_to_bq_daily",
    description: "Copy the daily orders partition into the reporting dataset.",
    domain: "reporting",
    tags: ["reporting", "bigquery", "copy"],
  },
  fields: [
    // Source -------------------------------------------------------------------------------
    {
      key: "source_project", label: "Source project (optional)", type: "text", section: "source", default: "",
      envSpecific: true, validate: (x) => v.bqProject(String(x)),
      help: "Env-specific. Project that owns the source tables. Empty = the job's project (Project id in GCP connection).",
    },
    {
      key: "source_dataset", label: "Source dataset", type: "text", section: "source", default: "stg_reporting",
      required: true, envSpecific: true, suggestable: true, validate: (x) => v.bqDataset(String(x)),
      help: "Env-specific.",
    },
    {
      key: "source_tables", label: "Source tables", type: "list", section: "source", default: ["orders"],
      required: true, validateItem: (x) => v.bqTable(x),
      help: "One per line, without $ decorators. Several tables are combined into the destination (their schemas must match).",
    },
    // Destination ---------------------------------------------------------------------------
    {
      key: "destination_project", label: "Destination project (optional)", type: "text", section: "destination",
      default: "", envSpecific: true, validate: (x) => v.bqProject(String(x)),
      help: "Env-specific. Project that owns the destination table. Empty = the job's project.",
    },
    {
      key: "destination_dataset", label: "Destination dataset", type: "text", section: "destination",
      default: "curated_reporting", required: true, envSpecific: true, suggestable: true,
      validate: (x) => v.bqDataset(String(x)), help: "Env-specific.",
    },
    {
      key: "destination_table", label: "Destination table", type: "text", section: "destination", default: "orders",
      required: true, validate: (x) => v.bqTable(String(x)), help: "Without a $ decorator; it is added from the option below.",
    },
    {
      key: "partition_copy", label: "What to copy", type: "select", section: "destination", default: "DAY",
      options: [
        { value: "DAY", label: "The run's daily partition: table$YYYYMMDD → table$YYYYMMDD" },
        { value: "MONTH", label: "The run's monthly partition: table$YYYYMM → table$YYYYMM" },
        { value: "NONE", label: "The whole table" },
      ],
      help: "Partition copies need source and destination tables with the same time partitioning.",
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
        { value: "CREATE_IF_NEEDED", label: "CREATE_IF_NEEDED — create the table if missing" },
        { value: "CREATE_NEVER", label: "CREATE_NEVER — the table must exist" },
      ],
    },
  ],

  samples: {
    daily_partition_copy: {
      dag_id: "reporting_bq_to_bq_daily",
      email: ["reporting-data@example.com"],
    },
    cross_project_tables_variables: {
      dag_id: "reporting_bq_to_bq_weekly",
      description: "Weekly full copy of the shared customer tables into the reporting project.",
      config_mode: "variables",
      env: "prd",
      project_id: "acme-prd-reporting",
      source_project: "acme-prd-data",
      source_dataset: "curated_sales",
      source_tables: ["customers_na", "customers_eu"],
      destination_project: "acme-prd-reporting",
      destination_dataset: "curated_reporting",
      destination_table: "customers",
      partition_copy: "NONE",
      schedule_preset: "@weekly",
      impersonation_chain: ["copy-runner@acme-prd-reporting.iam.gserviceaccount.com"],
    },
    monthly_partition_audit: {
      dag_id: "audit_bq_to_bq_monthly",
      description: "Archive the month's audit log partition into the audit archive dataset.",
      domain: "audit",
      source_dataset: "raw_audit",
      source_tables: ["access_log"],
      destination_dataset: "curated_audit",
      destination_table: "access_log_archive",
      partition_copy: "MONTH",
      create_disposition: "CREATE_NEVER",
      schedule_preset: "custom",
      schedule_cron: "0 4 2 * *",
      deferrable: false,
      tags: ["audit", "bigquery", "copy"],
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => {
      out[k] = out[k] ? v.merge(out[k], i) : i;
    };
    const wd = str(p, "write_disposition");
    if (wd === "WRITE_APPEND") {
      add("write_disposition", v.warn("Not idempotent: re-running a date appends the rows again. Prefer WRITE_TRUNCATE."));
    } else if (wd === "WRITE_EMPTY") {
      add("write_disposition", v.warn("Re-runs fail once the destination partition/table already has data."));
    }
    const srcProject = str(p, "source_project") || str(p, "project_id");
    const dstProject = str(p, "destination_project") || str(p, "project_id");
    if (
      srcProject === dstProject &&
      str(p, "source_dataset") === str(p, "destination_dataset") &&
      list(p, "source_tables").includes(str(p, "destination_table"))
    ) {
      add("destination_table", v.err("The destination is one of the source tables. Choose another table, dataset or project."));
    }
    if (new Set(list(p, "source_tables")).size !== list(p, "source_tables").length) {
      add("source_tables", v.err("Each source table may be listed only once."));
    }
    return out;
  },

  suggest(ctx) {
    const layers = DATASET_LAYER_SETS[ctx.layerSet as keyof typeof DATASET_LAYER_SETS] ?? DATASET_LAYER_SETS.classic;
    const out: Partial<Params> = {};
    const source = fillPattern("bqDataset", { layer: layers[1], domain: ctx.domain });
    const dest = fillPattern("bqDataset", { layer: layers[2], domain: ctx.domain });
    if (source) out.source_dataset = source;
    if (dest) out.destination_dataset = dest;
    return out;
  },

  generate(p) {
    const ctx = new DagContext(bigqueryTableCopy, p);
    ctx.importFrom("airflow.providers.google.cloud.operators.bigquery", "BigQueryInsertJobOperator");

    const partition = str(p, "partition_copy");
    const decorator =
      partition === "DAY" ? "${{ ds_nodash }}" : partition === "MONTH" ? "${{ logical_date.strftime('%Y%m') }}" : "";
    const sources = list(p, "source_tables");
    const destTable = str(p, "destination_table");
    const taskId = `copy_${toSnakeIdentifier(destTable, "table")}`;
    // Optional projects fall back to the job's project (both are templated values).
    const projectRef = (key: string) => ctx.ref(str(p, key) ? key : "project_id");
    const tableRef = (projectKey: string, datasetKey: string, table: string): Py =>
      pyDict(
        [
          ["projectId", pyStr(projectRef(projectKey))],
          ["datasetId", pyStr(ctx.ref(datasetKey))],
          ["tableId", pyStr(table + decorator)],
        ],
        { explode: true },
      );

    const copy: Array<[string, Py]> = [
      ["sourceTables", pyList(sources.map((t) => tableRef("source_project", "source_dataset", t)), { explode: true })],
      ["destinationTable", tableRef("destination_project", "destination_dataset", destTable)],
      ["writeDisposition", pyStr(str(p, "write_disposition"))],
      ["createDisposition", pyStr(str(p, "create_disposition"))],
    ];

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
              ["copy", pyDict(copy, { explode: true })],
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

    // --- docs --------------------------------------------------------------------------------
    const srcProject = str(p, "source_project") ? "<source_project>" : "<project_id>";
    const dstProject = str(p, "destination_project") ? "<destination_project>" : "<project_id>";
    const docSteps = [
      `\`${taskId}\`: copy ${sources.map((t) => `\`${srcProject}.<source_dataset>.${t}${decorator}\``).join(", ")} ` +
        `to \`${dstProject}.<destination_dataset>.${destTable}${decorator}\` with ${str(p, "write_disposition")}.`,
    ];

    const iamRoles = [
      "`roles/bigquery.jobUser` on the project that runs the jobs (Project id)",
      "`roles/bigquery.dataViewer` on the source dataset",
      "`roles/bigquery.dataEditor` on the destination dataset",
    ];
    if (list(p, "impersonation_chain").length) {
      iamRoles.push("`roles/iam.serviceAccountTokenCreator` for the Composer service account on the impersonated service account");
    }

    return ctx.assemble({
      docSteps,
      body: [assign(taskId, task, 1)],
      iamRoles,
      readmeNotes: [
        isPartitionCopy(p)
          ? "The copy job writes only the run's partition; with WRITE_TRUNCATE a re-run replaces that partition. Source and destination must use the same time partitioning."
          : "The copy job replaces the whole destination table on every run (with WRITE_TRUNCATE).",
        "Copy jobs are free (no bytes scanned); storage of the copy is billed in the destination project.",
        `Source and destination projects/datasets are env-specific (${ctx.mode === "variables" ? "Airflow Variable" : "ENV_CONFIG"}). Keep both datasets in the same location; cross-region copies have extra limits and data transfer charges.`,
        "Labels (env, team, owner, cost_center, dag_id) are set on every BigQuery job for cost tracking.",
        ...(p.deferrable === true
          ? ["Operators run in deferrable mode: make sure the Composer environment has a triggerer (Composer 2: set triggerer count >= 1)."]
          : []),
      ],
    });
  },
};
