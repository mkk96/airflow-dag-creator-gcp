/**
 * Dataform workflow: compile → invoke → wait.
 *
 * apache-airflow-providers-google 12.0.0 (google-cloud-dataform v1beta1 types):
 *   - DataformCreateCompilationResultOperator: template_fields = project_id, region,
 *     repository_id, compilation_result, impersonation_chain. Returns
 *     CompilationResult.to_dict() (XCom), whose "name" is the full resource name.
 *   - DataformCreateWorkflowInvocationOperator: template_fields = project_id, region,
 *     repository_id, workflow_invocation, impersonation_chain. asynchronous=True returns
 *     right after creating the invocation; asynchronous=False polls every `wait_time`
 *     seconds and raises on FAILED / CANCELLED.
 *   - DataformWorkflowInvocationStateSensor: template_fields = ("workflow_invocation_id",)
 *     ONLY — project_id and impersonation_chain are not templated and it is not deferrable.
 *     Env-specific values are Jinja, so the DAG defines a two-line subclass that adds
 *     project_id and impersonation_chain to template_fields, and the sensor always uses
 *     mode="reschedule" with a timeout.
 * Dataform's API takes no job labels; BigQuery labels are set in the Dataform project.
 */
import { TEAM } from "../config/naming";
import {
  assign,
  pyBool,
  pyCall,
  pyDict,
  pyIdent,
  pyInt,
  pyList,
  pyName,
  pyStr,
  pyTuple,
  withHeader,
  type Py,
} from "../lib/python";
import * as v from "../validation/rules";
import { bool, DagContext, list, num, str } from "./common";
import type { Params, Scenario } from "./types";

const GROUP_ID = "dataform";
const COMPILE_ID = "create_compilation_result";
const INVOKE_ID = "create_workflow_invocation";
const WAIT_ID = "wait_for_invocation";
const SENSOR_CLASS = "DataformInvocationStateSensor";

const isWorkspace = (p: Params) => p.compile_source === "workspace";
const isGit = (p: Params) => p.compile_source !== "workspace";
const isAsync = (p: Params) => p.run_mode !== "synchronous";
const scoped = (p: Params) => p.invocation_scope === "tags" || p.invocation_scope === "targets";

const STATES = ["SUCCEEDED", "FAILED", "CANCELLED"];
const STATE_OPTIONS = [
  { value: "SUCCEEDED", label: "SUCCEEDED" },
  { value: "FAILED", label: "FAILED" },
  { value: "CANCELLED", label: "CANCELLED" },
];

/** Resource-id segment (repository / workspace): letters, digits, '_' and '-'. */
function resourceId(value: string, what: string): v.Issues {
  if (!/^[A-Za-z0-9_-]{1,100}$/.test(value)) return v.err(`${what} may only contain letters, digits, '_' and '-'.`);
  return v.ok();
}

function gitRef(value: string): v.Issues {
  if (!/^[A-Za-z0-9._/-]+$/.test(value) || value.includes("..") || value.endsWith("/") || value.startsWith("-")) {
    return v.err("Enter a branch, tag or commit SHA (letters, digits, '.', '_', '-', '/').");
  }
  return v.ok();
}

function suffixToken(value: string): v.Issues {
  return /^[A-Za-z0-9_]*$/.test(value) ? v.ok() : v.err("Use letters, digits and '_' only.");
}

/** "key=value" lines (compilation vars). */
function parseVar(item: string): { key: string; value: string } | null {
  const i = item.indexOf("=");
  if (i <= 0) return null;
  return { key: item.slice(0, i).trim(), value: item.slice(i + 1) };
}

function varItem(item: string): v.Issues {
  const kv = parseVar(item);
  if (!kv) return v.err(`"${item}" must look like name=value.`);
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(kv.key)) return v.err(`"${kv.key}" is not a valid variable name.`);
  return v.ok();
}

/** "dataset.name" or "project.dataset.name". */
function parseTarget(item: string): { database: string | null; schema: string; name: string } | null {
  const parts = item.split(".");
  if (parts.length === 2) return { database: null, schema: parts[0], name: parts[1] };
  if (parts.length === 3) return { database: parts[0], schema: parts[1], name: parts[2] };
  return null;
}

function targetItem(item: string): v.Issues {
  const t = parseTarget(item);
  if (!t) return v.err(`"${item}" must be dataset.name or project.dataset.name.`);
  if (t.database !== null && v.bqProject(t.database).errors.length) return v.err(`"${t.database}" is not a valid project id.`);
  if (!/^[A-Za-z0-9_]+$/.test(t.schema)) return v.err(`"${t.schema}" is not a valid dataset name.`);
  if (!/^[A-Za-z0-9_-]+$/.test(t.name)) return v.err(`"${t.name}" is not a valid table / action name.`);
  if (t.database !== null) {
    return v.warn("A project in the target is the same in every env; prefer dataset.name (uses the default project).");
  }
  return v.ok();
}

/** `WorkflowInvocation.State.<X>` expression (trusted enum names only). */
const state = (s: string): Py => pyName(`WorkflowInvocation.State.${pyIdent(s)}`);

/**
 * Sensor kwargs for a sensor that cannot defer: ctx.sensorKwargs() without
 * `deferrable`, always mode="reschedule" plus poke_interval and timeout.
 */
function rescheduleSensorKwargs(ctx: DagContext, timeoutMinutes: number, pokeSeconds: number): Array<[string, Py]> {
  return [
    ["mode", pyStr("reschedule")],
    ...ctx.sensorKwargs(timeoutMinutes, pokeSeconds).filter(([k]) => k !== "deferrable" && k !== "mode"),
  ];
}

export const dataformWorkflow: Scenario = {
  id: "dataform_workflow",
  title: "Dataform workflow",
  description:
    "Compile a Dataform repository (from a branch, tag or workspace) and run its workflow — all actions, selected tags or selected tables — then wait for it to finish.",
  naming: { source: TEAM.systemCodes.dataform, target: TEAM.systemCodes.bigquery },
  sections: [
    {
      id: "repository",
      title: "Dataform repository",
      description: "The repository lives in the Project id and Location / region set under GCP connection.",
    },
    { id: "compilation", title: "Compilation overrides", description: "Optional. Empty values keep workflow_settings.yaml." },
    { id: "invocation", title: "What to run" },
    { id: "wait", title: "Waiting for the result" },
  ],
  commonDefaults: {
    dag_id: "warehouse_dfm_to_bq_daily",
    description: "Run the daily-tagged Dataform actions of the warehouse repository.",
    domain: "warehouse",
    location: "northamerica-northeast1",
    tags: ["warehouse", "dataform", "bigquery"],
  },
  fields: [
    // Repository -------------------------------------------------------------------------
    {
      key: "repository_id", label: "Repository id", type: "text", section: "repository", default: "warehouse",
      required: true, validate: (x) => resourceId(String(x), "Repository ids"),
      help: "Name of the Dataform repository (not the full resource path). The same name is used in every env.",
    },
    {
      key: "compile_source", label: "Compile from", type: "select", section: "repository", default: "git_commitish",
      options: [
        { value: "git_commitish", label: "Git branch, tag or commit" },
        { value: "workspace", label: "Development workspace (testing only)" },
      ],
    },
    {
      key: "git_commitish", label: "Branch, tag or commit SHA", type: "text", section: "repository", default: "main",
      required: true, showIf: isGit, validate: (x) => gitRef(String(x)),
      help: "Code that is compiled on every run, e.g. main, a release tag such as v1.4.0, or a commit SHA.",
    },
    {
      key: "workspace_id", label: "Workspace", type: "text", section: "repository", default: "dev-sandbox",
      required: true, showIf: isWorkspace, validate: (x) => resourceId(String(x), "Workspace names"),
      help: "Compiles the workspace's current (possibly uncommitted) files. Use for testing, not production.",
    },
    {
      key: "dataform_service_account", label: "Run as service account (optional)", type: "text",
      section: "repository", default: "", envSpecific: true, validate: (x) => v.serviceAccount(String(x)),
      help: "Service account that runs the BigQuery jobs of the workflow (invocation config). Empty = the repository's default. Env-specific.",
    },
    // Compilation overrides --------------------------------------------------------------
    {
      key: "dataform_default_project", label: "Default project override", type: "text", section: "compilation",
      default: "", envSpecific: true, validate: (x) => v.bqProject(String(x)),
      help: "Project where tables are created when they don't name one (defaultProject). Empty = workflow_settings.yaml. Env-specific.",
    },
    {
      key: "schema_suffix", label: "Dataset suffix", type: "text", section: "compilation", default: "",
      validate: (x) => suffixToken(String(x)),
      help: "Appended to every dataset name, e.g. _staging. Empty = none.",
    },
    {
      key: "table_prefix", label: "Table prefix", type: "text", section: "compilation", default: "",
      validate: (x) => suffixToken(String(x)),
      help: "Prepended to every table name. Empty = none.",
    },
    {
      key: "compilation_vars", label: "Compilation variables", type: "list", section: "compilation",
      default: ["run_date={{ ds }}"], allowJinja: true, validateItem: (x) => varItem(x),
      help: "One name=value per line, read in SQLX as dataform.projectConfig.vars.<name>. Values may use Airflow macros such as {{ ds }} to process the run's date.",
    },
    // Invocation -------------------------------------------------------------------------
    {
      key: "invocation_scope", label: "Run", type: "select", section: "invocation", default: "tags",
      options: [
        { value: "all", label: "All actions in the repository" },
        { value: "tags", label: "Actions with selected tags" },
        { value: "targets", label: "Selected tables / actions" },
      ],
    },
    {
      key: "included_tags", label: "Tags", type: "list", section: "invocation", default: ["daily"], required: true,
      showIf: (p) => p.invocation_scope === "tags",
      validateItem: (x) => (/^[A-Za-z0-9_.-]+$/.test(x) ? v.ok() : v.err(`"${x}" is not a valid tag.`)),
      help: "One tag per line, as set in the SQLX config blocks (tags: [...]).",
    },
    {
      key: "included_targets", label: "Tables / actions", type: "list", section: "invocation",
      default: ["curated_warehouse.daily_sales"], required: true, showIf: (p) => p.invocation_scope === "targets",
      validateItem: (x) => targetItem(x),
      help: "One per line as dataset.name (default project) or project.dataset.name, as compiled (including any dataset suffix / table prefix).",
    },
    {
      key: "transitive_dependencies", label: "Also run their dependencies", type: "boolean", section: "invocation",
      default: true, showIf: scoped, help: "Also runs every upstream action the selected ones depend on.",
    },
    {
      key: "transitive_dependents", label: "Also run their dependents", type: "boolean", section: "invocation",
      default: false, showIf: scoped, help: "Also runs every downstream action that depends on the selected ones.",
    },
    {
      key: "fully_refresh_incremental", label: "Fully refresh incremental tables", type: "boolean",
      section: "invocation", default: false,
      help: "Rebuilds incremental tables from scratch instead of adding new rows. Can be slow and costly.",
    },
    // Wait -------------------------------------------------------------------------------
    {
      key: "run_mode", label: "How to wait", type: "select", section: "wait", default: "async_sensor",
      options: [
        { value: "async_sensor", label: "Start the workflow, then wait with a sensor (frees the worker slot between checks)" },
        { value: "synchronous", label: "Start and wait inside one task (simpler; holds a worker slot)" },
      ],
      help: (p) =>
        isAsync(p)
          ? "Recommended for long workflows: the sensor uses mode=\"reschedule\"."
          : "The task polls until the workflow finishes and fails if it fails or is cancelled. It is limited by the task execution timeout.",
    },
    {
      key: "expected_statuses", label: "Success states", type: "multiselect", section: "wait", default: ["SUCCEEDED"],
      required: true, options: STATE_OPTIONS, showIf: isAsync,
      help: "The sensor succeeds when the workflow reaches one of these states.",
    },
    {
      key: "failure_statuses", label: "Failure states", type: "multiselect", section: "wait",
      default: ["FAILED", "CANCELLED"], options: STATE_OPTIONS, showIf: isAsync,
      help: "The sensor fails immediately when the workflow reaches one of these states.",
    },
    {
      key: "sensor_timeout_minutes", label: "Wait timeout (minutes)", type: "number", section: "wait", default: 120,
      min: 1, max: 10080, showIf: isAsync, help: "The sensor fails if the workflow has not finished within this time.",
    },
    {
      key: "poke_interval_seconds", label: "Check every (seconds)", type: "number", section: "wait", default: 60,
      min: 10, max: 3600,
    },
  ],

  samples: {
    tags_daily_async: {
      dag_id: "warehouse_dfm_to_bq_daily",
      email: ["warehouse-oncall@example.com"],
    },
    targets_variables_service_account: {
      dag_id: "payments_dfm_to_bq_hourly",
      description: "Refresh payment settlement tables and everything downstream of them.",
      domain: "payments",
      config_mode: "variables",
      env: "prd",
      project_id: "acme-prd-payments",
      impersonation_chain: ["dataform-trigger@acme-prd-payments.iam.gserviceaccount.com"],
      repository_id: "payments-models",
      git_commitish: "release/v2.3",
      dataform_service_account: "dataform-runner@acme-prd-payments.iam.gserviceaccount.com",
      dataform_default_project: "acme-prd-payments-curated",
      compilation_vars: ["run_hour={{ logical_date.strftime('%Y-%m-%dT%H') }}", "region=ca"],
      invocation_scope: "targets",
      included_targets: ["curated_payments.settlements", "curated_payments.refunds"],
      transitive_dependencies: false,
      transitive_dependents: true,
      expected_statuses: ["SUCCEEDED"],
      failure_statuses: ["FAILED", "CANCELLED"],
      sensor_timeout_minutes: 45,
      poke_interval_seconds: 30,
      schedule_preset: "@hourly",
      dagrun_timeout_minutes: 55,
      tags: ["payments", "dataform"],
    },
    workspace_full_refresh_sync: {
      dag_id: "warehouse_dfm_to_bq_adhoc",
      description: "Manual full rebuild of the warehouse models from the dev sandbox workspace.",
      compile_source: "workspace",
      workspace_id: "dev-sandbox",
      schema_suffix: "_sandbox",
      table_prefix: "tmp",
      compilation_vars: [],
      invocation_scope: "all",
      fully_refresh_incremental: true,
      run_mode: "synchronous",
      poke_interval_seconds: 30,
      schedule_preset: "none",
      execution_timeout_minutes: 240,
      dagrun_timeout_minutes: 300,
      retries: 0,
      tags: ["warehouse", "dataform", "manual"],
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => {
      out[k] = out[k] ? v.merge(out[k], i) : i;
    };
    if (/^(US|EU)$/.test(str(p, "location"))) {
      add("location", v.err('Dataform repositories live in a region (e.g. "northamerica-northeast1"), not a multi-region.'));
    }
    if (isWorkspace(p)) {
      add("compile_source", v.warn("Workspaces hold uncommitted development code; compile from a branch or tag in production."));
    }
    if (bool(p, "fully_refresh_incremental")) {
      add("fully_refresh_incremental", v.warn("Every run rebuilds incremental tables from scratch (slow, and scans all source data)."));
    }
    const vars = list(p, "compilation_vars").map((x) => parseVar(x)?.key ?? "");
    const dupes = vars.filter((k, i) => k && vars.indexOf(k) !== i);
    if (dupes.length) add("compilation_vars", v.err(`Duplicate variable(s): ${[...new Set(dupes)].join(", ")}.`));
    if (isAsync(p)) {
      const expected = list(p, "expected_statuses");
      const failure = list(p, "failure_statuses");
      const both = expected.filter((s) => failure.includes(s));
      if (both.length) add("failure_statuses", v.err(`${both.join(", ")} cannot be both a success and a failure state.`));
      if (!expected.includes("FAILED") && !failure.includes("FAILED")) {
        add("failure_statuses", v.warn("A failed workflow then keeps the sensor waiting until the timeout. Add FAILED."));
      }
      if (num(p, "sensor_timeout_minutes") > num(p, "dagrun_timeout_minutes")) {
        add("sensor_timeout_minutes", v.warn("Longer than the DAG run timeout: the run times out before the sensor does."));
      }
    }
    return out;
  },

  generate(p) {
    const ctx = new DagContext(dataformWorkflow, p);
    ctx.importFrom("airflow.providers.google.cloud.operators.dataform", "DataformCreateCompilationResultOperator");
    ctx.importFrom("airflow.providers.google.cloud.operators.dataform", "DataformCreateWorkflowInvocationOperator");
    ctx.importFrom("airflow.utils.task_group", "TaskGroup");

    const repo = str(p, "repository_id");
    const location = str(p, "location");
    const hasDefaultProject = str(p, "dataform_default_project") !== "";
    const hasServiceAccount = str(p, "dataform_service_account") !== "";
    const asyncMode = isAsync(p);
    const scope = str(p, "invocation_scope");

    // --- compilation result -------------------------------------------------------------
    const compilation: Array<[string, Py]> = [];
    if (isWorkspace(p)) {
      compilation.push([
        "workspace",
        pyStr(
          `projects/${ctx.ref("project_id")}/locations/${location}/repositories/${repo}/workspaces/${str(p, "workspace_id")}`,
        ),
      ]);
    } else {
      compilation.push(["git_commitish", pyStr(str(p, "git_commitish"))]);
    }
    const codeConfig: Array<[string, Py]> = [];
    if (hasDefaultProject) codeConfig.push(["default_database", pyStr(ctx.ref("dataform_default_project"))]);
    if (str(p, "schema_suffix")) codeConfig.push(["schema_suffix", pyStr(str(p, "schema_suffix"))]);
    if (str(p, "table_prefix")) codeConfig.push(["table_prefix", pyStr(str(p, "table_prefix"))]);
    const vars = list(p, "compilation_vars")
      .map(parseVar)
      .filter((x): x is { key: string; value: string } => x !== null);
    if (vars.length) codeConfig.push(["vars", pyDict(vars.map((x) => [x.key, pyStr(x.value)]), { explode: true })]);
    if (codeConfig.length) compilation.push(["code_compilation_config", pyDict(codeConfig, { explode: true })]);

    const common = (taskId: string): Array<[string, Py]> => [
      ["task_id", pyStr(taskId)],
      ["project_id", pyStr(ctx.ref("project_id"))],
      ["region", pyName("LOCATION")],
      ["repository_id", pyStr(repo)],
    ];
    const auth: Array<[string, Py]> = [
      ["gcp_conn_id", pyName("GCP_CONN_ID")],
      ["impersonation_chain", ctx.impersonationChain()],
    ];

    const compileTask = pyCall(
      "DataformCreateCompilationResultOperator",
      [],
      [...common(COMPILE_ID), ["compilation_result", pyDict(compilation, { explode: true })], ...auth],
      { explode: true },
    );

    // --- workflow invocation ---------------------------------------------------------------
    const invocationConfig: Array<[string, Py]> = [];
    if (scope === "tags") {
      invocationConfig.push(["included_tags", pyList(list(p, "included_tags").map(pyStr))]);
    } else if (scope === "targets") {
      const defaultDb = hasDefaultProject ? ctx.ref("dataform_default_project") : ctx.ref("project_id");
      const targets = list(p, "included_targets")
        .map(parseTarget)
        .filter((t): t is NonNullable<ReturnType<typeof parseTarget>> => t !== null)
        .map((t) =>
          pyDict([
            ["database", pyStr(t.database ?? defaultDb)],
            ["schema", pyStr(t.schema)],
            ["name", pyStr(t.name)],
          ]),
        );
      invocationConfig.push(["included_targets", pyList(targets, { explode: true })]);
    }
    if (scope !== "all") {
      invocationConfig.push(
        ["transitive_dependencies_included", pyBool(bool(p, "transitive_dependencies"))],
        ["transitive_dependents_included", pyBool(bool(p, "transitive_dependents"))],
      );
    }
    invocationConfig.push(["fully_refresh_incremental_tables_enabled", pyBool(bool(p, "fully_refresh_incremental"))]);
    if (hasServiceAccount) invocationConfig.push(["service_account", pyStr(ctx.ref("dataform_service_account"))]);

    const compileRef = `${GROUP_ID}.${COMPILE_ID}`;
    const invokeRef = `${GROUP_ID}.${INVOKE_ID}`;
    const invokeTask = pyCall(
      "DataformCreateWorkflowInvocationOperator",
      [],
      [
        ...common(INVOKE_ID),
        [
          "workflow_invocation",
          pyDict(
            [
              ["compilation_result", pyStr(`{{ task_instance.xcom_pull(task_ids='${compileRef}')['name'] }}`)],
              ["invocation_config", pyDict(invocationConfig, { explode: true })],
            ],
            { explode: true },
          ),
        ],
        ["asynchronous", pyBool(asyncMode)],
        ["wait_time", asyncMode ? null : pyInt(Math.max(10, Math.round(num(p, "poke_interval_seconds"))))],
        ...auth,
      ],
      { explode: true },
    );

    // --- sensor ------------------------------------------------------------------------------
    const constants: string[] = [];
    let waitTask: Py | null = null;
    if (asyncMode) {
      ctx.importFrom("airflow.providers.google.cloud.sensors.dataform", "DataformWorkflowInvocationStateSensor");
      ctx.importFrom("google.cloud.dataform_v1beta1", "WorkflowInvocation");
      constants.push(
        "",
        `class ${SENSOR_CLASS}(DataformWorkflowInvocationStateSensor):`,
        '    """State sensor that also templates project_id and impersonation_chain.',
        "",
        "    In apache-airflow-providers-google 12.0.0 only workflow_invocation_id is templated;",
        "    env-specific values in this DAG are Jinja, resolved at run time.",
        '    """',
        "",
        "    template_fields = (",
        "        *DataformWorkflowInvocationStateSensor.template_fields,",
        '        "project_id",',
        '        "impersonation_chain",',
        "    )",
      );
      const expected = STATES.filter((s) => list(p, "expected_statuses").includes(s));
      const failure = STATES.filter((s) => list(p, "failure_statuses").includes(s));
      waitTask = pyCall(
        SENSOR_CLASS,
        [],
        [
          ...common(WAIT_ID),
          [
            "workflow_invocation_id",
            pyStr(`{{ task_instance.xcom_pull(task_ids='${invokeRef}')['name'].split('/') | last }}`),
          ],
          ["expected_statuses", expected.length === 1 ? state(expected[0]) : pyTuple(expected.map(state))],
          ["failure_statuses", failure.length ? pyList(failure.map(state)) : null],
          ...auth,
          ...rescheduleSensorKwargs(ctx, num(p, "sensor_timeout_minutes"), num(p, "poke_interval_seconds")),
        ],
        { explode: true },
      );
    }

    // --- DAG body ------------------------------------------------------------------------------
    const body: string[] = [
      withHeader(
        pyCall("TaskGroup", [], [
          ["group_id", pyStr(GROUP_ID)],
          ["tooltip", pyStr("Compile the Dataform repository and run its workflow")],
        ]),
        null,
        1,
      ),
      assign(COMPILE_ID, compileTask, 2),
      "",
      assign(INVOKE_ID, invokeTask, 2),
      "",
    ];
    if (waitTask) {
      body.push(assign(WAIT_ID, waitTask, 2), "", `    ${COMPILE_ID} >> ${INVOKE_ID} >> ${WAIT_ID}`);
    } else {
      body.push(`    ${COMPILE_ID} >> ${INVOKE_ID}`);
    }

    // --- docs ------------------------------------------------------------------------------------
    const source = isWorkspace(p) ? `workspace \`${str(p, "workspace_id")}\`` : `\`${str(p, "git_commitish")}\``;
    const what =
      scope === "tags"
        ? `actions tagged ${list(p, "included_tags").map((t) => `\`${t}\``).join(", ")}`
        : scope === "targets"
          ? `${list(p, "included_targets").map((t) => `\`${t}\``).join(", ")}`
          : "all actions";
    const extras: string[] = [];
    if (scope !== "all" && bool(p, "transitive_dependencies")) extras.push("their dependencies");
    if (scope !== "all" && bool(p, "transitive_dependents")) extras.push("their dependents");
    const docSteps = [
      `\`${COMPILE_ID}\`: compile the Dataform repository \`${repo}\` (${location}) from ${source}` +
        (vars.length ? ` with vars ${vars.map((x) => `\`${x.key}\``).join(", ")}` : "") +
        ".",
      `\`${INVOKE_ID}\`: run ${what}${extras.length ? ` plus ${extras.join(" and ")}` : ""}` +
        (bool(p, "fully_refresh_incremental") ? ", fully refreshing incremental tables" : "") +
        (asyncMode ? " (returns right after starting the workflow)." : ", and wait until the workflow finishes."),
    ];
    if (asyncMode) {
      docSteps.push(
        `\`${WAIT_ID}\`: wait (reschedule mode, up to ${num(p, "sensor_timeout_minutes")} min) until the workflow is ${list(p, "expected_statuses").join(" / ")}` +
          (list(p, "failure_statuses").length ? `; fail on ${list(p, "failure_statuses").join(" / ")}.` : "."),
      );
    }

    const iamRoles = [
      "`roles/dataform.editor` on the repository (or project): create compilation results and workflow invocations and read their state",
      hasServiceAccount
        ? "`roles/iam.serviceAccountUser` on the run-as service account (`dataform_service_account`) for the identity that creates the invocation"
        : "The Dataform service agent (`service-<PROJECT_NUMBER>@gcp-sa-dataform.iam.gserviceaccount.com`) runs the workflow: grant it the BigQuery roles below",
      hasServiceAccount
        ? "The run-as service account needs `roles/bigquery.jobUser` on the project and `roles/bigquery.dataEditor` on the output datasets; the Dataform service agent needs `roles/iam.serviceAccountTokenCreator` on it"
        : "`roles/bigquery.jobUser` on the project and `roles/bigquery.dataEditor` on the output datasets (for the Dataform service agent)",
    ];
    if (list(p, "impersonation_chain").length) {
      iamRoles.push("`roles/iam.serviceAccountTokenCreator` for the Composer service account on the impersonated service account");
    }

    const notes = [
      "Re-running is safe when the Dataform actions are idempotent (tables are rebuilt; incremental tables use unique keys or the run's vars to replace their slice).",
      "Only the small compilation result and workflow invocation records pass through XCom (their resource names); no data does.",
      "Dataform's API takes no job labels: set BigQuery labels in the Dataform project (`bigquery: { labels: ... }` in SQLX config blocks) for cost tracking.",
    ];
    if (asyncMode) {
      notes.push(
        `\`DataformWorkflowInvocationStateSensor\` cannot defer and only templates \`workflow_invocation_id\` in providers-google 12.0.0, so the DAG uses a small subclass (\`${SENSOR_CLASS}\`) that also templates \`project_id\` and \`impersonation_chain\`; it runs in mode="reschedule".`,
        `To re-run a failed workflow, clear \`${INVOKE_ID}\` with its downstream tasks; re-running only the sensor re-checks the same invocation.`,
      );
    } else {
      notes.push(
        "Synchronous mode keeps a worker slot busy while the workflow runs; raise the task execution timeout for long workflows (it caps the wait).",
      );
    }

    return ctx.assemble({ docSteps, constants, body, iamRoles, readmeNotes: notes });
  },
};
