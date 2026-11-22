/**
 * Cloud Run job / Cloud Function trigger.
 *
 * apache-airflow-providers-google 12.0.0:
 *   - CloudRunExecuteJobOperator: template_fields = project_id, region, gcp_conn_id,
 *     impersonation_chain, job_name, overrides. Supports deferrable=True. Overrides map
 *     to run_v2 RunJobRequest.Overrides (container_overrides[].args/env/name,
 *     task_count, timeout as a "<n>s" duration string). Overrides take no labels.
 *   - CloudFunctionInvokeFunctionOperator: template_fields = function_id, input_data,
 *     location, project_id, impersonation_chain. Not deferrable. Calls the Cloud
 *     Functions v1 `functions.call` API, whose body is {"data": "<string>"} — so the
 *     JSON payload is sent as a string. That API only reaches 1st gen functions and
 *     is rate-limited.
 * Every env-specific value (project_id, impersonation_chain) only goes into templated fields.
 */
import { OFFICIAL, TEAM } from "../config/naming";
import {
  assign,
  comment,
  pyBool,
  pyCall,
  pyDict,
  pyInt,
  pyList,
  pyName,
  pyStr,
  toSnakeIdentifier,
  type Py,
} from "../lib/python";
import * as v from "../validation/rules";
import { bool, DagContext, list, num, str } from "./common";
import type { Params, Scenario } from "./types";

const isJob = (p: Params) => p.target_type !== "cloud_function";
const isFunction = (p: Params) => p.target_type === "cloud_function";

/** Cloud Run job / container names: lowercase letters, digits, hyphens; start with a letter. */
function cloudRunName(value: string, what: string): v.Issues {
  if (!value) return v.ok();
  if (value.length > OFFICIAL.resourceName.maxLength || !OFFICIAL.resourceName.re.test(value)) {
    return v.err(`${what} must be 1-63 lowercase letters, digits or hyphens, start with a letter and not end with a hyphen.`);
  }
  return v.ok();
}

/** Cloud Functions (1st gen) name: letters, digits, '_' or '-', starts with a letter, max 63. */
function functionName(value: string): v.Issues {
  if (!/^[A-Za-z][A-Za-z0-9_-]{0,62}$/.test(value)) {
    return v.err("Function names start with a letter and use letters, digits, '_' or '-' (max 63).");
  }
  return v.ok();
}

/** "NAME=value" lines (env var overrides). */
function parseEnvVar(item: string): { name: string; value: string } | null {
  const i = item.indexOf("=");
  if (i <= 0) return null;
  return { name: item.slice(0, i).trim(), value: item.slice(i + 1) };
}

/** Variables set by Cloud Run itself. */
const RESERVED_ENV = new Set([
  "PORT", "K_SERVICE", "K_REVISION", "K_CONFIGURATION", "CLOUD_RUN_JOB", "CLOUD_RUN_EXECUTION",
  "CLOUD_RUN_TASK_INDEX", "CLOUD_RUN_TASK_ATTEMPT", "CLOUD_RUN_TASK_COUNT",
]);

function envVarItem(item: string): v.Issues {
  const kv = parseEnvVar(item);
  if (!kv) return v.err(`"${item}" must look like NAME=value.`);
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(kv.name)) {
    return v.err(`"${kv.name}" is not a valid variable name (letters, digits, '_'; not starting with a digit).`);
  }
  if (RESERVED_ENV.has(kv.name)) return v.err(`"${kv.name}" is reserved by Cloud Run.`);
  if (v.noJinja(kv.name).errors.length) return v.err("Templates are only allowed in the value, not in the name.");
  return v.ok();
}

function parseJson(text: string): { ok: true; value: unknown } | { ok: false; error: string } {
  try {
    return { ok: true, value: JSON.parse(text) };
  } catch (e) {
    return { ok: false, error: (e as Error).message };
  }
}

const DEFAULT_FUNCTION_INPUT = JSON.stringify({ run_date: "{{ ds }}", source: "airflow" }, null, 2);

export const cloudRunFunction: Scenario = {
  id: "cloud_run_function",
  title: "Cloud Run job / Cloud Function",
  description:
    "Execute an existing Cloud Run job (optionally overriding its arguments, environment variables, task count and timeout) or invoke a Cloud Function with a JSON payload.",
  naming: { source: "composer", target: TEAM.systemCodes.cloudrun },
  sections: [
    { id: "target", title: "What to trigger" },
    { id: "run_job", title: "Cloud Run job", description: "The job must already be deployed in the project and region below." },
    { id: "function", title: "Cloud Function" },
  ],
  commonDefaults: {
    dag_id: "warehouse_composer_to_run_daily",
    description: "Run the warehouse export Cloud Run job for the run date.",
    domain: "warehouse",
    location: "northamerica-northeast1",
    tags: ["warehouse", "cloud-run"],
  },
  fields: [
    {
      key: "target_type", label: "Target", type: "select", section: "target", default: "cloud_run_job",
      options: [
        { value: "cloud_run_job", label: "Cloud Run job — execute and wait until it finishes" },
        { value: "cloud_function", label: "Cloud Function (1st gen) — invoke with a JSON payload" },
      ],
      help: (p) =>
        isFunction(p)
          ? "Uses the Cloud Functions v1 call API: 1st gen functions only, and only low traffic is allowed (a few calls per minute). For 2nd gen functions (Cloud Run functions) or heavy work, deploy a Cloud Run job instead."
          : "The task starts a new execution of the job and succeeds only when every task of that execution succeeds. The Project id and Location / region fields (GCP connection) say where the job lives.",
    },
    // Cloud Run job ----------------------------------------------------------------------
    {
      key: "job_name", label: "Job name", type: "text", section: "run_job", default: "warehouse-export",
      required: true, showIf: isJob, validate: (x) => cloudRunName(String(x), "Job names"),
      help: "Name of the deployed Cloud Run job (not the full resource path). The same name is used in every env; the project changes per env.",
    },
    {
      key: "container_name", label: "Container name (optional)", type: "text", section: "run_job", default: "",
      showIf: isJob, validate: (x) => cloudRunName(String(x), "Container names"),
      help: "Only needed when the job has more than one container: the container that receives the argument and variable overrides below.",
    },
    {
      key: "container_args", label: "Container arguments override", type: "list", section: "run_job",
      default: ["--run-date={{ ds }}"], allowJinja: true, showIf: isJob,
      help: "One argument per line; replaces the job's default arguments for this run. Airflow macros such as {{ ds }} are allowed. Leave empty to keep the job's own arguments.",
    },
    {
      key: "env_vars", label: "Environment variable overrides", type: "list", section: "run_job", default: [],
      allowJinja: true, showIf: isJob, validateItem: (x) => envVarItem(x),
      help: "One NAME=value per line, added to (or replacing) the job's variables for this run. Values may use macros, e.g. RUN_DATE={{ ds }}. Never put secrets here; use Secret Manager references on the job.",
    },
    {
      key: "task_count", label: "Task count override", type: "number", section: "run_job", default: 0,
      min: 0, max: 10000, showIf: isJob,
      help: "Number of parallel tasks for this execution. 0 keeps the job's configured task count.",
    },
    {
      key: "task_timeout_seconds", label: "Task timeout override (seconds)", type: "number", section: "run_job",
      default: 0, min: 0, max: 604800, showIf: isJob,
      help: "Maximum run time of each job task for this execution. 0 keeps the job's configured timeout.",
    },
    // Cloud Function ---------------------------------------------------------------------
    {
      key: "function_id", label: "Function name", type: "text", section: "function", default: "refresh-cache",
      required: true, showIf: isFunction, validate: (x) => functionName(String(x)),
      help: "Name of the deployed function (not the full resource path), in the Project id and Location / region set under GCP connection.",
    },
    {
      key: "function_input", label: "Input data (JSON)", type: "code", language: "json", accept: ".json",
      section: "function", default: DEFAULT_FUNCTION_INPUT, required: true, allowJinja: true, showIf: isFunction,
      validate: (x) => {
        const r = parseJson(String(x));
        return r.ok ? v.ok() : v.err(`Invalid JSON: ${r.error}`);
      },
      help: "JSON sent to the function as its request data (as a string, as the call API requires). Airflow macros such as {{ ds }} are allowed inside JSON strings. Keep it small.",
    },
  ],

  samples: {
    run_job_with_overrides: {
      dag_id: "warehouse_composer_to_run_daily",
      email: ["warehouse-oncall@example.com"],
      container_args: ["--run-date={{ ds }}", "--mode=incremental"],
      env_vars: ["RUN_DATE={{ ds }}", "LOG_LEVEL=INFO"],
      task_count: 4,
      task_timeout_seconds: 1800,
    },
    run_job_variables_impersonated: {
      dag_id: "partners_composer_to_run_hourly",
      description: "Hourly partner feed sync on Cloud Run (no overrides).",
      domain: "partners",
      config_mode: "variables",
      env: "stg",
      project_id: "acme-stg-partners",
      impersonation_chain: ["partner-sync@acme-stg-partners.iam.gserviceaccount.com"],
      job_name: "partner-feed-sync",
      container_args: [],
      schedule_preset: "@hourly",
      dagrun_timeout_minutes: 55,
      execution_timeout_minutes: 45,
      deferrable: false,
      tags: ["partners", "cloud-run"],
    },
    invoke_function: {
      dag_id: "partners_composer_to_run_daily",
      description: "Ask the partner API cache function to refresh after the nightly load.",
      domain: "partners",
      target_type: "cloud_function",
      function_id: "refresh-partner-cache",
      function_input: JSON.stringify(
        { run_date: "{{ ds }}", partners: ["north", "west"], full_refresh: false, note: "O'Brien \"quoted\"" },
        null,
        2,
      ),
      schedule_preset: "custom",
      schedule_cron: "15 5 * * *",
      timezone: "America/Toronto",
      tags: ["partners", "cloud-functions"],
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => {
      out[k] = out[k] ? v.merge(out[k], i) : i;
    };
    if (/^(US|EU)$/.test(str(p, "location"))) {
      add("location", v.err('Cloud Run and Cloud Functions need a region (e.g. "northamerica-northeast1"), not a multi-region.'));
    }
    if (isJob(p)) {
      const names = list(p, "env_vars").map((x) => parseEnvVar(x)?.name ?? "");
      const dupes = names.filter((n, i) => n && names.indexOf(n) !== i);
      if (dupes.length) add("env_vars", v.err(`Duplicate variable(s): ${[...new Set(dupes)].join(", ")}.`));
      const timeout = num(p, "task_timeout_seconds");
      if (bool(p, "deferrable") && timeout > num(p, "execution_timeout_minutes") * 60) {
        add(
          "task_timeout_seconds",
          v.warn("Longer than the task execution timeout: Airflow stops waiting (and fails the task) before the job can time out. Raise the execution timeout."),
        );
      }
      if (str(p, "container_name") && !list(p, "container_args").length && !list(p, "env_vars").length) {
        add("container_name", v.warn("Only used with argument or variable overrides."));
      }
    }
    return out;
  },

  generate(p) {
    const ctx = new DagContext(cloudRunFunction, p);
    const body: string[] = [];
    const docSteps: string[] = [];
    const iamRoles: string[] = [];
    const notes: string[] = [];
    const constants: string[] = [];

    if (isJob(p)) {
      ctx.importFrom("airflow.providers.google.cloud.operators.cloud_run", "CloudRunExecuteJobOperator");
      const job = str(p, "job_name");
      const taskId = `run_${toSnakeIdentifier(job, "job")}_job`;

      // --- overrides (RunJobRequest.Overrides) -----------------------------------------
      const args = list(p, "container_args");
      const envs = list(p, "env_vars")
        .map(parseEnvVar)
        .filter((x): x is { name: string; value: string } => x !== null);
      const container: Array<[string, Py]> = [];
      if (str(p, "container_name")) container.push(["name", pyStr(str(p, "container_name"))]);
      if (args.length) container.push(["args", pyList(args.map(pyStr))]);
      if (envs.length) {
        container.push([
          "env",
          pyList(envs.map((e) => pyDict([["name", pyStr(e.name)], ["value", pyStr(e.value)]]))),
        ]);
      }
      const overrides: Array<[string, Py]> = [];
      if (args.length || envs.length) {
        overrides.push(["container_overrides", pyList([pyDict(container, { explode: true })])]);
      }
      if (num(p, "task_count") > 0) overrides.push(["task_count", pyInt(num(p, "task_count"))]);
      if (num(p, "task_timeout_seconds") > 0) overrides.push(["timeout", pyStr(`${num(p, "task_timeout_seconds")}s`)]);

      body.push(
        assign(
          taskId,
          pyCall(
            "CloudRunExecuteJobOperator",
            [],
            [
              ["task_id", pyStr(taskId)],
              ["project_id", pyStr(ctx.ref("project_id"))],
              ["region", pyName("LOCATION")],
              ["job_name", pyStr(job)],
              ["overrides", overrides.length ? pyDict(overrides, { explode: true }) : null],
              ["gcp_conn_id", pyName("GCP_CONN_ID")],
              ["impersonation_chain", ctx.impersonationChain()],
              ["deferrable", ctx.deferrable()],
              ["do_xcom_push", pyBool(false)],
            ],
            { explode: true },
          ),
          1,
        ),
      );

      const what: string[] = [];
      if (args.length) what.push(`arguments ${args.map((a) => `\`${a}\``).join(" ")}`);
      if (envs.length) what.push(`variables ${envs.map((e) => `\`${e.name}\``).join(", ")}`);
      if (num(p, "task_count") > 0) what.push(`${num(p, "task_count")} tasks`);
      if (num(p, "task_timeout_seconds") > 0) what.push(`task timeout ${num(p, "task_timeout_seconds")}s`);
      docSteps.push(
        `\`${taskId}\`: execute the Cloud Run job \`${job}\` in \`${str(p, "location")}\`` +
          (what.length ? ` with overrides (${what.join("; ")})` : "") +
          " and wait until every task of the execution succeeds.",
      );
      iamRoles.push(
        overrides.length
          ? "`roles/run.developer` on the job (or project): executing with overrides needs `run.jobs.runWithOverrides`"
          : "`roles/run.invoker` (`run.jobs.run`) and `roles/run.viewer` (read the job and its operations) on the job (or project)",
        "The job's own runtime service account keeps its existing roles; Airflow does not need `iam.serviceAccountUser` on it to run the job",
      );
      notes.push(
        "Every run starts a new job execution; Cloud Run names executions uniquely, so re-runs never collide. The job itself must be idempotent for the run date it receives (e.g. overwrite a date partition).",
        "The job result is not pushed to XCom (`do_xcom_push=False`): it contains the full job definition, including plain environment variable values.",
        "Cloud Run job overrides do not accept labels; set labels on the job itself (`gcloud run jobs update --labels`) for cost tracking.",
      );
      if (bool(p, "deferrable")) {
        notes.push(
          "The operator runs in deferrable mode: make sure the Composer environment has a triggerer (Composer 2: set triggerer count >= 1). The task execution timeout also caps how long it waits for the job.",
        );
      }
    } else {
      ctx.importFrom("airflow.providers.google.cloud.operators.functions", "CloudFunctionInvokeFunctionOperator");
      const fn = str(p, "function_id");
      const taskId = `trigger_${toSnakeIdentifier(fn, "function")}_function`;
      const parsed = parseJson(String(p.function_input ?? "{}"));
      const payload = parsed.ok ? JSON.stringify(parsed.value) : "{}";
      constants.push(
        comment(
          "Request data for the function. The Cloud Functions call API expects a string, so the\n" +
            "JSON payload is serialized; macros such as {{ ds }} are rendered at run time.",
        ),
        assign("FUNCTION_INPUT", pyDict([["data", pyStr(payload)]])),
      );
      body.push(
        assign(
          taskId,
          pyCall(
            "CloudFunctionInvokeFunctionOperator",
            [],
            [
              ["task_id", pyStr(taskId)],
              ["project_id", pyStr(ctx.ref("project_id"))],
              ["location", pyName("LOCATION")],
              ["function_id", pyStr(fn)],
              ["input_data", pyName("FUNCTION_INPUT")],
              ["gcp_conn_id", pyName("GCP_CONN_ID")],
              ["impersonation_chain", ctx.impersonationChain()],
              ["do_xcom_push", pyBool(false)],
            ],
            { explode: true },
          ),
          1,
        ),
      );
      docSteps.push(
        `\`${taskId}\`: invoke the Cloud Function \`${fn}\` in \`${str(p, "location")}\` with the JSON payload in \`FUNCTION_INPUT\`; the task fails if the function returns an error.`,
      );
      iamRoles.push("`roles/cloudfunctions.developer` on the function (or project): the call API needs `cloudfunctions.functions.call`");
      notes.push(
        "The Cloud Functions v1 call API only reaches 1st gen functions and allows very limited traffic; use it for light, once-per-run triggers. For 2nd gen functions (Cloud Run functions) or heavy work, use a Cloud Run job.",
        "The function runs synchronously within the call; keep it well under the task execution timeout. A retry calls the function again, so make it idempotent for the run date.",
        "Only the execution id is kept in XCom (key `execution_id`); the function response is not pushed (`do_xcom_push=False`) so large responses never reach the Airflow database.",
      );
    }

    if (list(p, "impersonation_chain").length) {
      iamRoles.push("`roles/iam.serviceAccountTokenCreator` for the Composer service account on the impersonated service account");
    }

    return ctx.assemble({ docSteps, constants, body, iamRoles, readmeNotes: notes });
  },
};
