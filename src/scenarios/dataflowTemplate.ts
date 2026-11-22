/**
 * Scenario — Dataflow template job (Classic or Flex).
 *
 * apache-airflow-providers-google 12.0.0:
 *   - DataflowTemplatedJobStartOperator (Classic): `template`, `job_name`,
 *     `parameters`, `environment`, `project_id`, `location` are templated.
 *     Labels go in `environment.additionalUserLabels` (RuntimeEnvironment).
 *   - DataflowStartFlexTemplateOperator (Flex): only `body`, `location`,
 *     `project_id`, `gcp_conn_id` are templated, so the job name, container spec
 *     path, parameters and environment (with additionalUserLabels) live in `body`.
 *   - Neither operator defines `template_ext`; gs:// values ending in .json/.sql
 *     are still guarded so no generated literal looks like a template file.
 *   - deferrable and wait_until_finished=True are mutually exclusive (ValueError).
 *   - append_job_name=False: the job name is already unique per run
 *     (ctx.resourceName), and a deterministic name makes Dataflow refuse a
 *     second concurrent job for the same run instead of silently duplicating it.
 *
 * The small generic helpers exported below ({{ cfg.<key> }} references in
 * free-text values, key=value lists, region / subnetwork checks) are reused by
 * the Dataproc scenarios. They are candidates for common.ts.
 */
import { fillPattern, TEAM } from "../config/naming";
import { comment, pyBool, pyCall, pyDict, pyInt, pyName, pyStr, assign, type Py } from "../lib/python";
import * as v from "../validation/rules";
import { bool, DagContext, guardTemplateExt, list, num, str } from "./common";
import type { Params, Scenario } from "./types";

// ---------------------------------------------------------------------------
// Shared helpers (also used by dataprocEphemeral.ts / dataprocServerless.ts)
// ---------------------------------------------------------------------------

/**
 * Extensions guarded in every templated gs:// value. The Dataflow / Dataproc
 * operators have no template_ext, but the generated DAG must never contain a
 * gs:// literal ending in .json/.sql (project rule), so render them as Jinja.
 */
export const GCS_GUARD_EXT = [".json", ".sql"] as const;

const CFG_REF_RE = /\{\{\s*cfg\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}/g;

/** Env-specific string keys a user may reference as {{ cfg.<key> }} (lists excluded). */
export function cfgKeys(scenario: Scenario, p: Params): string[] {
  return new DagContext(scenario, p).envKeys().filter((k) => !Array.isArray(p[k]));
}

/** "{{ cfg.a }}, {{ cfg.b }}" for help texts. */
export function cfgHint(scenario: Scenario, p: Params): string {
  const keys = cfgKeys(scenario, p);
  return keys.length ? keys.map((k) => `{{ cfg.${k} }}`).join(", ") : "none yet";
}

/**
 * Rewrite {{ cfg.<key> }} in a user-written templated value into the
 * expression for the chosen config mode (config dict or Airflow Variable),
 * so users write the same syntax as in SQL files in both modes.
 */
export function resolveCfgRefs(text: string, ctx: DagContext): string {
  return text.replace(CFG_REF_RE, (_m, key: string) => ctx.ref(key));
}

/** resolveCfgRefs + template_ext guard: the value for a templated operator field. */
export function templatedValue(text: string, ctx: DagContext): string {
  return guardTemplateExt(resolveCfgRefs(text, ctx), GCS_GUARD_EXT);
}

/** Every {{ cfg.x }} must name an env-specific value that is set; no other cfg usage. */
export function cfgRefIssues(text: string, scenario: Scenario, p: Params): v.Issues {
  const used = [...text.matchAll(CFG_REF_RE)].map((m) => m[1]);
  const anyCfg = (text.match(/\bcfg\s*\./g) ?? []).length;
  if (anyCfg > used.length) {
    return v.err("Write env values exactly as {{ cfg.<key> }} (no filters or expressions around cfg).");
  }
  const keys = cfgKeys(scenario, p);
  const unknown = [...new Set(used.filter((k) => !keys.includes(k)))];
  if (unknown.length) {
    return v.err(
      `Unknown or empty env value ${unknown.map((k) => `{{ cfg.${k} }}`).join(", ")}. Available: ${cfgHint(scenario, p)}.`,
    );
  }
  return v.ok();
}

/** Split "key=value" lines (first "=" separates). */
export function parseKeyValues(items: string[]): Array<[string, string]> {
  return items.map((it) => {
    const i = it.indexOf("=");
    return i < 0 ? [it.trim(), ""] : [it.slice(0, i).trim(), it.slice(i + 1).trim()];
  });
}

/** Validate one "key=value" line. */
export function keyValueItem(item: string, keyRe: RegExp, keyHelp: string): v.Issues {
  const i = item.indexOf("=");
  if (i <= 0) return v.err(`"${item}" must look like key=value.`);
  const key = item.slice(0, i).trim();
  if (!keyRe.test(key)) return v.err(`"${key}": ${keyHelp}`);
  return v.ok();
}

/** Duplicate keys in a key=value list. */
export function duplicateKeyIssues(items: string[]): v.Issues {
  const keys = parseKeyValues(items).map(([k]) => k);
  const dups = [...new Set(keys.filter((k, i) => keys.indexOf(k) !== i))];
  return dups.length ? v.err(`Duplicate key(s): ${dups.join(", ")}.`) : v.ok();
}

/** Dataflow / Dataproc need a region, not a BigQuery multi-region. */
export function regionIssues(p: Params, service: string): v.Issues {
  const loc = str(p, "location");
  if (/^(US|EU)$/i.test(loc)) {
    return v.err(`${service} runs in a single GCP region (e.g. northamerica-northeast1), not the multi-region "${loc}".`);
  }
  return v.ok();
}

const SUBNET_PATH_RE =
  /^(?:https:\/\/www\.googleapis\.com\/compute\/v1\/)?(?:projects\/[a-z][a-z0-9-]{4,28}[a-z0-9]\/)?regions\/([a-z]+-[a-z]+\d+)\/subnetworks\/[a-z]([-a-z0-9]{0,61}[a-z0-9])?$/;
const SUBNET_NAME_RE = /^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$/;

/** Subnetwork as a full URL, projects/…/regions/…/subnetworks/…, regions/…/subnetworks/… or a bare name. */
export function subnetworkIssues(value: string, p: Params, allowBareName: boolean): v.Issues {
  if (!value) return v.ok();
  const m = SUBNET_PATH_RE.exec(value);
  if (m) {
    const loc = str(p, "location");
    return m[1] !== loc && !/^(US|EU)$/i.test(loc)
      ? v.warn(`Subnetwork region ${m[1]} differs from the job region ${loc}.`)
      : v.ok();
  }
  if (SUBNET_NAME_RE.test(value)) {
    return allowBareName
      ? v.ok()
      : v.err("Use regions/REGION/subnetworks/NAME, or the full URL https://www.googleapis.com/compute/v1/projects/HOST_PROJECT/regions/REGION/subnetworks/NAME for Shared VPC.");
  }
  return v.err("Not a subnetwork. Use https://www.googleapis.com/compute/v1/projects/HOST_PROJECT/regions/REGION/subnetworks/NAME (Shared VPC) or regions/REGION/subnetworks/NAME.");
}

/** Compute Engine machine type such as n2-standard-4 or custom-4-16384 (empty = service default). */
export function machineTypeIssues(value: string): v.Issues {
  if (!value) return v.ok();
  return /^[a-z][a-z0-9]*(-[a-z0-9]+)+$/.test(value) ? v.ok() : v.err("Use a machine type such as n2-standard-4.");
}

export const MACHINE_TYPES = [
  "n2-standard-2", "n2-standard-4", "n2-standard-8", "n2-highmem-4", "e2-standard-4", "e2-standard-8", "n1-standard-4",
];

// ---------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------

const isClassic = (p: Params) => p.template_type === "classic";
const isFlex = (p: Params) => p.template_type === "flex";
const GOOGLE_TEMPLATES_RE = /^dataflow-templates(?:-([a-z]+-[a-z]+\d+))?$/;

function templateBucketIssues(value: string, p: Params): v.Issues {
  const m = GOOGLE_TEMPLATES_RE.exec(value);
  if (!m) return v.gcsBucket(value);
  const loc = str(p, "location");
  if (m[1] && m[1] !== loc && !/^(US|EU)$/i.test(loc)) {
    return v.warn(`Google-provided templates should come from the job region's bucket: dataflow-templates-${loc}.`);
  }
  return v.ok();
}

const PARAM_KEY_RE = /^[A-Za-z][A-Za-z0-9_.-]*$/;

export const dataflowTemplate: Scenario = {
  id: "dataflow_template",
  title: "Dataflow template job",
  description:
    "Launch a Dataflow job from a Classic template or a Flex template (Google-provided or your own), with env-specific temp bucket, worker service account and network, and cost labels on the job.",
  naming: { source: TEAM.systemCodes.gcs, target: TEAM.systemCodes.dataflow },
  sections: [
    { id: "template", title: "Template" },
    { id: "job", title: "Job parameters" },
    { id: "workers", title: "Workers, network & storage" },
  ],
  commonDefaults: {
    dag_id: "clickstream_gcs_to_df_daily",
    description: "Load daily clickstream JSON files into BigQuery with a Dataflow template.",
    domain: "clickstream",
    tags: ["clickstream", "dataflow"],
    location: "northamerica-northeast1",
    execution_timeout_minutes: 120,
  },
  fields: [
    // Template -----------------------------------------------------------------------
    {
      key: "template_type", label: "Template type", type: "select", section: "template", default: "classic",
      options: [
        { value: "classic", label: "Classic template (DataflowTemplatedJobStartOperator)" },
        { value: "flex", label: "Flex template (DataflowStartFlexTemplateOperator)" },
      ],
      help: "Classic templates are staged job graphs; Flex templates are container images described by a container spec JSON file. Google-provided templates exist in both forms.",
    },
    {
      key: "template_bucket", label: "Template bucket", type: "text", section: "template",
      default: "dataflow-templates-northamerica-northeast1", required: true, envSpecific: true,
      validate: (x, p) => templateBucketIssues(String(x), p),
      help: "Bucket name only (no gs://). Google-provided templates: dataflow-templates-<region>. Your own templates: your env-specific code bucket.",
    },
    {
      key: "template_object", label: "Template path in the bucket", type: "text", section: "template",
      default: "latest/GCS_Text_to_BigQuery", required: true,
      validate: (x) => v.gcsObject(String(x)),
      help: (p) =>
        isFlex(p)
          ? "Path of the Flex template container spec, e.g. latest/flex/GCS_Text_to_BigQuery_Flex (Google) or templates/flex/my_pipeline.json (yours)."
          : "Path of the Classic template, e.g. latest/GCS_Text_to_BigQuery (Google) or templates/my_pipeline (yours).",
    },
    // Job -----------------------------------------------------------------------------
    {
      key: "template_parameters", label: "Template parameters", type: "list", section: "job",
      default: [
        "inputFilePattern=gs://{{ cfg.data_bucket }}/clickstream/{{ ds_nodash }}/*.json",
        "JSONPath=gs://{{ cfg.data_bucket }}/schemas/clickstream_events.json",
        "javascriptTextTransformGcsPath=gs://{{ cfg.data_bucket }}/udf/clickstream_transform.js",
        "javascriptTextTransformFunctionName=transform",
        "outputTable={{ cfg.project_id }}:raw_clickstream.events",
        "bigQueryLoadingTemporaryDirectory=gs://{{ cfg.temp_bucket }}/dataflow/bq_load",
      ],
      allowJinja: true,
      validateItem: (x, p) =>
        v.merge(
          keyValueItem(x, PARAM_KEY_RE, "parameter names start with a letter and use letters, digits, '_', '.', '-'."),
          cfgRefIssues(x, dataflowTemplate, p),
        ),
      help: (p) =>
        `One key=value per line, as listed in the template's documentation. Airflow macros such as {{ ds }} / {{ ds_nodash }} are allowed. Env-specific values: ${cfgHint(dataflowTemplate, p)}.`,
    },
    {
      key: "wait_mode", label: "Completion", type: "select", section: "job", default: "wait",
      options: [
        { value: "wait", label: "Wait until the job finishes (batch)" },
        { value: "submit", label: "Submit only, don't wait (streaming / fire-and-forget)" },
      ],
      help: "Batch jobs: wait, so downstream tasks see the results and failures fail the task (deferrable when enabled). Streaming jobs never finish: submit only.",
    },
    // Workers / network / storage ---------------------------------------------------------
    {
      key: "temp_bucket", label: "Temp bucket", type: "text", section: "workers",
      default: "acme-dev-clickstream-temp", required: true, envSpecific: true, suggestable: true,
      validate: (x) => v.gcsBucket(String(x)),
      help: (p) =>
        `Bucket name only. Dataflow writes temporary files to gs://<bucket>/dataflow/temp${isFlex(p) ? " and stages files in gs://<bucket>/dataflow/staging" : ""}. Env-specific.`,
    },
    {
      key: "data_bucket", label: "Data bucket (optional)", type: "text", section: "workers",
      default: "acme-dev-clickstream-landing", envSpecific: true, suggestable: true,
      validate: (x) => v.gcsBucket(String(x)),
      help: "Bucket name only. Not used directly: reference it in template parameters as {{ cfg.data_bucket }} so each env reads its own bucket.",
    },
    {
      key: "dataflow_service_account", label: "Worker service account (optional)", type: "text", section: "workers",
      default: "dataflow-worker@acme-dev-data.iam.gserviceaccount.com", envSpecific: true,
      validate: (x) => v.serviceAccount(String(x)),
      help: "Identity of the Dataflow workers (needs roles/dataflow.worker). Empty = Compute Engine default service account (not recommended). Env-specific.",
    },
    {
      key: "machine_type", label: "Worker machine type (optional)", type: "text", section: "workers", default: "",
      suggestions: MACHINE_TYPES, validate: (x) => machineTypeIssues(String(x)),
      help: "Empty = Dataflow default for the template.",
    },
    {
      key: "max_workers", label: "Max workers", type: "number", section: "workers", default: 10, min: 1, max: 1000,
      help: "Upper bound for autoscaling; caps cost.",
    },
    {
      key: "network", label: "Network (optional)", type: "text", section: "workers", default: "", envSpecific: true,
      validate: (x) => (SUBNET_NAME_RE.test(String(x)) ? v.ok() : v.err("Use the VPC network name, e.g. default.")),
      help: "VPC network name. Leave empty when you set a subnetwork (always the case for Shared VPC). Env-specific.",
    },
    {
      key: "subnetwork", label: "Subnetwork (optional)", type: "text", section: "workers", default: "", envSpecific: true,
      validate: (x, p) => subnetworkIssues(String(x), p, false),
      help: "regions/REGION/subnetworks/NAME, or for Shared VPC the full URL https://www.googleapis.com/compute/v1/projects/HOST_PROJECT/regions/REGION/subnetworks/NAME. Env-specific.",
    },
    {
      key: "internal_ip_only", label: "Workers use internal IPs only", type: "boolean", section: "workers", default: false,
      help: "No public IPs on workers (ipConfiguration=WORKER_IP_PRIVATE). The subnetwork needs Private Google Access.",
    },
  ],

  samples: {
    classic_gcs_text_to_bq: {
      dag_id: "clickstream_gcs_to_df_daily",
      email: ["clickstream-alerts@example.com"],
    },
    flex_custom_variables: {
      dag_id: "iot_gcs_to_df_hourly",
      description: "Enrich hourly IoT sensor readings with a custom Flex template.",
      domain: "iot",
      config_mode: "variables",
      template_type: "flex",
      template_bucket: "acme-dev-iot-code",
      template_object: "templates/flex/iot_sensor_enrichment.json",
      template_parameters: [
        "input=gs://{{ cfg.data_bucket }}/iot/readings/{{ ds_nodash }}/{{ logical_date.strftime('%H') }}/*.avro",
        "output_table={{ cfg.project_id }}:curated_iot.sensor_readings",
        "device_registry=gs://{{ cfg.data_bucket }}/reference/devices.json",
        "window_minutes=5",
      ],
      temp_bucket: "acme-dev-iot-temp",
      data_bucket: "acme-dev-iot-landing",
      dataflow_service_account: "iot-dataflow@acme-dev-data.iam.gserviceaccount.com",
      machine_type: "n2-standard-4",
      max_workers: 20,
      subnetwork:
        "https://www.googleapis.com/compute/v1/projects/acme-dev-network/regions/northamerica-northeast1/subnetworks/acme-dev-dataflow",
      internal_ip_only: true,
      impersonation_chain: ["iot-composer@acme-dev-data.iam.gserviceaccount.com"],
      schedule_preset: "@hourly",
      dagrun_timeout_minutes: 55,
      execution_timeout_minutes: 50,
      tags: ["iot", "dataflow", "flex"],
    },
    classic_streaming_submit: {
      dag_id: "logs_ps_to_df_adhoc",
      description: "Start the streaming Pub/Sub to BigQuery pipeline for application logs.",
      domain: "logs",
      template_object: "latest/PubSub_Subscription_to_BigQuery",
      template_parameters: [
        "inputSubscription=projects/{{ cfg.project_id }}/subscriptions/app-logs-dataflow",
        "outputTableSpec={{ cfg.project_id }}:raw_logs.app_events",
        "outputDeadletterTable={{ cfg.project_id }}:raw_logs.app_events_deadletter",
      ],
      wait_mode: "submit",
      temp_bucket: "acme-dev-logs-temp",
      data_bucket: "",
      network: "default",
      max_workers: 5,
      machine_type: "e2-standard-4",
      schedule_preset: "none",
      retries: 1,
      tags: ["logs", "dataflow", "streaming"],
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => {
      out[k] = out[k] ? v.merge(out[k], i) : i;
    };
    add("location", regionIssues(p, "Dataflow"));
    const params = list(p, "template_parameters");
    add("template_parameters", duplicateKeyIssues(params));
    const obj = str(p, "template_object");
    if (isClassic(p) && obj.endsWith(".json")) {
      add("template_object", v.warn("A .json file is usually a Flex template container spec: choose the Flex template type."));
    }
    if (isFlex(p) && !obj.endsWith(".json") && !obj.includes("flex/")) {
      add("template_object", v.warn("Flex templates are launched from a container spec file (usually .json, or latest/flex/<Name> for Google templates)."));
    }
    if (str(p, "network") && str(p, "subnetwork")) {
      add("network", v.warn("With a subnetwork set, the network is usually left empty (it must match the subnetwork's network)."));
    }
    if (p.wait_mode === "submit" && !["none", "@once"].includes(str(p, "schedule_preset"))) {
      add("wait_mode", v.warn("Submit only on a schedule starts a new job every run; streaming jobs are normally started once (schedule None)."));
    }
    if (p.wait_mode === "wait" && num(p, "execution_timeout_minutes") < 30) {
      add("execution_timeout_minutes", v.warn("Dataflow jobs take minutes to start; the task timeout must cover the whole job."));
    }
    return out;
  },

  suggest(ctx) {
    const out: Partial<Params> = {};
    const temp = fillPattern("gcsBucket", { ...ctx, purpose: TEAM.bucketPurposes.temp });
    const data = fillPattern("gcsBucket", { ...ctx, purpose: TEAM.bucketPurposes.landing });
    if (temp) out.temp_bucket = temp;
    if (data) out.data_bucket = data;
    return out;
  },

  generate(p) {
    const ctx = new DagContext(dataflowTemplate, p);
    const flex = isFlex(p);
    const wait = str(p, "wait_mode") !== "submit";
    const deferrable = wait && bool(p, "deferrable");
    const has = (k: string) => ctx.envKeys().includes(k);

    const parameters = pyDict(
      parseKeyValues(list(p, "template_parameters")).map(([k, val]): [string, Py] => [k, pyStr(templatedValue(val, ctx))]),
      { explode: true },
    );

    // RuntimeEnvironment (classic) / FlexTemplateRuntimeEnvironment (flex)
    const tempRoot = `gs://${ctx.ref("temp_bucket")}/dataflow`;
    const env: Array<[string, Py]> = [["tempLocation", pyStr(`${tempRoot}/temp`)]];
    if (flex) env.push(["stagingLocation", pyStr(`${tempRoot}/staging`)]);
    if (has("dataflow_service_account")) env.push(["serviceAccountEmail", pyStr(ctx.ref("dataflow_service_account"))]);
    if (str(p, "machine_type")) env.push(["machineType", pyStr(str(p, "machine_type"))]);
    env.push(["maxWorkers", pyInt(num(p, "max_workers"))]);
    if (has("network")) env.push(["network", pyStr(ctx.ref("network"))]);
    if (has("subnetwork")) env.push(["subnetwork", pyStr(ctx.ref("subnetwork"))]);
    if (bool(p, "internal_ip_only")) env.push(["ipConfiguration", pyStr("WORKER_IP_PRIVATE")]);
    env.push(["additionalUserLabels", pyName("LABELS")]);
    const environment = pyDict(env, { explode: true });

    const templatePath = templatedValue(`gs://${ctx.ref("template_bucket")}/${str(p, "template_object").replace(/^\/+/, "")}`, ctx);
    const jobName = ctx.resourceName({ suffix: "ts" });

    const waitKwargs: Array<[string, Py]> = wait
      ? deferrable
        ? [["deferrable", pyBool(true)]]
        : [["wait_until_finished", pyBool(true)], ["deferrable", pyBool(false)]]
      : [["wait_until_finished", pyBool(false)], ["deferrable", pyBool(false)]];

    const taskId = "run_dataflow_template";
    let task: Py;
    if (flex) {
      ctx.importFrom("airflow.providers.google.cloud.operators.dataflow", "DataflowStartFlexTemplateOperator");
      task = pyCall(
        "DataflowStartFlexTemplateOperator",
        [],
        [
          ["task_id", pyStr(taskId)],
          ["project_id", pyStr(ctx.ref("project_id"))],
          ["location", pyName("LOCATION")],
          [
            "body",
            pyDict(
              [
                [
                  "launchParameter",
                  pyDict(
                    [
                      ["jobName", pyName("JOB_NAME")],
                      ["containerSpecGcsPath", pyStr(templatePath)],
                      ["parameters", parameters],
                      ["environment", environment],
                    ],
                    { explode: true },
                  ),
                ],
              ],
              { explode: true },
            ),
          ],
          ["append_job_name", pyBool(false)],
          ["gcp_conn_id", pyName("GCP_CONN_ID")],
          ["impersonation_chain", ctx.impersonationChain()],
          ...waitKwargs,
        ],
        { explode: true },
      );
    } else {
      ctx.importFrom("airflow.providers.google.cloud.operators.dataflow", "DataflowTemplatedJobStartOperator");
      task = pyCall(
        "DataflowTemplatedJobStartOperator",
        [],
        [
          ["task_id", pyStr(taskId)],
          ["project_id", pyStr(ctx.ref("project_id"))],
          ["location", pyName("LOCATION")],
          ["template", pyStr(templatePath)],
          ["job_name", pyName("JOB_NAME")],
          ["parameters", parameters],
          ["environment", environment],
          ["append_job_name", pyBool(false)],
          ["gcp_conn_id", pyName("GCP_CONN_ID")],
          ["impersonation_chain", ctx.impersonationChain()],
          ...waitKwargs,
        ],
        { explode: true },
      );
    }

    const constants = [
      comment("Dataflow job name: dag_id + run timestamp, unique per run (rendered at run time)."),
      assign("JOB_NAME", pyStr(jobName)),
    ];

    const kind = flex ? "Flex" : "Classic";
    const docSteps = [
      `\`${taskId}\`: launch the ${kind} template \`gs://<template_bucket>/${str(p, "template_object")}\` as job \`<dag_id>-<run timestamp>\` in ${str(p, "location")}` +
        (wait ? ", and wait until it finishes." : " without waiting for it to finish."),
    ];

    const iamRoles = [
      "`roles/dataflow.developer` on the project that runs the job",
      "`roles/iam.serviceAccountUser` on the worker service account",
      "Worker service account: `roles/dataflow.worker`, `roles/storage.objectAdmin` on the temp bucket, plus read/write access to the job's sources and sinks",
    ];
    if (flex) iamRoles.push("Worker service account: `roles/artifactregistry.reader` on the Flex template image repository");
    if (has("subnetwork")) {
      iamRoles.push("Shared VPC: `roles/compute.networkUser` on the subnetwork for the Dataflow service agent and the Compute Engine service agent");
    }
    if (list(p, "impersonation_chain").length) {
      iamRoles.push("`roles/iam.serviceAccountTokenCreator` for the Composer service account on the impersonated service account");
    }

    return ctx.assemble({
      docSteps,
      constants,
      body: [assign(taskId, task, 1)],
      iamRoles,
      readmeNotes: [
        "The job name is `<dag_id>-<run timestamp>` (append_job_name=False): a retry of the same run reuses the name, so Dataflow refuses to start a duplicate while the first job is still running.",
        "Labels (env, team, owner, cost_center, dag_id) are applied to the job and its workers via additionalUserLabels.",
        "Whether a re-run is idempotent depends on the template: most Google-provided BigQuery templates append rows. Write to a run-specific partition or deduplicate downstream.",
        ...(wait
          ? deferrable
            ? ["The task defers while the job runs: make sure the Composer environment has a triggerer (Composer 2: set triggerer count >= 1)."]
            : ["The task polls the job until it finishes; the task execution timeout must cover the whole job."]
          : ["The task only submits the job. Monitor and stop streaming jobs in the Dataflow console (drain to avoid data loss)."]),
      ],
    });
  },
};
