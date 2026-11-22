/**
 * Scenario — Dataproc Serverless batch (PySpark or Spark).
 *
 * apache-airflow-providers-google 12.0.0, DataprocCreateBatchOperator:
 *   - `batch`, `batch_id`, `project_id`, `region` are templated; no template_ext.
 *   - batch labels go in `batch["labels"]`; deferrable=True is supported.
 *   - batch_id: 4-63 chars of [a-z0-9-]. If the id already exists the operator
 *     attaches to that batch instead of creating a new one, so a retry of a
 *     FAILED batch would just re-read the failure. The id therefore ends with
 *     the try number: a retry (or a cleared task) launches a fresh batch, while
 *     a resumed attempt still attaches to its own batch.
 */
import { fillPattern, TEAM } from "../config/naming";
import { assign, comment, pyCall, pyDict, pyList, pyName, pyStr, type Py } from "../lib/python";
import * as v from "../validation/rules";
import { bool, DagContext, list, str } from "./common";
import { subnetworkIssues } from "./dataflowTemplate";
import { isPySpark, sparkJobFields, sparkJobSpec, sparkJobValidate } from "./dataprocEphemeral";
import type { Params, Scenario } from "./types";

/** Suffix appended to the batch id; renders to "-<try number>" (max 3 chars for retries <= 10). */
const TRY_SUFFIX = "-{{ ti.try_number }}";
const TRY_SUFFIX_LEN = 3;

export const dataprocServerless: Scenario = {
  id: "dataproc_serverless",
  title: "Dataproc Serverless batch",
  description:
    "Run a PySpark or Spark workload on Dataproc Serverless — no cluster to manage — with an env-specific service account and network, runtime properties and cost labels on the batch.",
  naming: { source: TEAM.systemCodes.gcs, target: TEAM.systemCodes.dataproc },
  sections: [
    { id: "job", title: "Spark workload" },
    { id: "runtime", title: "Runtime" },
    { id: "network", title: "Service account & network" },
    { id: "storage", title: "Buckets" },
  ],
  commonDefaults: {
    dag_id: "clickstream_gcs_to_dp_daily",
    description: "Sessionize daily clickstream events with a Dataproc Serverless PySpark batch.",
    domain: "clickstream",
    tags: ["clickstream", "dataproc", "serverless"],
    location: "northamerica-northeast1",
    execution_timeout_minutes: 120,
  },
  fields: [
    ...sparkJobFields(() => dataprocServerless, "batch", "clickstream"),
    // Runtime ------------------------------------------------------------------------------
    {
      key: "runtime_version", label: "Runtime version", type: "text", section: "runtime", default: "2.2",
      required: true, suggestions: ["2.2", "2.3", "1.2"],
      validate: (x) => (/^\d+\.\d+$/.test(String(x)) ? v.ok() : v.err("Use a Serverless runtime version such as 2.2.")),
      help: "Dataproc Serverless runtime (Spark, Java, Python versions). 2.2 = Spark 3.5, Python 3.12.",
    },
    // Network --------------------------------------------------------------------------------
    {
      key: "batch_service_account", label: "Batch service account (optional)", type: "text", section: "network",
      default: "dataproc-runner@acme-dev-data.iam.gserviceaccount.com", envSpecific: true,
      validate: (x) => v.serviceAccount(String(x)),
      help: "Identity the workload runs as (needs roles/dataproc.worker plus data access). Empty = Compute Engine default service account (not recommended). Env-specific.",
    },
    {
      key: "subnetwork", label: "Subnetwork (optional)", type: "text", section: "network", default: "", envSpecific: true,
      validate: (x, p) => subnetworkIssues(String(x), p, true),
      help: "Subnetwork name, or for Shared VPC the full URL https://www.googleapis.com/compute/v1/projects/HOST_PROJECT/regions/REGION/subnetworks/NAME. It needs Private Google Access. Empty = the default network. Env-specific.",
    },
    {
      key: "network_tags", label: "Network tags (optional)", type: "list", section: "network", default: [],
      validateItem: (x) => (/^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$/.test(x) ? v.ok() : v.err(`"${x}" is not a valid network tag.`)),
      help: "One per line; used by firewall rules.",
    },
    {
      key: "staging_bucket", label: "Staging bucket (optional)", type: "text", section: "storage", default: "",
      envSpecific: true, suggestable: true, validate: (x) => v.gcsBucket(String(x)),
      help: "Bucket name only, for workload dependencies and driver output. Empty = a Dataproc-managed bucket. Env-specific.",
    },
  ],

  samples: {
    pyspark_sessions_daily_variables: {
      dag_id: "clickstream_gcs_to_dp_daily",
      config_mode: "variables",
      email: ["clickstream-alerts@example.com"],
      script_name: "sessionize_events.py",
      job_args: [
        "--input=gs://{{ cfg.data_bucket }}/clickstream/events/dt={{ ds }}/",
        "--output=gs://{{ cfg.data_bucket }}/clickstream/sessions/",
        "--run-date={{ ds }}",
        "--input-format=json",
      ],
      python_file_uris: ["gs://{{ cfg.code_bucket }}/libs/clickstream_common.zip"],
      spark_properties: ["spark.executor.instances=4", "spark.dynamicAllocation.maxExecutors=20"],
      subnetwork:
        "https://www.googleapis.com/compute/v1/projects/acme-dev-network/regions/northamerica-northeast1/subnetworks/acme-dev-serverless",
      network_tags: ["dataproc-serverless"],
      staging_bucket: "acme-dev-clickstream-temp",
      impersonation_chain: ["clickstream-composer@acme-dev-data.iam.gserviceaccount.com"],
    },
    spark_jar_hourly: {
      dag_id: "logs_gcs_to_dp_hourly",
      description: "Hourly log compaction with a Spark (Scala) batch.",
      domain: "logs",
      job_type: "spark",
      main_class: "com.acme.logs.CompactHourly",
      jar_file_uris: ["gs://{{ cfg.code_bucket }}/jars/log-compactor-2.3.1.jar"],
      job_args: [
        "--input=gs://{{ cfg.data_bucket }}/logs/raw/{{ ds_nodash }}/{{ logical_date.strftime('%H') }}/",
        "--output=gs://{{ cfg.data_bucket }}/logs/compacted/",
      ],
      code_bucket: "acme-dev-logs-code",
      data_bucket: "acme-dev-logs-landing",
      runtime_version: "2.3",
      batch_service_account: "logs-spark@acme-dev-data.iam.gserviceaccount.com",
      schedule_preset: "@hourly",
      dagrun_timeout_minutes: 55,
      execution_timeout_minutes: 45,
      tags: ["logs", "dataproc", "serverless"],
    },
    pyspark_iot_monthly: {
      dag_id: "iot_gcs_to_dp_monthly",
      description: "Monthly IoT device health report.",
      domain: "iot",
      script_name: "device_health_report.py",
      job_args: [
        "--input=gs://{{ cfg.data_bucket }}/iot/readings/month={{ logical_date.strftime('%Y-%m') }}/",
        "--output=gs://{{ cfg.data_bucket }}/iot/reports/",
        "--run-date={{ ds }}",
      ],
      code_bucket: "acme-dev-iot-code",
      data_bucket: "acme-dev-iot-landing",
      batch_service_account: "",
      deferrable: false,
      schedule_preset: "@monthly",
    },
  },

  validate(p) {
    return sparkJobValidate(p, "Dataproc Serverless");
  },

  suggest(ctx) {
    const out: Partial<Params> = {};
    const code = fillPattern("gcsBucket", { ...ctx, purpose: TEAM.bucketPurposes.code });
    const data = fillPattern("gcsBucket", { ...ctx, purpose: TEAM.bucketPurposes.landing });
    if (code) out.code_bucket = code;
    if (data) out.data_bucket = data;
    return out;
  },

  generate(p) {
    const ctx = new DagContext(dataprocServerless, p);
    ctx.importFrom("airflow.providers.google.cloud.operators.dataproc", "DataprocCreateBatchOperator");
    const has = (k: string) => ctx.envKeys().includes(k);

    const spec = sparkJobSpec(ctx, p, { pyspark: "pyspark_batch", spark: "spark_batch" });

    const runtime: Array<[string, Py]> = [["version", pyStr(str(p, "runtime_version"))]];
    if (spec.properties.length) {
      runtime.push(["properties", pyDict(spec.properties.map(([k, val]): [string, Py] => [k, pyStr(val)]), { explode: true })]);
    }
    const exec: Array<[string, Py]> = [];
    if (has("batch_service_account")) exec.push(["service_account", pyStr(ctx.ref("batch_service_account"))]);
    if (has("subnetwork")) exec.push(["subnetwork_uri", pyStr(ctx.ref("subnetwork"))]);
    if (list(p, "network_tags").length) exec.push(["network_tags", pyList(list(p, "network_tags").map(pyStr))]);
    if (has("staging_bucket")) exec.push(["staging_bucket", pyStr(ctx.ref("staging_bucket"))]);

    const batch: Array<[string, Py]> = [
      [spec.key, pyDict(spec.entries, { explode: true })],
      ["runtime_config", pyDict(runtime, { explode: true })],
    ];
    if (exec.length) {
      batch.push(["environment_config", pyDict([["execution_config", pyDict(exec, { explode: true })]], { explode: true })]);
    }
    batch.push(["labels", pyName("LABELS")]);

    const taskId = isPySpark(p) ? "submit_pyspark_batch" : "submit_spark_batch";
    const task = pyCall(
      "DataprocCreateBatchOperator",
      [],
      [
        ["task_id", pyStr(taskId)],
        ["project_id", pyStr(ctx.ref("project_id"))],
        ["region", pyName("LOCATION")],
        ["batch_id", pyName("BATCH_ID")],
        ["batch", pyDict(batch, { explode: true })],
        ["gcp_conn_id", pyName("GCP_CONN_ID")],
        ["impersonation_chain", ctx.impersonationChain()],
        ["deferrable", ctx.deferrable()],
      ],
      { explode: true },
    );

    const batchId = ctx.resourceName({ suffix: "ts", maxLength: 63 - TRY_SUFFIX_LEN }) + TRY_SUFFIX;
    const constants = [
      comment("Batch id: dag_id + run timestamp + try number (4-63 chars, rendered at run time)."),
      comment("A retry starts a new batch instead of re-attaching to the failed one."),
      assign("BATCH_ID", pyStr(batchId)),
    ];

    const what = isPySpark(p) ? `PySpark script \`scripts/<dag_id>/${str(p, "script_name")}\`` : `Spark main class \`${str(p, "main_class")}\``;
    const docSteps = [
      `\`${taskId}\`: run the ${what} as Dataproc Serverless batch \`<dag_id>-<run timestamp>-<try>\` (runtime ${str(p, "runtime_version")}) in ${str(p, "location")} and wait for it to finish.`,
    ];

    const iamRoles = [
      "`roles/dataproc.editor` on the project (create batches)",
      "`roles/iam.serviceAccountUser` on the batch service account",
      "Batch service account: `roles/dataproc.worker`, `roles/storage.objectViewer` on the code bucket, plus access to the workload's data (e.g. `roles/storage.objectAdmin` on the data bucket, BigQuery roles)",
    ];
    if (has("subnetwork")) iamRoles.push("Shared VPC: `roles/compute.networkUser` on the subnetwork for the Dataproc service agent");
    if (list(p, "impersonation_chain").length) {
      iamRoles.push("`roles/iam.serviceAccountTokenCreator` for the Composer service account on the impersonated service account");
    }

    return ctx.assemble({
      docSteps,
      constants,
      body: [assign(taskId, task, 1)],
      iamRoles,
      readmeNotes: [
        ...(isPySpark(p)
          ? [`Replace the starter logic in \`scripts/${ctx.dagId}/${str(p, "script_name")}\` with your own, then upload it to the code bucket of every env before the first run.`]
          : []),
        "The batch id is `<dag_id>-<run timestamp>-<try number>`: every run and every retry gets its own batch, visible in the Dataproc Batches console.",
        "The subnetwork must have Private Google Access enabled; Serverless workers have internal IPs only.",
        "Labels (env, team, owner, cost_center, dag_id) are set on every batch for cost tracking.",
        ...(bool(p, "deferrable")
          ? ["The task defers while the batch runs: make sure the Composer environment has a triggerer (Composer 2: set triggerer count >= 1)."]
          : []),
      ],
    });
  },
};
