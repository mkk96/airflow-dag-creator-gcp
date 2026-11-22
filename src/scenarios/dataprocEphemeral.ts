/**
 * Scenario — Dataproc ephemeral cluster: create cluster → submit PySpark/Spark
 * job → delete cluster (always).
 *
 * apache-airflow-providers-google 12.0.0:
 *   - DataprocCreateClusterOperator: `cluster_name`, `cluster_config`, `labels`,
 *     `project_id`, `region` are templated. Cluster labels must be passed via
 *     `labels` (labels inside cluster_config are ignored). use_if_exists=True
 *     (default) makes a retry reuse the cluster of the same run.
 *   - DataprocSubmitJobOperator: `job`, `project_id`, `region` are templated;
 *     job labels go in `job["labels"]`.
 *   - DataprocDeleteClusterOperator: `cluster_name`, `project_id`, `region` are
 *     templated. Runs with trigger_rule=all_done so the cluster is always removed.
 *   - All three support deferrable=True. None defines `template_ext`.
 *
 * Because the delete task succeeds even when the job failed, a final
 * `end_pipeline` task (all_success) is the DAG's other leaf: it becomes
 * upstream_failed when the job fails, so the DAG run is marked failed.
 * A lifecycle idle-delete TTL is a safety net if the delete task never runs.
 */
import { fillPattern, TEAM } from "../config/naming";
import { GENERATOR_NAME, RUNTIME } from "../config/runtime";
import {
  assign,
  comment,
  pyBool,
  pyCall,
  pyDict,
  pyDocstring,
  pyIdent,
  pyInt,
  pyList,
  pyName,
  pyStr,
  withHeader,
  type Py,
} from "../lib/python";
import * as v from "../validation/rules";
import { bool, DagContext, list, num, str } from "./common";
import {
  cfgHint,
  cfgRefIssues,
  duplicateKeyIssues,
  keyValueItem,
  MACHINE_TYPES,
  machineTypeIssues,
  parseKeyValues,
  regionIssues,
  subnetworkIssues,
  templatedValue,
} from "./dataflowTemplate";
import type { FieldSchema, Params, Scenario } from "./types";

// ---------------------------------------------------------------------------
// Spark job helpers (shared with dataprocServerless.ts)
// ---------------------------------------------------------------------------

export const isPySpark = (p: Params) => p.job_type === "pyspark";
export const isSpark = (p: Params) => p.job_type === "spark";

const URI_RE = /^(gs|file|hdfs):\/\/\S.*$/;
const JAVA_CLASS_RE = /^[A-Za-z_$][\w$]*(\.[A-Za-z_$][\w$]*)+$/;
const SPARK_PROP_RE = /^[A-Za-z][A-Za-z0-9_.:-]*$/;

function uriItem(item: string, scenario: Scenario, p: Params): v.Issues {
  if (!URI_RE.test(item)) return v.err(`"${item}" must be a gs://, file:// or hdfs:// URI.`);
  return cfgRefIssues(item, scenario, p);
}

/**
 * Job fields (section "job") + code/data buckets (section "storage"), shared by
 * the cluster and serverless scenarios. `who` is "job" or "batch".
 */
export function sparkJobFields(scenario: () => Scenario, who: "job" | "batch", dataDomain: string): FieldSchema[] {
  return [
    {
      key: "job_type", label: "Job type", type: "select", section: "job", default: "pyspark",
      options: [
        { value: "pyspark", label: "PySpark (script shipped with the DAG)" },
        { value: "spark", label: "Spark (Java/Scala main class in a jar)" },
      ],
    },
    {
      key: "script_name", label: "PySpark script file name", type: "text", section: "job",
      default: `${dataDomain}_transform.py`, required: true, showIf: isPySpark,
      validate: (x) =>
        /^[a-z][a-z0-9_]*\.py$/.test(String(x)) ? v.ok() : v.err("Use a snake_case Python file name, e.g. build_features.py."),
      help: "A starter PySpark script with this name is generated in scripts/<dag_id>/. Replace its logic with yours, keep its arguments, and upload it to the code bucket.",
    },
    {
      key: "main_class", label: "Main class", type: "text", section: "job", default: "org.apache.spark.examples.SparkPi",
      required: true, showIf: isSpark,
      validate: (x) => (JAVA_CLASS_RE.test(String(x)) ? v.ok() : v.err("Use a fully qualified class name, e.g. com.acme.jobs.Main.")),
      help: "Fully qualified class with the main() method.",
    },
    {
      key: "jar_file_uris", label: "Jar files", type: "list", section: "job",
      default: ["file:///usr/lib/spark/examples/jars/spark-examples.jar"], allowJinja: true, showIf: isSpark,
      validateItem: (x, p) => uriItem(x, scenario(), p),
      help: (p) =>
        `One URI per line: the jar with the main class plus its dependencies, e.g. gs://{{ cfg.code_bucket }}/jars/my-job.jar. Env-specific values: ${cfgHint(scenario(), p)}.`,
    },
    {
      key: "python_file_uris", label: "Extra Python files (optional)", type: "list", section: "job", default: [],
      allowJinja: true, showIf: isPySpark,
      validateItem: (x, p) => uriItem(x, scenario(), p),
      help: "One URI per line (.py, .zip or .egg) added to the PYTHONPATH, e.g. gs://{{ cfg.code_bucket }}/libs/common.zip.",
    },
    {
      key: "pyspark_jar_file_uris", label: "Extra jars (optional)", type: "list", section: "job", default: [],
      allowJinja: true, showIf: isPySpark,
      validateItem: (x, p) => uriItem(x, scenario(), p),
      help: "One URI per line, e.g. a JDBC driver. The BigQuery and GCS connectors are already included.",
    },
    {
      key: "job_args", label: "Arguments", type: "list", section: "job",
      default: [
        `--input=gs://{{ cfg.data_bucket }}/${dataDomain}/raw/dt={{ ds }}/`,
        `--output=gs://{{ cfg.data_bucket }}/${dataDomain}/curated/`,
        "--run-date={{ ds }}",
      ],
      allowJinja: true,
      validateItem: (x, p) => cfgRefIssues(x, scenario(), p),
      help: (p) =>
        `One argument per line, passed to the ${who} as-is. Airflow macros such as {{ ds }} are allowed. Env-specific values: ${cfgHint(scenario(), p)}. The starter PySpark script expects --input, --output and --run-date.`,
    },
    {
      key: "spark_properties", label: "Spark properties (optional)", type: "list", section: "job", default: [],
      validateItem: (x) => keyValueItem(x, SPARK_PROP_RE, "use a Spark property name such as spark.executor.memory."),
      help: "One key=value per line, e.g. spark.executor.memory=8g or spark.sql.shuffle.partitions=200.",
    },
    // Storage --------------------------------------------------------------------------
    {
      key: "code_bucket", label: "Code bucket", type: "text", section: "storage",
      default: `acme-dev-${dataDomain}-code`, required: true, envSpecific: true, suggestable: true,
      validate: (x) => v.gcsBucket(String(x)),
      help: "Bucket name only. Upload the bundle's scripts/<dag_id>/ folder here (gs://<bucket>/scripts/<dag_id>/); also usable in URIs as {{ cfg.code_bucket }}. Env-specific.",
    },
    {
      key: "data_bucket", label: "Data bucket (optional)", type: "text", section: "storage",
      default: `acme-dev-${dataDomain}-landing`, envSpecific: true, suggestable: true,
      validate: (x) => v.gcsBucket(String(x)),
      help: "Bucket name only. Not used directly: reference it in arguments as {{ cfg.data_bucket }} so each env reads its own data.",
    },
  ];
}

/** Cross-field checks shared by both Dataproc scenarios. */
export function sparkJobValidate(p: Params, service: string): Record<string, v.Issues> {
  const out: Record<string, v.Issues> = {};
  const add = (k: string, i: v.Issues) => {
    out[k] = out[k] ? v.merge(out[k], i) : i;
  };
  add("location", regionIssues(p, service));
  add("spark_properties", duplicateKeyIssues(list(p, "spark_properties")));
  if (isSpark(p) && list(p, "jar_file_uris").length === 0) {
    add("jar_file_uris", v.warn("No jar: the main class must already be on the cluster's classpath."));
  }
  return out;
}

export interface SparkJobSpec {
  /** "pyspark_job" / "spark_job" (cluster) or "pyspark_batch" / "spark_batch" (serverless). */
  key: string;
  entries: Array<[string, Py]>;
  /** Spark properties (key, value) — placed by the caller (job vs runtime_config). */
  properties: Array<[string, string]>;
}

/** Build the PySpark / Spark job spec; registers the PySpark starter script. */
export function sparkJobSpec(ctx: DagContext, p: Params, keys: { pyspark: string; spark: string }): SparkJobSpec {
  const tv = (s: string) => pyStr(templatedValue(s, ctx));
  const uris = (k: string) => pyList(list(p, k).map(tv));
  const args = list(p, "job_args");
  const entries: Array<[string, Py]> = [];
  let key: string;
  if (isPySpark(p)) {
    key = keys.pyspark;
    const uri = ctx.addScript(str(p, "script_name"), pysparkStarterScript(ctx.dagId), "code_bucket");
    entries.push(["main_python_file_uri", pyStr(uri)]);
    if (args.length) entries.push(["args", pyList(args.map(tv), { explode: true })]);
    if (list(p, "python_file_uris").length) entries.push(["python_file_uris", uris("python_file_uris")]);
    if (list(p, "pyspark_jar_file_uris").length) entries.push(["jar_file_uris", uris("pyspark_jar_file_uris")]);
  } else {
    key = keys.spark;
    entries.push(["main_class", pyStr(str(p, "main_class"))]);
    if (list(p, "jar_file_uris").length) entries.push(["jar_file_uris", uris("jar_file_uris")]);
    if (args.length) entries.push(["args", pyList(args.map(tv), { explode: true })]);
  }
  return { key, entries, properties: parseKeyValues(list(p, "spark_properties")) };
}

/**
 * Starter PySpark script. Static code: the only user value (dag_id) enters
 * through pyStr / pyDocstring / comment.
 */
export function pysparkStarterScript(dagId: string): string {
  return [
    comment(`Generated by ${GENERATOR_NAME} v${RUNTIME.generatorVersion} for DAG ${dagId}.`),
    comment("Upload to gs://<code_bucket>/scripts/<dag_id>/ (see README.md)."),
    pyDocstring(
      [
        `PySpark job for the Airflow DAG ${dagId}.`,
        "",
        "Starter job: replace transform() with your logic. Keep the command-line",
        "arguments so the DAG can pass run-specific paths.",
      ].join("\n"),
    ),
    "",
    "from __future__ import annotations",
    "",
    "import argparse",
    "import logging",
    "",
    "from pyspark.sql import DataFrame, SparkSession",
    "from pyspark.sql import functions as sf",
    "",
    assign("APP_NAME", pyStr(dagId)),
    "",
    "log = logging.getLogger(__name__)",
    "",
    "",
    "def parse_args(argv: list[str] | None = None) -> argparse.Namespace:",
    '    parser = argparse.ArgumentParser(description=f"PySpark job {APP_NAME}")',
    '    parser.add_argument("--input", required=True, help="gs:// path or glob to read")',
    '    parser.add_argument("--output", required=True, help="gs:// prefix to write")',
    '    parser.add_argument("--run-date", required=True, help="logical date, YYYY-MM-DD")',
    "    parser.add_argument(",
    '        "--input-format",',
    '        default="parquet",',
    '        choices=["parquet", "json", "csv"],',
    '        help="format of the input files",',
    "    )",
    "    return parser.parse_args(argv)",
    "",
    "",
    "def read_input(spark: SparkSession, path: str, fmt: str) -> DataFrame:",
    "    reader = spark.read.format(fmt)",
    '    if fmt == "csv":',
    '        reader = reader.option("header", "true").option("inferSchema", "true")',
    "    return reader.load(path)",
    "",
    "",
    "def transform(df: DataFrame, run_date: str) -> DataFrame:",
    '    """Example transformation: drop exact duplicates and stamp the run date."""',
    '    return df.dropDuplicates().withColumn("run_date", sf.lit(run_date).cast("date"))',
    "",
    "",
    "def main(argv: list[str] | None = None) -> None:",
    "    logging.basicConfig(level=logging.INFO)",
    "    args = parse_args(argv)",
    "    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()",
    "    try:",
    "        df = transform(read_input(spark, args.input, args.input_format), args.run_date)",
    "        # Dynamic partition overwrite replaces only this run's run_date partition,",
    "        # so re-running a date is idempotent.",
    '        writer = df.write.mode("overwrite").option("partitionOverwriteMode", "dynamic")',
    '        writer.partitionBy("run_date").parquet(args.output)',
    '        log.info("Wrote run_date=%s to %s", args.run_date, args.output)',
    "    finally:",
    "        spark.stop()",
    "",
    "",
    'if __name__ == "__main__":',
    "    main()",
    "",
  ].join("\n");
}

export const IMAGE_VERSIONS = ["2.2-debian12", "2.2-ubuntu22", "2.2-rocky9", "2.1-debian11"];
const AUTOSCALING_ID_RE = /^[A-Za-z0-9][A-Za-z0-9_-]{1,48}[A-Za-z0-9]$/;

// ---------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------

export const dataprocEphemeral: Scenario = {
  id: "dataproc_ephemeral",
  title: "Dataproc cluster job",
  description:
    "Create a short-lived Dataproc cluster, run a PySpark or Spark job on it, and always delete the cluster afterwards — with labels, a private network and a per-run cluster name.",
  naming: { source: TEAM.systemCodes.gcs, target: TEAM.systemCodes.dataproc },
  sections: [
    { id: "job", title: "Spark job" },
    { id: "cluster", title: "Cluster" },
    { id: "network", title: "Service account & network" },
    { id: "storage", title: "Buckets" },
  ],
  commonDefaults: {
    dag_id: "ml_gcs_to_dp_daily",
    description: "Build daily ML features with PySpark on an ephemeral Dataproc cluster.",
    domain: "ml",
    tags: ["ml", "dataproc"],
    location: "northamerica-northeast1",
    execution_timeout_minutes: 120,
  },
  fields: [
    ...sparkJobFields(() => dataprocEphemeral, "job", "ml"),
    // Cluster -----------------------------------------------------------------------------
    {
      key: "image_version", label: "Image version", type: "text", section: "cluster", default: "2.2-debian12",
      required: true, suggestions: IMAGE_VERSIONS,
      validate: (x) => (/^\d+\.\d+(\.\d+)?(-[a-z]+\d*)?$/.test(String(x)) ? v.ok() : v.err("Use a Dataproc image version such as 2.2-debian12.")),
      help: "Dataproc image (Spark/Python versions). 2.2 = Spark 3.5, Python 3.11.",
    },
    {
      key: "master_machine_type", label: "Master machine type", type: "text", section: "cluster", default: "n2-standard-4",
      required: true, suggestions: MACHINE_TYPES, validate: (x) => machineTypeIssues(String(x)),
    },
    {
      key: "worker_machine_type", label: "Worker machine type", type: "text", section: "cluster", default: "n2-standard-4",
      required: true, suggestions: MACHINE_TYPES, validate: (x) => machineTypeIssues(String(x)),
    },
    {
      key: "num_workers", label: "Primary workers", type: "number", section: "cluster", default: 2, min: 0, max: 500,
      help: "At least 2, or 0 for a single-node cluster (master only, for small jobs).",
    },
    {
      key: "num_secondary_workers", label: "Spot secondary workers", type: "number", section: "cluster", default: 0,
      min: 0, max: 1000, help: "Extra cheaper Spot VMs (compute only, no HDFS). They can be reclaimed at any time.",
    },
    {
      key: "boot_disk_size_gb", label: "Boot disk size (GB)", type: "number", section: "cluster", default: 100,
      min: 30, max: 4000, help: "Per node, pd-balanced.",
    },
    {
      key: "autoscaling_policy", label: "Autoscaling policy id (optional)", type: "text", section: "cluster", default: "",
      validate: (x) =>
        AUTOSCALING_ID_RE.test(String(x)) ? v.ok() : v.err("Policy ids are 3-50 letters, digits, '_' or '-', not starting/ending with '_' or '-'."),
      help: "Id of an existing autoscaling policy in the same project and region.",
    },
    {
      key: "idle_delete_minutes", label: "Idle auto-delete (minutes)", type: "number", section: "cluster", default: 60,
      min: 0, max: 20160,
      help: "Safety net: Dataproc deletes the cluster after this much idle time if the delete task never ran. 0 = off (minimum 10).",
    },
    // Network ------------------------------------------------------------------------------
    {
      key: "dataproc_service_account", label: "Cluster service account (optional)", type: "text", section: "network",
      default: "dataproc-runner@acme-dev-data.iam.gserviceaccount.com", envSpecific: true,
      validate: (x) => v.serviceAccount(String(x)),
      help: "Identity of the cluster VMs and the job (needs roles/dataproc.worker plus data access). Empty = Compute Engine default service account (not recommended). Env-specific.",
    },
    {
      key: "subnetwork", label: "Subnetwork (optional)", type: "text", section: "network", default: "", envSpecific: true,
      validate: (x, p) => subnetworkIssues(String(x), p, true),
      help: "Subnetwork name, or for Shared VPC the full URL https://www.googleapis.com/compute/v1/projects/HOST_PROJECT/regions/REGION/subnetworks/NAME. Empty = the default network. Env-specific.",
    },
    {
      key: "internal_ip_only", label: "Internal IPs only", type: "boolean", section: "network", default: true,
      help: "No public IPs on cluster VMs (the default from image 2.2). The subnetwork — or the default network's subnet in this region — needs Private Google Access.",
    },
    {
      key: "network_tags", label: "Network tags (optional)", type: "list", section: "network", default: [],
      validateItem: (x) => (/^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$/.test(x) ? v.ok() : v.err(`"${x}" is not a valid network tag.`)),
      help: "One per line; used by firewall rules.",
    },
    {
      key: "staging_bucket", label: "Staging bucket (optional)", type: "text", section: "storage", default: "",
      envSpecific: true, suggestable: true, validate: (x) => v.gcsBucket(String(x)),
      help: "Bucket name only, for cluster staging files and job driver output. Empty = a Dataproc-managed bucket. Env-specific.",
    },
  ],

  samples: {
    pyspark_features_daily: {
      dag_id: "ml_gcs_to_dp_daily",
      email: ["ml-alerts@example.com"],
      script_name: "build_features.py",
      job_args: [
        "--input=gs://{{ cfg.data_bucket }}/ml/events/dt={{ ds }}/",
        "--output=gs://{{ cfg.data_bucket }}/ml/features/",
        "--run-date={{ ds }}",
        "--input-format=json",
      ],
      spark_properties: ["spark.executor.memory=6g", "spark.sql.shuffle.partitions=64"],
      subnetwork: "projects/acme-dev-network/regions/northamerica-northeast1/subnetworks/acme-dev-dataproc",
      network_tags: ["dataproc"],
      staging_bucket: "acme-dev-ml-temp",
    },
    spark_jar_variables: {
      dag_id: "media_gcs_to_dp_weekly",
      description: "Weekly media catalogue rollup with a Scala Spark job.",
      domain: "media",
      config_mode: "variables",
      env: "stg",
      project_id: "acme-stg-data",
      job_type: "spark",
      main_class: "com.acme.media.CatalogRollup",
      jar_file_uris: ["gs://{{ cfg.code_bucket }}/jars/media-rollup-1.4.0.jar"],
      job_args: ["--run-date={{ ds }}", "--source=gs://{{ cfg.data_bucket }}/media/catalogue/"],
      code_bucket: "acme-stg-media-code",
      data_bucket: "acme-stg-media-landing",
      worker_machine_type: "n2-highmem-4",
      num_workers: 2,
      num_secondary_workers: 4,
      autoscaling_policy: "media-weekly-policy",
      idle_delete_minutes: 30,
      dataproc_service_account: "media-dataproc@acme-stg-data.iam.gserviceaccount.com",
      impersonation_chain: ["media-composer@acme-stg-data.iam.gserviceaccount.com"],
      schedule_preset: "@weekly",
      dagrun_timeout_minutes: 360,
      execution_timeout_minutes: 240,
      tags: ["media", "dataproc", "spark"],
    },
    pyspark_single_node_adhoc: {
      dag_id: "ml_gcs_to_dp_adhoc",
      description: "Ad-hoc PySpark backfill on a single-node cluster.",
      script_name: "backfill_labels.py",
      num_workers: 0,
      master_machine_type: "n2-standard-8",
      internal_ip_only: false,
      dataproc_service_account: "",
      idle_delete_minutes: 0,
      deferrable: false,
      schedule_preset: "none",
      retries: 0,
    },
  },

  validate(p) {
    const out = sparkJobValidate(p, "Dataproc");
    const add = (k: string, i: v.Issues) => {
      out[k] = out[k] ? v.merge(out[k], i) : i;
    };
    const workers = num(p, "num_workers");
    if (workers === 1) add("num_workers", v.err("Dataproc needs at least 2 primary workers, or 0 for a single-node cluster."));
    if (workers === 0 && num(p, "num_secondary_workers") > 0) {
      add("num_secondary_workers", v.err("Single-node clusters cannot have secondary workers."));
    }
    if (workers === 0 && str(p, "autoscaling_policy")) {
      add("autoscaling_policy", v.err("Single-node clusters cannot autoscale."));
    }
    const idle = num(p, "idle_delete_minutes");
    if (idle > 0 && idle < 10) add("idle_delete_minutes", v.err("The minimum idle time is 10 minutes (or 0 = off)."));
    if (idle === 0) add("idle_delete_minutes", v.warn("No idle auto-delete: if the delete task never runs, the cluster keeps running (and billing)."));
    return out;
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
    const ctx = new DagContext(dataprocEphemeral, p);
    const mod = "airflow.providers.google.cloud.operators.dataproc";
    ctx.importFrom(mod, "DataprocCreateClusterOperator");
    ctx.importFrom(mod, "DataprocSubmitJobOperator");
    ctx.importFrom(mod, "DataprocDeleteClusterOperator");
    ctx.importFrom("airflow.operators.empty", "EmptyOperator");
    ctx.importFrom("airflow.utils.task_group", "TaskGroup");
    ctx.importFrom("airflow.utils.trigger_rule", "TriggerRule");
    const has = (k: string) => ctx.envKeys().includes(k);

    // --- cluster config (google.cloud.dataproc_v1.ClusterConfig as dict) -------------------
    const workers = num(p, "num_workers");
    const disk = pyDict([
      ["boot_disk_type", pyStr("pd-balanced")],
      ["boot_disk_size_gb", pyInt(num(p, "boot_disk_size_gb"))],
    ]);
    const gce: Array<[string, Py]> = [];
    if (has("dataproc_service_account")) gce.push(["service_account", pyStr(ctx.ref("dataproc_service_account"))]);
    gce.push(["service_account_scopes", pyList([pyStr("https://www.googleapis.com/auth/cloud-platform")])]);
    if (has("subnetwork")) gce.push(["subnetwork_uri", pyStr(ctx.ref("subnetwork"))]);
    gce.push(["internal_ip_only", pyBool(bool(p, "internal_ip_only"))]);
    if (list(p, "network_tags").length) gce.push(["tags", pyList(list(p, "network_tags").map(pyStr))]);

    const software: Array<[string, Py]> = [["image_version", pyStr(str(p, "image_version"))]];
    if (workers === 0) software.push(["properties", pyDict([["dataproc:dataproc.allow.zero.workers", pyStr("true")]])]);

    const cfg: Array<[string, Py]> = [];
    if (has("staging_bucket")) cfg.push(["config_bucket", pyStr(ctx.ref("staging_bucket"))]);
    cfg.push(
      ["gce_cluster_config", pyDict(gce, { explode: true })],
      [
        "master_config",
        pyDict(
          [
            ["num_instances", pyInt(1)],
            ["machine_type_uri", pyStr(str(p, "master_machine_type"))],
            ["disk_config", disk],
          ],
          { explode: true },
        ),
      ],
    );
    if (workers > 0) {
      cfg.push([
        "worker_config",
        pyDict(
          [
            ["num_instances", pyInt(workers)],
            ["machine_type_uri", pyStr(str(p, "worker_machine_type"))],
            ["disk_config", disk],
          ],
          { explode: true },
        ),
      ]);
      if (num(p, "num_secondary_workers") > 0) {
        cfg.push([
          "secondary_worker_config",
          pyDict(
            [
              ["num_instances", pyInt(num(p, "num_secondary_workers"))],
              ["preemptibility", pyStr("SPOT")],
              ["disk_config", disk],
            ],
            { explode: true },
          ),
        ]);
      }
    }
    cfg.push(["software_config", pyDict(software, { explode: true })]);
    if (workers > 0 && str(p, "autoscaling_policy")) {
      const policy = `projects/${ctx.ref("project_id")}/regions/${str(p, "location")}/autoscalingPolicies/${str(p, "autoscaling_policy")}`;
      cfg.push(["autoscaling_config", pyDict([["policy_uri", pyStr(policy)]])]);
    }
    if (num(p, "idle_delete_minutes") > 0) {
      cfg.push(["lifecycle_config", pyDict([["idle_delete_ttl", pyDict([["seconds", pyInt(num(p, "idle_delete_minutes") * 60)]])]])]);
    }

    // --- job ------------------------------------------------------------------------------------
    const spec = sparkJobSpec(ctx, p, { pyspark: "pyspark_job", spark: "spark_job" });
    if (spec.properties.length) {
      spec.entries.push(["properties", pyDict(spec.properties.map(([k, val]): [string, Py] => [k, pyStr(val)]), { explode: true })]);
    }
    const job = pyDict(
      [
        ["placement", pyDict([["cluster_name", pyName("CLUSTER_NAME")]])],
        [spec.key, pyDict(spec.entries, { explode: true })],
        ["labels", pyName("LABELS")],
      ],
      { explode: true },
    );

    const common = (): Array<[string, Py]> => [
      ["project_id", pyStr(ctx.ref("project_id"))],
      ["region", pyName("LOCATION")],
    ];
    const auth = (): Array<[string, Py]> => [
      ["gcp_conn_id", pyName("GCP_CONN_ID")],
      ["impersonation_chain", ctx.impersonationChain()],
      ["deferrable", ctx.deferrable()],
    ];
    const submitId = isPySpark(p) ? "submit_pyspark_job" : "submit_spark_job";
    const createTask = pyCall(
      "DataprocCreateClusterOperator",
      [],
      [
        ["task_id", pyStr("create_cluster")],
        ...common(),
        ["cluster_name", pyName("CLUSTER_NAME")],
        ["cluster_config", pyName("CLUSTER_CONFIG")],
        ["labels", pyName("LABELS")],
        ["use_if_exists", pyBool(true)],
        ["delete_on_error", pyBool(true)],
        ...auth(),
      ],
      { explode: true },
    );
    const submitTask = pyCall(
      "DataprocSubmitJobOperator",
      [],
      [["task_id", pyStr(submitId)], ...common(), ["job", job], ...auth()],
      { explode: true },
    );
    const deleteTask = pyCall(
      "DataprocDeleteClusterOperator",
      [],
      [
        ["task_id", pyStr("delete_cluster")],
        ...common(),
        ["cluster_name", pyName("CLUSTER_NAME")],
        ["trigger_rule", pyName("TriggerRule.ALL_DONE")],
        ...auth(),
      ],
      { explode: true },
    );
    const endTask = pyCall("EmptyOperator", [], [["task_id", pyStr("end_pipeline")]]);

    const constants = [
      comment("Cluster name: dag_id + run timestamp, unique per run (rendered at run time; max 51 chars)."),
      assign("CLUSTER_NAME", pyStr(ctx.resourceName({ suffix: "ts", maxLength: 51 }))),
      "",
      comment("google.cloud.dataproc_v1.ClusterConfig. Cluster labels are passed separately (labels=LABELS)."),
      assign("CLUSTER_CONFIG", pyDict(cfg, { explode: true })),
    ];

    const body = [
      withHeader(
        pyCall("TaskGroup", [], [
          ["group_id", pyStr("ephemeral_cluster")],
          ["tooltip", pyStr("Create a cluster, run the job, always delete the cluster")],
        ]),
        null,
        1,
      ),
      assign("create_cluster", createTask, 2),
      "",
      assign(pyIdent(submitId), submitTask, 2),
      "",
      assign("delete_cluster", deleteTask, 2),
      "",
      `        create_cluster >> ${submitId} >> delete_cluster`,
      "",
      comment("Fails the DAG run when the job fails: delete_cluster (all_done) succeeds either way.", 1),
      assign("end_pipeline", endTask, 1),
      "",
      `    ${submitId} >> end_pipeline`,
    ];

    const what = isPySpark(p) ? `PySpark script \`scripts/<dag_id>/${str(p, "script_name")}\`` : `Spark main class \`${str(p, "main_class")}\``;
    const shape = workers === 0 ? "a single-node" : `a ${workers}-worker${num(p, "num_secondary_workers") > 0 ? ` (+${num(p, "num_secondary_workers")} Spot)` : ""}`;
    const docSteps = [
      `\`ephemeral_cluster.create_cluster\`: create ${shape} Dataproc ${str(p, "image_version")} cluster \`<dag_id>-<run timestamp>\` in ${str(p, "location")} (reused if a retry finds it).`,
      `\`ephemeral_cluster.${submitId}\`: run the ${what} on it.`,
      "`ephemeral_cluster.delete_cluster`: delete the cluster, whether the job succeeded or not (trigger_rule=all_done).",
      "`end_pipeline`: marks the DAG run failed when the job failed.",
    ];

    const iamRoles = [
      "`roles/dataproc.editor` on the project (create/delete clusters, submit jobs)",
      "`roles/iam.serviceAccountUser` on the cluster service account",
      "Cluster service account: `roles/dataproc.worker`, `roles/storage.objectViewer` on the code bucket, plus access to the job's data (e.g. `roles/storage.objectAdmin` on the data bucket, BigQuery roles)",
    ];
    if (has("subnetwork")) iamRoles.push("Shared VPC: `roles/compute.networkUser` on the subnetwork for the Dataproc service agent");
    if (list(p, "impersonation_chain").length) {
      iamRoles.push("`roles/iam.serviceAccountTokenCreator` for the Composer service account on the impersonated service account");
    }

    return ctx.assemble({
      docSteps,
      constants,
      body,
      iamRoles,
      readmeNotes: [
        ...(isPySpark(p)
          ? [`Replace the starter logic in \`scripts/${ctx.dagId}/${str(p, "script_name")}\` with your own, then upload it to the code bucket of every env before the first run.`]
          : []),
        "The cluster name is `<dag_id>-<run timestamp>`: retries of a run reuse the same cluster (use_if_exists), and different runs never share one.",
        "Labels (env, team, owner, cost_center, dag_id) are set on the cluster (and so on its VMs and disks) and on the job.",
        ...(num(p, "idle_delete_minutes") > 0
          ? [`If the delete task cannot run, Dataproc deletes the cluster after ${num(p, "idle_delete_minutes")} idle minutes.`]
          : ["Idle auto-delete is off: check for leftover clusters if a delete task fails."]),
        ...(bool(p, "deferrable")
          ? ["Operators run in deferrable mode: make sure the Composer environment has a triggerer (Composer 2: set triggerer count >= 1)."]
          : []),
      ],
    });
  },
};
