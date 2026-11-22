/**
 * Deployment plan shared by "Deploy now" (browser → Google APIs) and the
 * Cloud Shell script. Pure functions: no network, no DOM.
 */
import { ENVIRONMENTS, OFFICIAL } from "../../config/naming";
import type { GeneratedBundle } from "../../scenarios/types";
import { gcsBucket } from "../../validation/rules";

export interface DeployTarget {
  /** Project that hosts the Composer environment. */
  project: string;
  /** Composer region, e.g. northamerica-northeast1. */
  region: string;
  /** Composer environment name. */
  environment: string;
  /** dev / stg / prd: the value DEPLOY_ENV must have on that environment. */
  targetEnv: string;
  /** Set the env-prefixed Airflow Variable (Variables mode). */
  setVariable: boolean;
  /** Upload scripts/<dag_id>/ to the code bucket. */
  uploadScripts: boolean;
  codeBucket: string;
  /** Set DEPLOY_ENV on the environment when it is missing or different (restarts Airflow). */
  setDeployEnv: boolean;
}

export type DeployStep =
  | { kind: "variable"; label: string; name: string; value: string }
  | { kind: "upload"; label: string; dest: "dags"; relPath: string; content: string; contentType: string }
  | { kind: "upload"; label: string; dest: "code"; bucket: string; object: string; content: string; contentType: string };

export const COMPOSER_REGIONS = [
  "northamerica-northeast1", "northamerica-northeast2", "us-central1", "us-east1", "us-east4",
  "us-west1", "us-west2", "europe-west1", "europe-west2", "europe-west3", "europe-west4",
  "asia-northeast1", "asia-south1", "asia-southeast1", "australia-southeast1",
];

const CONTENT_TYPES: Record<string, string> = {
  py: "text/x-python",
  sql: "application/sql",
  json: "application/json",
  md: "text/markdown",
};

function contentType(path: string): string {
  return CONTENT_TYPES[path.split(".").pop() ?? ""] ?? "text/plain";
}

export function defaultTarget(bundle: GeneratedBundle): DeployTarget {
  const env = bundle.deploy.formEnv;
  return {
    project: "",
    region: "",
    environment: "",
    targetEnv: env,
    setVariable: bundle.deploy.configMode === "variables",
    uploadScripts: !!bundle.deploy.codeBuckets,
    codeBucket: bundle.deploy.codeBuckets?.[env] ?? "",
    setDeployEnv: false,
  };
}

/** Field-level problems with the deploy target (empty object = OK). */
export function validateTarget(t: DeployTarget, bundle: GeneratedBundle): Record<string, string> {
  const out: Record<string, string> = {};
  if (!OFFICIAL.bqProject.re.test(t.project)) out.project = "Enter the project ID that hosts the Composer environment.";
  if (!/^[a-z]+(-[a-z]+)+\d+$/.test(t.region)) out.region = "Enter a Composer region, e.g. northamerica-northeast1.";
  if (!/^[a-z]([-a-z0-9]{0,62}[a-z0-9])?$/.test(t.environment)) {
    out.environment = "Environment names are lowercase letters, digits and hyphens (start with a letter).";
  }
  if (!(ENVIRONMENTS as readonly string[]).includes(t.targetEnv)) out.targetEnv = "Pick dev, stg or prd.";
  if (bundle.deploy.codeBuckets && t.uploadScripts) {
    const b = gcsBucket(t.codeBucket);
    if (b.errors.length) out.codeBucket = b.errors[0];
  }
  return out;
}

/** Ordered steps: Variable → scripts → SQL → DAG (last, so it never appears without its SQL). */
export function buildPlan(bundle: GeneratedBundle, t: DeployTarget): DeployStep[] {
  const steps: DeployStep[] = [];
  const v = bundle.deploy.variables?.[t.targetEnv];
  if (v && t.setVariable) {
    steps.push({ kind: "variable", label: `Set Airflow Variable ${v.name}`, name: v.name, value: JSON.stringify(v.value) });
  }
  if (bundle.deploy.codeBuckets && t.uploadScripts) {
    for (const f of bundle.files.filter((x) => x.role === "script")) {
      steps.push({
        kind: "upload",
        label: `Upload gs://${t.codeBucket}/${f.path}`,
        dest: "code",
        bucket: t.codeBucket,
        object: f.path,
        content: f.content,
        contentType: contentType(f.path),
      });
    }
  }
  const dagFiles = bundle.files.filter((x) => x.role === "sql").concat(bundle.files.filter((x) => x.role === "dag"));
  for (const f of dagFiles) {
    const relPath = f.path.replace(/^dags\//, "");
    steps.push({
      kind: "upload",
      label: `Upload <dags bucket>/dags/${relPath}`,
      dest: "dags",
      relPath,
      content: f.content,
      contentType: contentType(f.path),
    });
  }
  return steps;
}

/** gs://bucket/some/prefix → { bucket, prefix } */
export function parseGcsPrefix(uri: string): { bucket: string; prefix: string } {
  const m = /^gs:\/\/([^/]+)\/?(.*)$/.exec(uri.trim());
  if (!m) throw new Error(`Unexpected DAG folder URI: ${uri}`);
  return { bucket: m[1], prefix: m[2].replace(/\/+$/, "") };
}
