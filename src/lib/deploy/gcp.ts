/**
 * Minimal browser client for the Cloud Composer and Cloud Storage REST APIs,
 * authenticated with the user's own OAuth access token. Nothing is proxied:
 * requests go straight from this tab to *.googleapis.com.
 */
import { buildPlan, type DeployStep, type DeployTarget, parseGcsPrefix } from "./plan";
import type { GeneratedBundle } from "../../scenarios/types";

const COMPOSER = "https://composer.googleapis.com/v1";
const STORAGE_UPLOAD = "https://storage.googleapis.com/upload/storage/v1";

export class GcpError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
  }
}

const HINTS: Record<number, string> = {
  401: "Your Google session expired — sign in again.",
  403: "Permission denied. You need roles/composer.environmentAndStorageObjectAdmin (or composer.user + storage.objectAdmin on the bucket).",
  404: "Not found — check the project, region and environment name.",
};

async function call<T>(token: string, url: string, init: RequestInit = {}): Promise<T> {
  const res = await fetch(url, {
    ...init,
    headers: { Authorization: `Bearer ${token}`, ...(init.headers ?? {}) },
  });
  const text = await res.text();
  if (!res.ok) {
    let detail = text;
    try {
      detail = JSON.parse(text)?.error?.message ?? text;
    } catch {
      /* not JSON */
    }
    throw new GcpError(`${HINTS[res.status] ?? `Request failed (${res.status}).`} ${detail}`.trim(), res.status);
  }
  return (text ? JSON.parse(text) : {}) as T;
}

export interface ComposerEnvironment {
  name: string;
  state: string;
  dagGcsPrefix: string;
  airflowUri: string;
  imageVersion: string;
  envVariables: Record<string, string>;
}

interface RawEnvironment {
  name: string;
  state?: string;
  config?: {
    dagGcsPrefix?: string;
    airflowUri?: string;
    softwareConfig?: { imageVersion?: string; envVariables?: Record<string, string> };
  };
}

export function environmentName(t: Pick<DeployTarget, "project" | "region" | "environment">): string {
  return `projects/${t.project}/locations/${t.region}/environments/${t.environment}`;
}

export async function getEnvironment(token: string, t: DeployTarget): Promise<ComposerEnvironment> {
  const raw = await call<RawEnvironment>(token, `${COMPOSER}/${environmentName(t)}`);
  return {
    name: raw.name,
    state: raw.state ?? "UNKNOWN",
    dagGcsPrefix: raw.config?.dagGcsPrefix ?? "",
    airflowUri: raw.config?.airflowUri ?? "",
    imageVersion: raw.config?.softwareConfig?.imageVersion ?? "",
    envVariables: raw.config?.softwareConfig?.envVariables ?? {},
  };
}

export async function listEnvironments(token: string, project: string, region: string): Promise<string[]> {
  const res = await call<{ environments?: RawEnvironment[] }>(
    token,
    `${COMPOSER}/projects/${encodeURIComponent(project)}/locations/${encodeURIComponent(region)}/environments?pageSize=100`,
  );
  return (res.environments ?? []).map((e) => e.name.split("/").pop() ?? e.name);
}

export async function uploadObject(token: string, bucket: string, object: string, content: string, contentType: string) {
  const url = `${STORAGE_UPLOAD}/b/${encodeURIComponent(bucket)}/o?uploadType=media&name=${encodeURIComponent(object)}`;
  await call(token, url, { method: "POST", headers: { "Content-Type": contentType }, body: content });
}

interface ExecResponse {
  executionId: string;
  pod: string;
  podNamespace: string;
  error?: string;
}
interface PollResponse {
  output?: { lineNumber: number; content: string }[];
  outputEnd?: boolean;
  exitInfo?: { exitCode?: number; error?: string };
}

/** Run an Airflow CLI command through the Composer API and wait for it to finish. */
export async function runAirflowCommand(
  token: string,
  envName: string,
  command: string,
  subcommand: string,
  parameters: string[],
  timeoutMs = 180_000,
): Promise<string[]> {
  const exec = await call<ExecResponse>(token, `${COMPOSER}/${envName}:executeAirflowCommand`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ command, subcommand, parameters }),
  });
  if (exec.error) throw new GcpError(exec.error, 400);
  const lines: string[] = [];
  let next = 1;
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 2000));
    const poll = await call<PollResponse>(token, `${COMPOSER}/${envName}:pollAirflowCommand`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ executionId: exec.executionId, pod: exec.pod, podNamespace: exec.podNamespace, nextLineNumber: next }),
    });
    for (const l of poll.output ?? []) {
      lines.push(l.content);
      next = Math.max(next, l.lineNumber + 1);
    }
    if (poll.outputEnd) {
      const code = poll.exitInfo?.exitCode ?? 0;
      if (code !== 0) throw new GcpError(`airflow ${command} ${subcommand} failed (exit ${code}): ${poll.exitInfo?.error ?? lines.slice(-3).join(" ")}`, 500);
      return lines;
    }
  }
  throw new GcpError(`airflow ${command} ${subcommand} did not finish within ${Math.round(timeoutMs / 1000)}s`, 504);
}

/** Merge one env variable into the environment (Composer replaces the whole map). Returns the operation name. */
export async function setEnvVariable(token: string, env: ComposerEnvironment, name: string, value: string): Promise<string> {
  const merged = { ...env.envVariables, [name]: value };
  const op = await call<{ name: string }>(
    token,
    `${COMPOSER}/${env.name}?updateMask=config.softwareConfig.envVariables`,
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ config: { softwareConfig: { envVariables: merged } } }),
    },
  );
  return op.name;
}

export type LogLevel = "info" | "ok" | "warn" | "error";
export type Logger = (level: LogLevel, text: string) => void;

export interface DeployResult {
  airflowUri: string;
  dagUrl: string;
  envUpdateOperation: string | null;
}

/** Execute the deployment: check environment → DEPLOY_ENV → Variable → scripts → SQL → DAG. */
export async function deploy(token: string, bundle: GeneratedBundle, t: DeployTarget, log: Logger): Promise<DeployResult> {
  const envVar = bundle.deploy.deployEnvVar;
  log("info", `Checking ${environmentName(t)}…`);
  const env = await getEnvironment(token, t);
  if (env.state !== "RUNNING") throw new GcpError(`Environment state is ${env.state}; wait until it is RUNNING.`, 409);
  log("ok", `Environment is RUNNING (${env.imageVersion}). DAG folder: ${env.dagGcsPrefix}`);

  let envUpdateOperation: string | null = null;
  const current = env.envVariables[envVar];
  if (current !== t.targetEnv) {
    if (!t.setDeployEnv) {
      throw new GcpError(
        `${envVar} on this environment is "${current ?? "not set"}" but you are deploying for "${t.targetEnv}". Tick "Set ${envVar}" or pick the matching target env.`,
        412,
      );
    }
    envUpdateOperation = await setEnvVariable(token, env, envVar, t.targetEnv);
    log("warn", `Started setting ${envVar}=${t.targetEnv}; Airflow restarts over the next 10–25 min. New DAGs start paused — unpause after it finishes.`);
  } else {
    log("ok", `${envVar}=${current} already set.`);
  }

  const { bucket, prefix } = parseGcsPrefix(env.dagGcsPrefix);
  const steps: DeployStep[] = buildPlan(bundle, t);
  for (const s of steps) {
    if (s.kind === "variable") {
      log("info", `${s.label}…`);
      await runAirflowCommand(token, env.name, "variables", "set", [s.name, s.value]);
      log("ok", `${s.label} ✓`);
    } else if (s.dest === "code") {
      await uploadObject(token, s.bucket, s.object, s.content, s.contentType);
      log("ok", `Uploaded gs://${s.bucket}/${s.object}`);
    } else {
      const object = prefix ? `${prefix}/${s.relPath}` : s.relPath;
      await uploadObject(token, bucket, object, s.content, s.contentType);
      log("ok", `Uploaded gs://${bucket}/${object}`);
    }
  }
  const dagUrl = env.airflowUri ? `${env.airflowUri.replace(/\/+$/, "")}/dags/${encodeURIComponent(bundle.dagId)}/grid` : "";
  log("ok", `Deployed ${bundle.dagId}. It appears in Airflow within 1–2 minutes (paused by default).`);
  return { airflowUri: env.airflowUri, dagUrl, envUpdateOperation };
}
