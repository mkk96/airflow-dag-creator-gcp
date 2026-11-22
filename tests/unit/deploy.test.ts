import { spawnSync } from "node:child_process";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";
import { buildPlan, defaultTarget, type DeployTarget, parseGcsPrefix, validateTarget } from "../../src/lib/deploy/plan";
import { renderDeployScript, shq, toBase64 } from "../../src/lib/deploy/script";
import { sampleParams } from "../../src/scenarios/common";
import { getScenario } from "../../src/scenarios/registry";

function bundleFor(scenarioId: string, sample: string) {
  const s = getScenario(scenarioId)!;
  return s.generate(sampleParams(s, sample));
}

function target(overrides: Partial<DeployTarget> = {}, bundle = bundleFor("gcs_to_bigquery", "csv_daily_partitioned")): DeployTarget {
  return {
    ...defaultTarget(bundle),
    project: "acme-prd-composer",
    region: "northamerica-northeast1",
    environment: "composer-prd",
    targetEnv: "prd",
    ...overrides,
  };
}

describe("deploy plan", () => {
  it("uploads SQL before the DAG and the DAG last", () => {
    const b = bundleFor("gcs_to_bigquery", "csv_daily_partitioned");
    const plan = buildPlan(b, target({}, b));
    const uploads = plan.filter((s) => s.kind === "upload");
    expect(uploads.at(-1)).toMatchObject({ dest: "dags", relPath: `${b.dagId}.py` });
    expect(uploads.some((s) => s.kind === "upload" && s.dest === "dags" && s.relPath.startsWith("sql/"))).toBe(true);
    expect(plan.some((s) => s.kind === "upload" && s.dest === "dags" && s.relPath.endsWith("README.md"))).toBe(false);
  });

  it("sets the target env's Airflow Variable in Variables mode", () => {
    const b = bundleFor("gcs_to_bigquery", "json_explicit_schema_variables");
    const plan = buildPlan(b, target({ targetEnv: "stg", setVariable: true }, b));
    const v = plan.find((s) => s.kind === "variable");
    expect(v).toMatchObject({ name: `stg_${b.dagId}` });
    expect(JSON.parse(v!.kind === "variable" ? v!.value : "{}").project_id).toBe("acme-stg-data");
  });

  it("uploads scripts to the code bucket for Dataproc", () => {
    const b = bundleFor("dataproc_ephemeral", "pyspark_features_daily");
    expect(b.deploy.codeBuckets).not.toBeNull();
    const t = target({ uploadScripts: true, codeBucket: "acme-prd-ml-code" }, b);
    const scripts = buildPlan(b, t).filter((s) => s.kind === "upload" && s.dest === "code");
    expect(scripts.length).toBeGreaterThan(0);
    expect(scripts[0]).toMatchObject({ bucket: "acme-prd-ml-code" });
  });

  it("validates the target", () => {
    const b = bundleFor("gcs_to_bigquery", "csv_daily_partitioned");
    expect(validateTarget(target({}, b), b)).toEqual({});
    const bad = validateTarget(target({ project: "X", region: "US", environment: "Bad Name", targetEnv: "qa" }, b), b);
    expect(Object.keys(bad).sort()).toEqual(["environment", "project", "region", "targetEnv"]);
  });

  it("parses the DAG folder URI", () => {
    expect(parseGcsPrefix("gs://us-central1-env-1234-bucket/dags")).toEqual({ bucket: "us-central1-env-1234-bucket", prefix: "dags" });
  });
});

describe("shell safety helpers", () => {
  it("single-quotes values safely", () => {
    expect(shq("it's $(rm -rf /)")).toBe(`'it'\\''s $(rm -rf /)'`);
  });
  it("base64-encodes UTF-8", () => {
    expect(Buffer.from(toBase64("héllo → 東京").replace(/\n/g, ""), "base64").toString("utf8")).toBe("héllo → 東京");
  });
  it("rejects unsafe dag ids", () => {
    const b = bundleFor("gcs_to_bigquery", "csv_daily_partitioned");
    expect(() => renderDeployScript({ ...b, dagId: 'x"; rm -rf /' }, target({}, b))).toThrow();
  });
});

// ---------------------------------------------------------------------------
// Execute the generated script against a fake `gcloud` that records everything.
// ---------------------------------------------------------------------------

const FAKE_GCLOUD = `#!/usr/bin/env bash
set -euo pipefail
echo "$*" >> "$FAKE_ROOT/calls.log"
gs_to_local() { local p="\${1#gs://}"; echo "$FAKE_ROOT/gcs/$p"; }
if [ "$1 $2" = "composer environments" ]; then
  case "$3" in
    describe)
      fmt=""; for a in "$@"; do case "$a" in --format=*) fmt="\${a#--format=}";; esac; done
      case "$fmt" in
        *dagGcsPrefix*) echo "gs://fake-composer-bucket/dags" ;;
        *airflowUri*) echo "https://airflow.example.com" ;;
        *envVariables.DEPLOY_ENV*) echo "\${FAKE_DEPLOY_ENV:-}" ;;
      esac ;;
    run)
      args=("$@"); for i in "\${!args[@]}"; do if [ "\${args[$i]}" = "--" ]; then name="\${args[$((i+1))]}"; value="\${args[$((i+2))]}"; fi; done
      mkdir -p "$FAKE_ROOT/vars"; printf '%s' "$value" > "$FAKE_ROOT/vars/$name" ;;
    update) echo "update $*" >> "$FAKE_ROOT/updates.log" ;;
  esac
elif [ "$1 $2" = "storage cp" ]; then
  shift 2; rec=""; if [ "$1" = "--recursive" ]; then rec="-r"; shift; fi
  src="$1"; dst="$(gs_to_local "$2")"
  if [ -n "$rec" ]; then mkdir -p "$dst"; cp -r "$src" "$dst"; else mkdir -p "$(dirname "$dst")"; cp "$src" "$dst"; fi
fi
`;

const bashOk = spawnSync("bash", ["-c", "command -v base64 >/dev/null && echo ok"], { encoding: "utf8" }).stdout?.trim() === "ok";
const roots: string[] = [];
afterAll(() => roots.forEach((r) => rmSync(r, { recursive: true, force: true })));

function runScript(script: string, env: Record<string, string> = {}) {
  const root = mkdtempSync(join(tmpdir(), "deploy-test-"));
  roots.push(root);
  const bin = join(root, "bin");
  mkdirSync(bin);
  writeFileSync(join(bin, "gcloud"), FAKE_GCLOUD.replace(/\r/g, ""));
  chmodSync(join(bin, "gcloud"), 0o755);
  writeFileSync(join(root, "deploy.sh"), script);
  const toPosix = (p: string) => p.replace(/\\/g, "/");
  const r = spawnSync("bash", [toPosix(join(root, "deploy.sh"))], {
    encoding: "utf8",
    env: {
      ...process.env,
      FAKE_ROOT: toPosix(root),
      ...env,
      PATH: `${bin}${process.platform === "win32" ? ";" : ":"}${process.env.PATH}`,
    },
  });
  return { root, ...r };
}

describe.skipIf(!bashOk)("generated Cloud Shell script (executed with a fake gcloud)", () => {
  it("uploads every file byte-for-byte and sets the Variable", () => {
    const b = bundleFor("gcs_to_bigquery", "json_explicit_schema_variables");
    const t = target({ targetEnv: "prd", setVariable: true }, b);
    const r = runScript(renderDeployScript(b, t), { FAKE_DEPLOY_ENV: "prd" });
    expect(r.status, r.stderr).toBe(0);
    for (const f of b.files.filter((x) => x.role === "dag" || x.role === "sql")) {
      const local = join(r.root, "gcs", "fake-composer-bucket", f.path);
      expect(existsSync(local), f.path).toBe(true);
      expect(readFileSync(local, "utf8")).toBe(f.content);
    }
    const varFile = join(r.root, "vars", `prd_${b.dagId}`);
    expect(JSON.parse(readFileSync(varFile, "utf8"))).toEqual(b.deploy.variables!.prd.value);
    expect(r.stdout).toContain(`https://airflow.example.com/dags/${b.dagId}/grid`);
  });

  it("uploads Dataproc scripts to the code bucket", () => {
    const b = bundleFor("dataproc_ephemeral", "pyspark_features_daily");
    const t = target({ uploadScripts: true, codeBucket: "acme-prd-ml-code" }, b);
    const r = runScript(renderDeployScript(b, t), { FAKE_DEPLOY_ENV: "prd" });
    expect(r.status, r.stderr).toBe(0);
    for (const f of b.files.filter((x) => x.role === "script")) {
      expect(readFileSync(join(r.root, "gcs", "acme-prd-ml-code", f.path), "utf8")).toBe(f.content);
    }
  });

  it("refuses to deploy when DEPLOY_ENV doesn't match, unless asked to set it", () => {
    const b = bundleFor("gcs_to_bigquery", "csv_daily_partitioned");
    const refused = runScript(renderDeployScript(b, target({ setDeployEnv: false }, b)), { FAKE_DEPLOY_ENV: "dev" });
    expect(refused.status).not.toBe(0);
    expect(refused.stderr).toContain("DEPLOY_ENV is 'dev'");
    expect(existsSync(join(refused.root, "gcs"))).toBe(false);

    const fixed = runScript(renderDeployScript(b, target({ setDeployEnv: true }, b)), { FAKE_DEPLOY_ENV: "" });
    expect(fixed.status, fixed.stderr).toBe(0);
    expect(readFileSync(join(fixed.root, "updates.log"), "utf8")).toContain("--update-env-variables=DEPLOY_ENV=prd");
  });
});

// ---------------------------------------------------------------------------
// "Deploy now": browser client against a mocked fetch.
// ---------------------------------------------------------------------------

import { vi } from "vitest";
import { deploy } from "../../src/lib/deploy/gcp";

function mockGoogle(deployEnv: string | undefined) {
  const calls: { method: string; url: string; body: string }[] = [];
  const envName = "projects/acme-prd-composer/locations/northamerica-northeast1/environments/composer-prd";
  const fetchMock = vi.fn(async (url: string, init: RequestInit = {}) => {
    const method = init.method ?? "GET";
    calls.push({ method, url, body: String(init.body ?? "") });
    const json = (o: unknown) => new Response(JSON.stringify(o), { status: 200 });
    if (method === "GET" && url.endsWith(envName)) {
      return json({
        name: envName,
        state: "RUNNING",
        config: {
          dagGcsPrefix: "gs://env-bucket/dags",
          airflowUri: "https://airflow.example.com",
          softwareConfig: { imageVersion: "composer-2.9.0-airflow-2.10.5", envVariables: { KEEP_ME: "1", ...(deployEnv ? { DEPLOY_ENV: deployEnv } : {}) } },
        },
      });
    }
    if (url.includes(":executeAirflowCommand")) return json({ executionId: "e1", pod: "p", podNamespace: "ns" });
    if (url.includes(":pollAirflowCommand")) return json({ output: [{ lineNumber: 1, content: "ok" }], outputEnd: true, exitInfo: { exitCode: 0 } });
    if (method === "PATCH") return json({ name: "operations/op-1" });
    if (url.startsWith("https://storage.googleapis.com/upload/")) return json({});
    return new Response("{}", { status: 404 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return calls;
}

describe("Deploy now (browser client)", () => {
  it("checks the environment, sets the Variable, and uploads SQL then the DAG", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true, advanceTimeDelta: 1000 });
    const calls = mockGoogle("prd");
    const b = bundleFor("gcs_to_bigquery", "json_explicit_schema_variables");
    const logs: string[] = [];
    const r = await deploy("tok", b, target({ setVariable: true }, b), (_l, t) => logs.push(t));
    vi.useRealTimers();
    vi.unstubAllGlobals();

    const exec = calls.find((c) => c.url.includes(":executeAirflowCommand"))!;
    const body = JSON.parse(exec.body);
    expect(body).toMatchObject({ command: "variables", subcommand: "set" });
    expect(body.parameters[0]).toBe(`prd_${b.dagId}`);
    expect(JSON.parse(body.parameters[1])).toEqual(b.deploy.variables!.prd.value);
    const uploads = calls.filter((c) => c.url.startsWith("https://storage.googleapis.com/upload/"));
    const names = uploads.map((u) => decodeURIComponent(new URL(u.url).searchParams.get("name")!));
    expect(names.at(-1)).toBe(`dags/${b.dagId}.py`);
    expect(names.some((n) => n.startsWith(`dags/sql/${b.dagId}/`))).toBe(true);
    expect(uploads.every((u) => u.url.includes("/b/env-bucket/o?uploadType=media"))).toBe(true);
    expect(calls.some((c) => c.method === "PATCH")).toBe(false);
    expect(r.dagUrl).toBe(`https://airflow.example.com/dags/${b.dagId}/grid`);
  });

  it("refuses a DEPLOY_ENV mismatch, or merges it into existing env vars when asked", async () => {
    const b = bundleFor("gcs_to_bigquery", "csv_daily_partitioned");
    mockGoogle("dev");
    await expect(deploy("tok", b, target({ setDeployEnv: false }, b), () => {})).rejects.toThrow(/DEPLOY_ENV/);
    const calls = mockGoogle(undefined);
    await deploy("tok", b, target({ setDeployEnv: true }, b), () => {});
    vi.unstubAllGlobals();
    const patch = calls.find((c) => c.method === "PATCH")!;
    expect(patch.url).toContain("updateMask=config.softwareConfig.envVariables");
    expect(JSON.parse(patch.body).config.softwareConfig.envVariables).toEqual({ KEEP_ME: "1", DEPLOY_ENV: "prd" });
  });
});
