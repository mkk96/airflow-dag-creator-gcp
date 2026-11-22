/** Export / import of form configuration as JSON. */
import { RUNTIME } from "../config/runtime";
import { allFields, defaultParams } from "../scenarios/common";
import { getScenario } from "../scenarios/registry";
import type { FieldSchema, ParamValue, Params, Scenario } from "../scenarios/types";

export const CONFIG_KIND = "airflow-dag-creator-gcp/config";

export interface ExportedConfig {
  kind: typeof CONFIG_KIND;
  generatorVersion: string;
  scenario: string;
  params: Params;
}

export function exportConfig(scenario: Scenario, params: Params): string {
  const out: ExportedConfig = {
    kind: CONFIG_KIND,
    generatorVersion: RUNTIME.generatorVersion,
    scenario: scenario.id,
    params,
  };
  return JSON.stringify(out, null, 2) + "\n";
}

function coerce(f: FieldSchema, raw: unknown): ParamValue | undefined {
  switch (f.type) {
    case "boolean":
      return typeof raw === "boolean" ? raw : undefined;
    case "number":
      return typeof raw === "number" && Number.isFinite(raw) ? raw : undefined;
    case "list":
    case "multiselect":
      return Array.isArray(raw) && raw.every((x) => typeof x === "string") ? (raw as string[]) : undefined;
    default:
      return typeof raw === "string" ? raw : undefined;
  }
}

export interface ImportResult {
  scenario: Scenario;
  params: Params;
  notes: string[];
}

/** Parse and sanitize an exported config. Unknown or mistyped keys fall back to defaults. */
export function importConfig(text: string): ImportResult {
  let data: unknown;
  try {
    data = JSON.parse(text);
  } catch (e) {
    throw new Error(`Not valid JSON: ${(e as Error).message}`, { cause: e });
  }
  if (typeof data !== "object" || data === null) throw new Error("Config must be a JSON object.");
  const obj = data as Partial<ExportedConfig>;
  if (obj.kind !== CONFIG_KIND) throw new Error(`Not a DAG Creator config (expected kind "${CONFIG_KIND}").`);
  const scenario = getScenario(String(obj.scenario ?? ""));
  if (!scenario) throw new Error(`Unknown scenario "${obj.scenario}".`);

  const params = defaultParams(scenario);
  const notes: string[] = [];
  const raw = (obj.params ?? {}) as Record<string, unknown>;
  const fields = new Map(allFields(scenario).map((f) => [f.key, f]));
  for (const [k, value] of Object.entries(raw)) {
    const f = fields.get(k);
    if (!f) {
      notes.push(`Ignored unknown field "${k}".`);
      continue;
    }
    const c = coerce(f, value);
    if (c === undefined) notes.push(`Ignored "${k}": expected a ${f.type} value.`);
    else params[k] = c;
  }
  if (obj.generatorVersion && obj.generatorVersion !== RUNTIME.generatorVersion) {
    notes.push(`Config was exported by v${obj.generatorVersion}; this is v${RUNTIME.generatorVersion}.`);
  }
  return { scenario, params, notes };
}
