/**
 * Generate every registered scenario with each of its sample parameter sets
 * into ./generated/<scenario>__<sample>/ (mirroring the Composer bucket layout).
 * Fails if a sample does not pass form validation or dag_ids collide.
 *
 *   npm run generate
 *
 * Work on one scenario in isolation (no registry change needed):
 *   npx tsx scripts/generate-samples.ts --scenario src/scenarios/myScenario.ts --out generated-my
 *   GENERATED_DIR=generated-my pytest tests/dags
 */
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { sampleParams } from "../src/scenarios/common";
import type { Scenario } from "../src/scenarios/types";
import { validateParams } from "../src/validation/validate";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function parseArgs(argv: string[]): { scenarioFiles: string[]; out: string } {
  const scenarioFiles: string[] = [];
  let out = "generated";
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--scenario") scenarioFiles.push(argv[++i]);
    else if (argv[i] === "--out") out = argv[++i];
    else throw new Error(`Unknown argument ${argv[i]}`);
  }
  return { scenarioFiles, out };
}

function isScenario(x: unknown): x is Scenario {
  return typeof x === "object" && x !== null && "id" in x && "generate" in x && "samples" in x;
}

async function loadScenarios(files: string[]): Promise<Scenario[]> {
  if (files.length === 0) return (await import("../src/scenarios/registry")).SCENARIOS;
  const out: Scenario[] = [];
  for (const f of files) {
    const mod = (await import(pathToFileURL(resolve(root, f)).href)) as Record<string, unknown>;
    const found = Object.values(mod).filter(isScenario);
    if (found.length === 0) throw new Error(`${f} exports no Scenario`);
    out.push(...found);
  }
  return out;
}

const { scenarioFiles, out } = parseArgs(process.argv.slice(2));
const outDir = resolve(root, out);
const scenarios = await loadScenarios(scenarioFiles);
rmSync(outDir, { recursive: true, force: true });

const seen = new Map<string, string>();
let failures = 0;
let count = 0;

for (const scenario of scenarios) {
  const names = Object.keys(scenario.samples);
  if (names.length === 0) throw new Error(`Scenario ${scenario.id} has no samples`);
  for (const name of names) {
    const label = `${scenario.id}__${name}`;
    const params = sampleParams(scenario, name);
    const errors = Object.entries(validateParams(scenario, params)).flatMap(([k, i]) =>
      i.errors.map((e) => `${k}: ${e}`),
    );
    if (errors.length) {
      console.error(`✗ ${label} has validation errors:\n  ${errors.join("\n  ")}`);
      failures++;
      continue;
    }
    const bundle = scenario.generate(params);
    if (seen.has(bundle.dagId)) {
      console.error(`✗ ${label}: dag_id "${bundle.dagId}" already used by ${seen.get(bundle.dagId)}`);
      failures++;
      continue;
    }
    seen.set(bundle.dagId, label);
    const base = join(outDir, label);
    for (const f of bundle.files) {
      const target = resolve(base, f.path);
      if (!target.startsWith(base + sep)) throw new Error(`Unsafe path ${f.path}`);
      mkdirSync(dirname(target), { recursive: true });
      writeFileSync(target, f.content, "utf8");
    }
    count++;
    console.log(`✓ ${label} → ${bundle.files.map((f) => f.path).join(", ")}`);
  }
}

if (failures) {
  console.error(`\n${failures} sample(s) failed.`);
  process.exit(1);
}
console.log(`\nGenerated ${count} sample bundle(s) into ${outDir}`);
