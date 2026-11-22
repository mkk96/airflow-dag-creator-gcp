/**
 * Generate every registered scenario with each of its sample parameter sets
 * into ./generated/<scenario>__<sample>/ (mirroring the Composer bucket layout).
 * Fails if a sample does not pass form validation or dag_ids collide.
 *
 *   npm run generate
 */
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { sampleParams } from "../src/scenarios/common";
import { SCENARIOS } from "../src/scenarios/registry";
import { validateParams } from "../src/validation/validate";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const outDir = join(root, "generated");
rmSync(outDir, { recursive: true, force: true });

const seen = new Map<string, string>();
let failures = 0;
let count = 0;

for (const scenario of SCENARIOS) {
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
