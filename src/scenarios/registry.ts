/**
 * Scenario registry. To add a scenario:
 *   1. create src/scenarios/<name>.ts exporting a `Scenario`
 *   2. import it here and add it to SCENARIOS
 *   3. remove its entry from PLANNED_SCENARIOS (src/scenarios/planned.ts)
 */
import { bigqueryDqChecks } from "./bigqueryDqChecks";
import { bigquerySqlTransform } from "./bigquerySqlTransform";
import { bigqueryTableCopy } from "./bigqueryTableCopy";
import { bigqueryToGcs } from "./bigqueryToGcs";
import { cloudsqlToBigquery } from "./cloudsqlToBigquery";
import { cloudRunFunction } from "./cloudRunFunction";
import { dataflowTemplate } from "./dataflowTemplate";
import { dataformWorkflow } from "./dataformWorkflow";
import { dataprocEphemeral } from "./dataprocEphemeral";
import { dataprocServerless } from "./dataprocServerless";
import { fullElt } from "./fullElt";
import { gcsSensorLoadArchive } from "./gcsSensorLoadArchive";
import { gcsToBigquery } from "./gcsToBigquery";
import { PLANNED_SCENARIOS } from "./planned";
import { pubsub } from "./pubsub";
import type { PlannedScenario, Scenario } from "./types";

export const SCENARIOS: Scenario[] = [
  gcsToBigquery,
  gcsSensorLoadArchive,
  cloudsqlToBigquery,
  bigquerySqlTransform,
  bigqueryToGcs,
  bigqueryTableCopy,
  dataflowTemplate,
  dataprocEphemeral,
  dataprocServerless,
  cloudRunFunction,
  pubsub,
  bigqueryDqChecks,
  dataformWorkflow,
  fullElt,
];

export function getScenario(id: string): Scenario | undefined {
  return SCENARIOS.find((s) => s.id === id);
}

/** Planned scenarios not implemented yet (shown disabled in the picker). */
export function plannedScenarios(): PlannedScenario[] {
  const ids = new Set(SCENARIOS.map((s) => s.id));
  return PLANNED_SCENARIOS.filter((s) => !ids.has(s.id));
}
