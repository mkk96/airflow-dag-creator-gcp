/** "Suggest names": fill naming fields from org / env / domain / source / target. */
import { fillPattern, TEAM, toToken } from "../config/naming";
import { frequencyFor } from "../config/schedules";
import { str } from "../scenarios/common";
import type { NamingContext, Params, Scenario } from "../scenarios/types";

export function namingContext(scenario: Scenario, p: Params): NamingContext {
  return {
    org: toToken(str(p, "org")),
    env: str(p, "env"),
    domain: toToken(str(p, "domain")),
    source: scenario.naming.source,
    target: scenario.naming.target,
    frequency: frequencyFor(str(p, "schedule_preset"), str(p, "schedule_cron")),
    layerSet: TEAM.layerSet,
  };
}

export function suggestNames(scenario: Scenario, p: Params): Partial<Params> {
  const ctx = namingContext(scenario, p);
  const out: Partial<Params> = {};
  const dagId = fillPattern("dagId", ctx as unknown as Record<string, string>);
  if (dagId) out.dag_id = dagId;
  return { ...out, ...(scenario.suggest?.(ctx, p) ?? {}) };
}
