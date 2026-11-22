import { plannedScenarios, SCENARIOS } from "../scenarios/registry";
import type { Scenario } from "../scenarios/types";

interface Props {
  scenario: Scenario;
  onChange: (id: string) => void;
}

export function ScenarioPicker({ scenario, onChange }: Props) {
  const planned = plannedScenarios();
  return (
    <div className="rounded-lg border border-slate-200 bg-white p-4 shadow-sm">
      <label htmlFor="scenario" className="text-[13px] font-semibold text-slate-700">
        DAG scenario
      </label>
      <select
        id="scenario"
        className="mt-1 w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm font-medium text-slate-900 shadow-sm outline-none focus:border-blue-500 focus:ring-2 focus:ring-blue-500/20"
        value={scenario.id}
        onChange={(e) => onChange(e.target.value)}
      >
        <optgroup label="Available">
          {SCENARIOS.map((s) => (
            <option key={s.id} value={s.id}>
              {s.title}
            </option>
          ))}
        </optgroup>
        {planned.length > 0 && (
          <optgroup label="Coming soon">
            {planned.map((s) => (
              <option key={s.id} value={s.id} disabled>
                {s.title}
              </option>
            ))}
          </optgroup>
        )}
      </select>
      <p className="mt-2 text-sm leading-relaxed text-slate-600">{scenario.description}</p>
    </div>
  );
}
