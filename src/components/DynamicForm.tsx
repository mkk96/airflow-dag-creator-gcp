import { allFields, allSections, isVisible } from "../scenarios/common";
import type { ParamValue, Params, Scenario } from "../scenarios/types";
import type { ValidationResult } from "../validation/validate";
import { Field } from "./Field";

interface Props {
  scenario: Scenario;
  params: Params;
  validation: ValidationResult;
  onChange: (key: string, value: ParamValue) => void;
  onSuggest: () => void;
}

export function DynamicForm({ scenario, params, validation, onChange, onSuggest }: Props) {
  const fields = allFields(scenario).filter((f) => isVisible(f, params));
  const scenarioSectionIds = new Set(scenario.sections.map((s) => s.id));

  return (
    <div className="flex flex-col gap-3">
      {allSections(scenario).map((section) => {
        const sectionFields = fields.filter((f) => f.section === section.id);
        if (sectionFields.length === 0) return null;
        const errors = sectionFields.reduce((n, f) => n + (validation[f.key]?.errors.length ?? 0), 0);
        const warnings = sectionFields.reduce((n, f) => n + (validation[f.key]?.warnings.length ?? 0), 0);
        return (
          <details key={section.id} open className="group rounded-lg border border-slate-200 bg-white shadow-sm">
            <summary className="flex cursor-pointer list-none items-center justify-between gap-2 px-4 py-2.5 select-none">
              <span className="flex items-center gap-2">
                <svg
                  className="h-3.5 w-3.5 text-slate-400 transition group-open:rotate-90"
                  viewBox="0 0 20 20"
                  fill="currentColor"
                  aria-hidden
                >
                  <path d="M7 5l6 5-6 5V5z" />
                </svg>
                <span className="text-sm font-semibold text-slate-800">{section.title}</span>
                {scenarioSectionIds.has(section.id) && (
                  <span className="rounded bg-blue-50 px-1.5 py-px text-[10px] font-semibold text-blue-700 uppercase">
                    scenario
                  </span>
                )}
              </span>
              <span className="flex items-center gap-1.5 text-[11px] font-semibold">
                {errors > 0 && <span className="rounded-full bg-red-100 px-2 py-0.5 text-red-700">{errors}</span>}
                {warnings > 0 && (
                  <span className="rounded-full bg-amber-100 px-2 py-0.5 text-amber-800">{warnings}</span>
                )}
              </span>
            </summary>
            <div className="flex flex-col gap-4 border-t border-slate-100 px-4 pt-3 pb-4">
              {section.description && <p className="-mb-1 text-xs text-slate-500">{section.description}</p>}
              {section.id === "env" && (
                <button
                  type="button"
                  onClick={onSuggest}
                  className="self-start rounded-md border border-blue-200 bg-blue-50 px-3 py-1.5 text-xs font-semibold text-blue-700 hover:bg-blue-100"
                  title="Fill dag_id, bucket and dataset names from org, env, domain, source/target and schedule"
                >
                  ✨ Suggest names
                </button>
              )}
              <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
                {sectionFields.map((f) => (
                  <div
                    key={f.key}
                    className={
                      f.type === "code" || f.type === "list" || f.type === "multiselect" || f.key === "description"
                        ? "sm:col-span-2"
                        : ""
                    }
                  >
                    <Field
                      field={f}
                      value={params[f.key]}
                      params={params}
                      issues={validation[f.key]}
                      onChange={(v) => onChange(f.key, v)}
                    />
                  </div>
                ))}
              </div>
            </div>
          </details>
        );
      })}
    </div>
  );
}
