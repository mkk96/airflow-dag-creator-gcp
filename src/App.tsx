import { useCallback, useEffect, useMemo, useState } from "react";
import { CodePreview } from "./components/CodePreview";
import { DynamicForm } from "./components/DynamicForm";
import { ScenarioPicker } from "./components/ScenarioPicker";
import { Toolbar } from "./components/Toolbar";
import { GENERATOR_NAME, RUNTIME, runtimeLabel } from "./config/runtime";
import { buildZip, dagFile, downloadBlob, downloadText } from "./lib/bundle";
import { exportConfig, importConfig } from "./lib/configIO";
import { suggestNames } from "./lib/suggest";
import { defaultParams } from "./scenarios/common";
import { getScenario, SCENARIOS } from "./scenarios/registry";
import type { GeneratedBundle, ParamValue, Params, Scenario } from "./scenarios/types";
import { countIssues, validateParams } from "./validation/validate";

const STORAGE_KEY = (id: string) => `dag-creator:v1:${id}`;

/** Restore the last form for a scenario (per-browser convenience only). */
function loadSaved(s: Scenario): Params {
  try {
    const raw = localStorage.getItem(STORAGE_KEY(s.id));
    if (raw) return importConfig(raw).params;
  } catch {
    /* storage unavailable or stale: fall back to defaults */
  }
  return defaultParams(s);
}

function save(s: Scenario, p: Params) {
  try {
    localStorage.setItem(STORAGE_KEY(s.id), exportConfig(s, p));
  } catch {
    /* ignore */
  }
}

export default function App() {
  const [scenarioId, setScenarioId] = useState(SCENARIOS[0].id);
  const scenario = getScenario(scenarioId) ?? SCENARIOS[0];
  const [params, setParams] = useState<Params>(() => loadSaved(scenario));
  const [activePath, setActivePath] = useState("");
  const [toast, setToast] = useState<{ text: string; kind: "ok" | "error" } | null>(null);

  useEffect(() => save(scenario, params), [scenario, params]);
  useEffect(() => {
    if (!toast) return;
    const t = setTimeout(() => setToast(null), 3500);
    return () => clearTimeout(t);
  }, [toast]);

  const validation = useMemo(() => validateParams(scenario, params), [scenario, params]);
  const { errors, warnings } = countIssues(validation);

  const { bundle, genError } = useMemo((): { bundle: GeneratedBundle | null; genError: string | null } => {
    try {
      return { bundle: scenario.generate(params), genError: null };
    } catch (e) {
      return { bundle: null, genError: (e as Error).message };
    }
  }, [scenario, params]);

  const onChange = useCallback((key: string, value: ParamValue) => {
    setParams((p) => ({ ...p, [key]: value }));
  }, []);

  const notify = (text: string, kind: "ok" | "error" = "ok") => setToast({ text, kind });

  const onSuggest = () => {
    const s = suggestNames(scenario, params);
    setParams((p) => ({ ...p, ...s }) as Params);
    notify(`Filled ${Object.keys(s).join(", ") || "nothing (set org, env and domain first)"}.`);
  };

  const onScenario = (id: string) => {
    const s = getScenario(id);
    if (!s) return;
    setScenarioId(id);
    setParams(loadSaved(s));
    setActivePath("");
  };

  const onDownloadPy = () => {
    if (!bundle) return;
    downloadText(dagFile(bundle).content, `${bundle.dagId}.py`, "text/x-python");
  };

  const onDownloadZip = async () => {
    if (!bundle) return;
    try {
      downloadBlob(await buildZip(bundle), `${bundle.dagId}.zip`);
    } catch (e) {
      notify((e as Error).message, "error");
    }
  };

  const onCopy = async () => {
    if (!bundle) return;
    try {
      await navigator.clipboard.writeText(dagFile(bundle).content);
      notify("DAG copied to clipboard.");
    } catch {
      notify("Clipboard is not available in this browser.", "error");
    }
  };

  const onExport = () => {
    const name = String(params.dag_id || scenario.id).replace(/[^A-Za-z0-9_.-]/g, "_");
    downloadText(exportConfig(scenario, params), `${name}.config.json`, "application/json");
  };

  const onImport = (text: string) => {
    try {
      const r = importConfig(text);
      setScenarioId(r.scenario.id);
      setParams(r.params);
      setActivePath("");
      notify(r.notes.length ? `Imported with notes: ${r.notes.join(" ")}` : "Config imported.");
    } catch (e) {
      notify((e as Error).message, "error");
    }
  };

  const onReset = () => {
    if (confirm("Reset all fields to the scenario defaults?")) setParams(defaultParams(scenario));
  };

  return (
    <div className="flex min-h-screen flex-col bg-slate-100 lg:h-screen">
      <header className="flex flex-wrap items-center gap-x-4 gap-y-1 border-b border-slate-200 bg-white px-4 py-3 sm:px-6">
        <div className="flex items-center gap-2.5">
          <img src={`${import.meta.env.BASE_URL}favicon.svg`} alt="" className="h-7 w-7" />
          <h1 className="text-base font-semibold text-slate-900 sm:text-lg">{GENERATOR_NAME}</h1>
        </div>
        <span className="rounded-full bg-slate-100 px-2.5 py-1 text-xs font-medium text-slate-600">
          {runtimeLabel()} · providers-google {RUNTIME.googleProviderVersion}
        </span>
        <span className="ml-auto text-xs text-slate-400">v{RUNTIME.generatorVersion} · runs entirely in your browser</span>
      </header>

      <main className="grid min-h-0 flex-1 grid-cols-1 gap-4 p-4 lg:grid-cols-[minmax(440px,5fr)_7fr]">
        <aside className="flex min-h-0 flex-col gap-3 lg:overflow-y-auto lg:pr-1">
          <ScenarioPicker scenario={scenario} onChange={onScenario} />
          <DynamicForm
            scenario={scenario}
            params={params}
            validation={validation}
            onChange={onChange}
            onSuggest={onSuggest}
          />
          <button onClick={onReset} className="self-start px-1 pb-2 text-xs text-slate-500 underline hover:text-slate-800">
            Reset to defaults
          </button>
        </aside>

        <section className="flex min-h-[70vh] flex-col gap-3 lg:min-h-0">
          <Toolbar
            bundle={bundle}
            errors={errors}
            warnings={warnings}
            onDownloadPy={onDownloadPy}
            onDownloadZip={onDownloadZip}
            onCopy={onCopy}
            onExport={onExport}
            onImport={onImport}
          />
          {genError ? (
            <div className="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700">
              Could not generate the DAG: {genError}
            </div>
          ) : (
            bundle && <CodePreview files={bundle.files} activePath={activePath} onSelect={setActivePath} />
          )}
        </section>
      </main>

      {toast && (
        <div
          role="status"
          className={`fixed right-4 bottom-4 max-w-md rounded-lg px-4 py-2.5 text-sm shadow-lg ${
            toast.kind === "ok" ? "bg-slate-900 text-white" : "bg-red-600 text-white"
          }`}
        >
          {toast.text}
        </div>
      )}
    </div>
  );
}
