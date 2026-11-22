import { CircleCheck, CircleX, Lock, RotateCcw, Workflow } from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { CodePreview } from "./components/CodePreview";
import { DeployDialog } from "./components/DeployDialog";
import { DynamicForm } from "./components/DynamicForm";
import { ScenarioPicker } from "./components/ScenarioPicker";
import { Toolbar } from "./components/Toolbar";
import { GENERATOR_NAME, RUNTIME } from "./config/runtime";
import { buildZip, dagFile, downloadBlob, downloadText } from "./lib/bundle";
import { exportConfig, importConfig } from "./lib/configIO";
import { suggestNames } from "./lib/suggest";
import { allFields, defaultParams, isVisible } from "./scenarios/common";
import { getScenario, SCENARIOS } from "./scenarios/registry";
import type { GeneratedBundle, ParamValue, Params, Scenario } from "./scenarios/types";
import { countIssues, validateParams } from "./validation/validate";

const STORAGE_KEY = (id: string) => `dag-creator:v1:${id}`;
const LAST_SCENARIO_KEY = "dag-creator:v1:last-scenario";

function storageGet(key: string): string | null {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

function storageSet(key: string, value: string) {
  try {
    localStorage.setItem(key, value);
  } catch {
    /* storage unavailable: ignore */
  }
}

/** Restore the last form for a scenario (per-browser convenience only). */
function loadSaved(s: Scenario): Params {
  try {
    const raw = storageGet(STORAGE_KEY(s.id));
    if (raw) return importConfig(raw).params;
  } catch {
    /* stale or invalid: fall back to defaults */
  }
  return defaultParams(s);
}

function initialScenario(): Scenario {
  const id = storageGet(LAST_SCENARIO_KEY);
  return (id && getScenario(id)) || SCENARIOS[0];
}

export default function App() {
  const [scenarioId, setScenarioId] = useState(() => initialScenario().id);
  const scenario = getScenario(scenarioId) ?? SCENARIOS[0];
  const [params, setParams] = useState<Params>(() => loadSaved(scenario));
  const [activePath, setActivePath] = useState("");
  const [deployOpen, setDeployOpen] = useState(false);
  const [toast, setToast] = useState<{ text: string; kind: "ok" | "error" } | null>(null);

  useEffect(() => {
    storageSet(STORAGE_KEY(scenario.id), exportConfig(scenario, params));
    storageSet(LAST_SCENARIO_KEY, scenario.id);
  }, [scenario, params]);

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
    notify(Object.keys(s).length ? `Suggested: ${Object.keys(s).join(", ")}` : "Set org, env and domain first.");
  };

  const onScenario = (id: string) => {
    const s = getScenario(id);
    if (!s) return;
    setScenarioId(id);
    setParams(loadSaved(s));
    setActivePath("");
  };

  /** Scroll to the first field with an error (or warning) and flash it. */
  const onShowIssues = () => {
    const visible = allFields(scenario).filter((f) => isVisible(f, params));
    const target =
      visible.find((f) => validation[f.key]?.errors.length) ?? visible.find((f) => validation[f.key]?.warnings.length);
    if (!target) return;
    const section = document.getElementById(`section-${target.section}`) as HTMLDetailsElement | null;
    if (section) section.open = true;
    const el = document.getElementById(`field-${target.key}`);
    if (!el) return;
    el.scrollIntoView({ behavior: "smooth", block: "center" });
    el.classList.remove("flash-error");
    void el.offsetWidth;
    el.classList.add("flash-error");
  };

  const onDownloadPy = () => {
    if (!bundle) return;
    downloadText(dagFile(bundle).content, `${bundle.dagId}.py`, "text/x-python");
    notify(`Downloaded ${bundle.dagId}.py`);
  };

  const onDownloadZip = async () => {
    if (!bundle) return;
    try {
      downloadBlob(await buildZip(bundle), `${bundle.dagId}.zip`);
      notify(`Downloaded ${bundle.dagId}.zip`);
    } catch (e) {
      notify((e as Error).message, "error");
    }
  };

  const onCopy = async () => {
    if (!bundle) return;
    try {
      await navigator.clipboard.writeText(dagFile(bundle).content);
      notify("DAG copied to clipboard");
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
      notify(r.notes.length ? `Imported with notes: ${r.notes.join(" ")}` : "Config imported");
    } catch (e) {
      notify((e as Error).message, "error");
    }
  };

  const onReset = () => {
    if (confirm("Reset all fields to the scenario defaults?")) setParams(defaultParams(scenario));
  };

  return (
    <div className="flex min-h-screen flex-col lg:h-screen">
      <header className="relative overflow-hidden bg-[#0b1020] text-white">
        <div
          className="pointer-events-none absolute inset-0 opacity-60"
          style={{
            background:
              "radial-gradient(40rem 12rem at 15% 0%, rgb(99 102 241 / 0.45), transparent 70%), radial-gradient(30rem 10rem at 85% 0%, rgb(14 165 233 / 0.35), transparent 70%)",
          }}
          aria-hidden
        />
        <div className="relative flex flex-wrap items-center gap-x-5 gap-y-2 px-4 py-3.5 sm:px-6">
          <div className="flex items-center gap-3">
            <div className="grid h-9 w-9 place-items-center rounded-xl bg-gradient-to-br from-indigo-500 to-sky-500 shadow-lg shadow-indigo-500/30">
              <Workflow className="h-5 w-5" strokeWidth={2.2} />
            </div>
            <div className="leading-tight">
              <h1 className="text-[15px] font-semibold tracking-tight sm:text-base">{GENERATOR_NAME}</h1>
              <p className="text-[11.5px] text-slate-400">Production-ready Cloud Composer DAGs in seconds</p>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-1.5 text-[11px] font-medium">
            {[
              RUNTIME.composerTargets,
              `Airflow ${RUNTIME.airflowVersion}`,
              `Python ${RUNTIME.pythonVersion}`,
              `providers-google ${RUNTIME.googleProviderVersion}`,
            ].map((chip) => (
              <span key={chip} className="rounded-full border border-white/10 bg-white/5 px-2.5 py-1 text-slate-300">
                {chip}
              </span>
            ))}
          </div>
          <div className="ml-auto flex items-center gap-1.5 text-[11.5px] text-slate-400">
            <Lock className="h-3.5 w-3.5" />
            Runs entirely in your browser · v{RUNTIME.generatorVersion}
          </div>
        </div>
      </header>

      <main className="grid min-h-0 flex-1 grid-cols-1 gap-4 p-3 sm:p-4 lg:grid-cols-[minmax(460px,5fr)_7fr]">
        <aside className="scroll-thin flex min-h-0 min-w-0 flex-col gap-3 lg:overflow-x-hidden lg:overflow-y-auto lg:pr-1.5">
          <ScenarioPicker scenario={scenario} onChange={onScenario} />
          <DynamicForm scenario={scenario} params={params} validation={validation} onChange={onChange} onSuggest={onSuggest} />
          <button
            onClick={onReset}
            className="inline-flex items-center gap-1.5 self-start px-1 pb-3 text-xs font-medium text-slate-500 transition hover:text-slate-800"
          >
            <RotateCcw className="h-3.5 w-3.5" /> Reset to defaults
          </button>
        </aside>

        <section className="flex min-h-[75vh] flex-col gap-3 lg:min-h-0">
          <Toolbar
            bundle={bundle}
            errors={errors}
            warnings={warnings}
            onShowIssues={onShowIssues}
            onDownloadPy={onDownloadPy}
            onDownloadZip={onDownloadZip}
            onDeploy={() => setDeployOpen(true)}
            onCopy={onCopy}
            onExport={onExport}
            onImport={onImport}
          />
          {genError ? (
            <div className="rounded-2xl border border-red-200 bg-red-50 p-4 text-sm text-red-700">Could not generate the DAG: {genError}</div>
          ) : (
            bundle && <CodePreview files={bundle.files} activePath={activePath} onSelect={setActivePath} />
          )}
        </section>
      </main>

      {deployOpen && bundle && errors === 0 && (
        <DeployDialog bundle={bundle} onClose={() => setDeployOpen(false)} notify={notify} />
      )}

      {toast && (
        <div
          role="status"
          className={`animate-pop-in fixed right-4 bottom-4 z-50 flex max-w-md items-start gap-2 rounded-xl px-4 py-3 text-sm font-medium shadow-2xl ring-1 ${
            toast.kind === "ok" ? "bg-slate-900 text-white ring-white/10" : "bg-red-600 text-white ring-red-500"
          }`}
        >
          {toast.kind === "ok" ? (
            <CircleCheck className="mt-px h-4 w-4 shrink-0 text-emerald-400" />
          ) : (
            <CircleX className="mt-px h-4 w-4 shrink-0" />
          )}
          {toast.text}
        </div>
      )}
    </div>
  );
}
