import { ArrowRightLeft, Check, Search, X } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import { plannedScenarios, SCENARIOS } from "../scenarios/registry";
import type { Scenario } from "../scenarios/types";
import { CATEGORIES, scenarioMeta } from "./scenarioMeta";

interface Props {
  scenario: Scenario;
  onChange: (id: string) => void;
}

interface Item {
  id: string;
  title: string;
  description: string;
  available: boolean;
}

export function ScenarioPicker({ scenario, onChange }: Props) {
  const [open, setOpen] = useState(false);
  const meta = scenarioMeta(scenario.id);
  const Icon = meta.icon;

  return (
    <>
      <div className="rounded-2xl border border-slate-200/80 bg-white/90 p-4 shadow-sm ring-1 ring-black/[0.02] backdrop-blur">
        <div className="flex items-start gap-3.5">
          <div
            className={`grid h-11 w-11 shrink-0 place-items-center rounded-xl bg-gradient-to-br ${meta.tile} text-white shadow-md`}
          >
            <Icon className="h-5 w-5" strokeWidth={2.2} />
          </div>
          <div className="min-w-0 flex-1">
            <div className="text-[11px] font-semibold tracking-wider text-slate-400 uppercase">{meta.category}</div>
            <h2 className="truncate text-[15px] font-semibold text-slate-900">{scenario.title}</h2>
          </div>
          <button
            onClick={() => setOpen(true)}
            className="inline-flex shrink-0 items-center gap-1.5 rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 shadow-sm transition hover:border-indigo-300 hover:text-indigo-700"
          >
            <ArrowRightLeft className="h-3.5 w-3.5" />
            Change
          </button>
        </div>
        <p className="mt-3 text-[13px] leading-relaxed text-slate-600">{scenario.description}</p>
      </div>
      {open && (
        <ScenarioGallery
          current={scenario.id}
          onClose={() => setOpen(false)}
          onPick={(id) => {
            onChange(id);
            setOpen(false);
          }}
        />
      )}
    </>
  );
}

function ScenarioGallery({ current, onClose, onPick }: { current: string; onClose: () => void; onPick: (id: string) => void }) {
  const [query, setQuery] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    inputRef.current?.focus();
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const items: Item[] = useMemo(
    () => [
      ...SCENARIOS.map((s) => ({ id: s.id, title: s.title, description: s.description, available: true })),
      ...plannedScenarios().map((s) => ({ ...s, available: false })),
    ],
    [],
  );

  const q = query.trim().toLowerCase();
  const filtered = items.filter((i) => !q || `${i.title} ${i.description} ${scenarioMeta(i.id).category}`.toLowerCase().includes(q));

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-slate-950/50 p-4 backdrop-blur-sm sm:p-8" onMouseDown={onClose}>
      <div
        role="dialog"
        aria-modal="true"
        aria-label="Choose a DAG scenario"
        className="animate-pop-in w-full max-w-5xl rounded-2xl bg-white shadow-2xl ring-1 ring-black/5"
        onMouseDown={(e) => e.stopPropagation()}
      >
        <div className="flex items-center gap-3 border-b border-slate-100 px-5 py-4">
          <div>
            <h2 className="text-lg font-semibold text-slate-900">Choose a DAG scenario</h2>
            <p className="text-sm text-slate-500">{items.filter((i) => i.available).length} ready-to-use pipelines for Cloud Composer</p>
          </div>
          <button onClick={onClose} className="ml-auto rounded-lg p-2 text-slate-400 hover:bg-slate-100 hover:text-slate-700" aria-label="Close">
            <X className="h-5 w-5" />
          </button>
        </div>
        <div className="px-5 pt-4">
          <label className="relative block">
            <Search className="pointer-events-none absolute top-1/2 left-3 h-4 w-4 -translate-y-1/2 text-slate-400" />
            <input
              ref={inputRef}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search scenarios — e.g. dataproc, merge, pub/sub"
              className="w-full rounded-xl border border-slate-200 bg-slate-50 py-2.5 pr-3 pl-9 text-sm outline-none transition focus:border-indigo-400 focus:bg-white focus:ring-4 focus:ring-indigo-500/10"
            />
          </label>
        </div>
        <div className="scroll-thin max-h-[70vh] overflow-y-auto px-5 pt-2 pb-5">
          {CATEGORIES.map((cat) => {
            const inCat = filtered.filter((i) => scenarioMeta(i.id).category === cat);
            if (!inCat.length) return null;
            return (
              <section key={cat} className="mt-4">
                <h3 className="mb-2 text-[11px] font-semibold tracking-wider text-slate-400 uppercase">{cat}</h3>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
                  {inCat.map((i) => {
                    const m = scenarioMeta(i.id);
                    const Icon = m.icon;
                    const selected = i.id === current;
                    return (
                      <button
                        key={i.id}
                        disabled={!i.available}
                        onClick={() => onPick(i.id)}
                        className={`group relative flex h-full flex-col gap-2 rounded-xl border p-4 text-left transition ${
                          selected
                            ? "border-indigo-400 bg-indigo-50/60 ring-4 ring-indigo-500/10"
                            : "border-slate-200 bg-white hover:-translate-y-0.5 hover:border-indigo-300 hover:shadow-lg hover:shadow-indigo-500/5"
                        } disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:translate-y-0 disabled:hover:shadow-none`}
                      >
                        <div className="flex items-center gap-3">
                          <div className={`grid h-9 w-9 place-items-center rounded-lg bg-gradient-to-br ${m.tile} text-white shadow`}>
                            <Icon className="h-[18px] w-[18px]" strokeWidth={2.2} />
                          </div>
                          <span className="text-sm font-semibold text-slate-900">{i.title}</span>
                          {selected && <Check className="ml-auto h-4 w-4 text-indigo-600" />}
                          {!i.available && (
                            <span className="ml-auto rounded-full bg-slate-100 px-2 py-0.5 text-[10px] font-semibold text-slate-500">Soon</span>
                          )}
                        </div>
                        <p className="text-[12.5px] leading-relaxed text-slate-500">{i.description}</p>
                      </button>
                    );
                  })}
                </div>
              </section>
            );
          })}
          {filtered.length === 0 && <p className="py-10 text-center text-sm text-slate-500">No scenario matches “{query}”.</p>}
        </div>
      </div>
    </div>
  );
}
