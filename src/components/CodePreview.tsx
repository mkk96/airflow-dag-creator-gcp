import CodeMirror, { EditorView } from "@uiw/react-codemirror";
import { useMemo } from "react";
import { languageFor } from "../lib/languages";
import type { GeneratedFile } from "../scenarios/types";

interface Props {
  files: GeneratedFile[];
  activePath: string;
  onSelect: (path: string) => void;
}

const ROLE_ORDER = { dag: 0, sql: 1, script: 2, config: 3, readme: 4 } as const;

export function CodePreview({ files, activePath, onSelect }: Props) {
  const sorted = useMemo(() => [...files].sort((a, b) => ROLE_ORDER[a.role] - ROLE_ORDER[b.role]), [files]);
  const active = sorted.find((f) => f.path === activePath) ?? sorted[0];
  const extensions = useMemo(
    () => [...languageFor(active?.language ?? "python"), EditorView.lineWrapping],
    [active?.language],
  );

  return (
    <div className="flex min-h-0 flex-1 flex-col overflow-hidden rounded-lg border border-slate-200 bg-white shadow-sm">
      <div className="flex gap-1 overflow-x-auto border-b border-slate-200 bg-slate-50 px-2 pt-2" role="tablist">
        {sorted.map((f) => {
          const isActive = f.path === active?.path;
          return (
            <button
              key={f.path}
              role="tab"
              aria-selected={isActive}
              onClick={() => onSelect(f.path)}
              title={f.path}
              className={`shrink-0 rounded-t-md border border-b-0 px-3 py-1.5 font-mono text-xs ${
                isActive
                  ? "border-slate-200 bg-white text-slate-900"
                  : "border-transparent text-slate-500 hover:bg-white/60 hover:text-slate-800"
              }`}
            >
              {f.path.split("/").pop()}
            </button>
          );
        })}
      </div>
      {active && (
        <>
          <div className="border-b border-slate-100 px-3 py-1 font-mono text-[11px] text-slate-400">{active.path}</div>
          <div className="min-h-0 flex-1 overflow-auto">
            <CodeMirror
              value={active.content}
              editable={false}
              readOnly
              extensions={extensions}
              basicSetup={{ highlightActiveLine: false, highlightActiveLineGutter: false, foldGutter: true }}
              className="text-[13px]"
            />
          </div>
        </>
      )}
    </div>
  );
}
