import { tokyoNight } from "@uiw/codemirror-theme-tokyo-night";
import CodeMirror, { EditorView } from "@uiw/react-codemirror";
import { FileCode, FileJson, FileText, type LucideIcon, Terminal } from "lucide-react";
import { useMemo } from "react";
import { languageFor } from "../lib/languages";
import type { FileLanguage, GeneratedFile } from "../scenarios/types";

interface Props {
  files: GeneratedFile[];
  activePath: string;
  onSelect: (path: string) => void;
}

const ROLE_ORDER = { dag: 0, sql: 1, script: 2, config: 3, readme: 4 } as const;

const FILE_ICON: Record<FileLanguage, { icon: LucideIcon; color: string }> = {
  python: { icon: FileCode, color: "text-sky-400" },
  sql: { icon: Terminal, color: "text-amber-400" },
  json: { icon: FileJson, color: "text-lime-400" },
  markdown: { icon: FileText, color: "text-slate-400" },
};

const editorTheme = EditorView.theme({
  "&": { backgroundColor: "transparent" },
  ".cm-gutters": { backgroundColor: "transparent", borderRight: "1px solid rgb(255 255 255 / 0.06)" },
  ".cm-content": { padding: "12px 0" },
});

export function CodePreview({ files, activePath, onSelect }: Props) {
  const sorted = useMemo(() => [...files].sort((a, b) => ROLE_ORDER[a.role] - ROLE_ORDER[b.role]), [files]);
  const active = sorted.find((f) => f.path === activePath) ?? sorted[0];
  const extensions = useMemo(
    () => [...languageFor(active?.language ?? "python"), EditorView.lineWrapping, editorTheme],
    [active?.language],
  );
  const lines = active ? active.content.split("\n").length - 1 : 0;
  const kb = active ? new Blob([active.content]).size / 1024 : 0;

  return (
    <div className="flex min-h-0 flex-1 flex-col overflow-hidden rounded-2xl border border-slate-800 bg-[#1a1b26] shadow-2xl shadow-slate-900/20 ring-1 ring-black/10">
      <div className="flex items-center gap-3 border-b border-white/5 bg-[#16161e] px-3 pt-2.5">
        <div className="mb-2.5 flex gap-1.5 pl-1" aria-hidden>
          <span className="h-3 w-3 rounded-full bg-[#ff5f57]" />
          <span className="h-3 w-3 rounded-full bg-[#febc2e]" />
          <span className="h-3 w-3 rounded-full bg-[#28c840]" />
        </div>
        <div className="scroll-thin flex gap-0.5 overflow-x-auto" role="tablist">
          {sorted.map((f) => {
            const isActive = f.path === active?.path;
            const { icon: Icon, color } = FILE_ICON[f.language];
            return (
              <button
                key={f.path}
                role="tab"
                aria-selected={isActive}
                onClick={() => onSelect(f.path)}
                title={f.path}
                className={`flex shrink-0 items-center gap-1.5 rounded-t-lg px-3 py-2 font-mono text-[12px] transition ${
                  isActive ? "bg-[#1a1b26] text-slate-100" : "text-slate-500 hover:bg-white/[0.03] hover:text-slate-300"
                }`}
              >
                <Icon className={`h-3.5 w-3.5 ${isActive ? color : ""}`} />
                {f.path.split("/").pop()}
              </button>
            );
          })}
        </div>
      </div>
      {active && (
        <>
          <div className="scroll-thin min-h-0 flex-1 overflow-auto">
            <CodeMirror
              value={active.content}
              editable={false}
              readOnly
              theme={tokyoNight}
              extensions={extensions}
              basicSetup={{ highlightActiveLine: false, highlightActiveLineGutter: false, foldGutter: true }}
              className="text-[12.5px]"
            />
          </div>
          <div className="flex items-center gap-4 border-t border-white/5 bg-[#16161e] px-4 py-1.5 font-mono text-[11px] text-slate-500">
            <span className="truncate">{active.path}</span>
            <span className="ml-auto shrink-0">{lines} lines</span>
            <span className="shrink-0">{kb.toFixed(1)} KB</span>
            <span className="shrink-0 uppercase">{active.language}</span>
          </div>
        </>
      )}
    </div>
  );
}
