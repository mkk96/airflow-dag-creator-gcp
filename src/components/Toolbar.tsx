import { CircleCheck, CircleX, ClipboardCopy, Download, FileCode, Package, TriangleAlert, Upload } from "lucide-react";
import { useRef } from "react";
import type { GeneratedBundle } from "../scenarios/types";

interface Props {
  bundle: GeneratedBundle | null;
  errors: number;
  warnings: number;
  onShowIssues: () => void;
  onDownloadPy: () => void;
  onDownloadZip: () => void;
  onCopy: () => void;
  onExport: () => void;
  onImport: (text: string) => void;
}

const btn =
  "inline-flex items-center gap-1.5 rounded-lg px-3 py-2 text-[13px] font-semibold transition " +
  "disabled:cursor-not-allowed disabled:opacity-40";
const ghost = `${btn} border border-slate-200 bg-white text-slate-700 shadow-xs hover:border-slate-300 hover:bg-slate-50`;
const primary = `${btn} bg-gradient-to-r from-indigo-600 to-violet-600 text-white shadow-md shadow-indigo-500/25 hover:shadow-lg hover:shadow-indigo-500/35`;

export function Toolbar(props: Props) {
  const { bundle, errors, warnings, onShowIssues, onDownloadPy, onDownloadZip, onCopy, onExport, onImport } = props;
  const fileRef = useRef<HTMLInputElement>(null);
  const blocked = errors > 0 || !bundle;
  const blockedTitle = errors > 0 ? "Fix the errors in the form first" : undefined;
  const zipPrimary = !!bundle?.needsZip;

  const status =
    errors > 0
      ? { cls: "bg-red-50 text-red-700 ring-red-200 hover:bg-red-100", icon: CircleX, text: `${errors} error${errors === 1 ? "" : "s"} — show` }
      : warnings > 0
        ? {
            cls: "bg-amber-50 text-amber-800 ring-amber-200 hover:bg-amber-100",
            icon: TriangleAlert,
            text: `Ready · ${warnings} suggestion${warnings === 1 ? "" : "s"}`,
          }
        : { cls: "bg-emerald-50 text-emerald-700 ring-emerald-200", icon: CircleCheck, text: "Ready to deploy" };
  const StatusIcon = status.icon;

  return (
    <div className="flex flex-wrap items-center gap-2 rounded-2xl border border-slate-200/80 bg-white/90 p-2 shadow-sm ring-1 ring-black/[0.02] backdrop-blur">
      <button
        onClick={onShowIssues}
        disabled={errors + warnings === 0}
        className={`mr-auto inline-flex items-center gap-1.5 rounded-lg px-3 py-2 text-[12.5px] font-semibold ring-1 transition disabled:cursor-default ${status.cls}`}
        title={errors + warnings ? "Jump to the first issue" : undefined}
      >
        <StatusIcon className="h-4 w-4" />
        {status.text}
      </button>

      <button className={ghost} onClick={onExport} title="Save the form as JSON">
        <Download className="h-4 w-4" /> <span className="hidden sm:inline">Export</span>
      </button>
      <button className={ghost} onClick={() => fileRef.current?.click()} title="Load a saved JSON config">
        <Upload className="h-4 w-4" /> <span className="hidden sm:inline">Import</span>
      </button>
      <input
        ref={fileRef}
        type="file"
        accept=".json,application/json"
        className="hidden"
        onChange={(e) => {
          const file = e.target.files?.[0];
          e.target.value = "";
          if (file) file.text().then(onImport);
        }}
      />
      <span className="mx-0.5 hidden h-6 w-px bg-slate-200 sm:block" aria-hidden />
      <button className={ghost} disabled={!bundle} onClick={onCopy} title="Copy the DAG .py to the clipboard">
        <ClipboardCopy className="h-4 w-4" /> <span className="hidden sm:inline">Copy</span>
      </button>
      <button
        className={zipPrimary ? ghost : primary}
        disabled={blocked}
        title={blockedTitle ?? (zipPrimary ? "DAG file only — this DAG also needs its SQL/config/script files, prefer the .zip" : "Download the DAG file")}
        onClick={onDownloadPy}
      >
        <FileCode className="h-4 w-4" /> .py
      </button>
      <button
        className={zipPrimary ? primary : ghost}
        disabled={blocked}
        title={blockedTitle ?? "dags/, sql/, scripts/, config/ and a deployment README in the Composer bucket layout"}
        onClick={onDownloadZip}
      >
        <Package className="h-4 w-4" /> .zip
      </button>
    </div>
  );
}
