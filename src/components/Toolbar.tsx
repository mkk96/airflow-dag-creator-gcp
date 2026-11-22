import { useRef } from "react";
import type { GeneratedBundle } from "../scenarios/types";

interface Props {
  bundle: GeneratedBundle | null;
  errors: number;
  warnings: number;
  onDownloadPy: () => void;
  onDownloadZip: () => void;
  onCopy: () => void;
  onExport: () => void;
  onImport: (text: string) => void;
}

const btn =
  "inline-flex items-center gap-1.5 rounded-md border px-3 py-1.5 text-sm font-medium shadow-sm transition " +
  "disabled:cursor-not-allowed disabled:opacity-50";
const secondary = `${btn} border-slate-300 bg-white text-slate-700 hover:bg-slate-50`;
const primary = `${btn} border-blue-600 bg-blue-600 text-white hover:bg-blue-700`;

export function Toolbar({ bundle, errors, warnings, onDownloadPy, onDownloadZip, onCopy, onExport, onImport }: Props) {
  const fileRef = useRef<HTMLInputElement>(null);
  const blocked = errors > 0 || !bundle;
  const blockedTitle = errors > 0 ? "Fix the errors in the form first" : undefined;
  const zipPrimary = !!bundle?.needsZip;

  return (
    <div className="flex flex-wrap items-center gap-2">
      <span
        className={`mr-auto inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-semibold ${
          errors > 0
            ? "bg-red-100 text-red-700"
            : warnings > 0
              ? "bg-amber-100 text-amber-800"
              : "bg-emerald-100 text-emerald-800"
        }`}
      >
        {errors > 0
          ? `${errors} error${errors === 1 ? "" : "s"}`
          : warnings > 0
            ? `Valid · ${warnings} convention warning${warnings === 1 ? "" : "s"}`
            : "Valid"}
      </span>

      <button
        className={zipPrimary ? secondary : primary}
        disabled={blocked}
        title={
          blockedTitle ??
          (zipPrimary ? "DAG file only. This DAG also needs its SQL/config files — prefer the .zip" : undefined)
        }
        onClick={onDownloadPy}
      >
        ⬇ .py
      </button>
      <button
        className={zipPrimary ? primary : secondary}
        disabled={blocked}
        title={blockedTitle ?? "dags/, sql/, config/ and README in the Composer bucket layout"}
        onClick={onDownloadZip}
      >
        ⬇ .zip
      </button>
      <button className={secondary} disabled={!bundle} onClick={onCopy} title="Copy the DAG .py to the clipboard">
        Copy
      </button>
      <span className="mx-1 h-6 w-px bg-slate-200" aria-hidden />
      <button className={secondary} onClick={onExport} title="Save the form as JSON">
        Export JSON
      </button>
      <button className={secondary} onClick={() => fileRef.current?.click()} title="Load a saved JSON config">
        Import JSON
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
    </div>
  );
}
