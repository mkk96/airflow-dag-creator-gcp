import CodeMirror from "@uiw/react-codemirror";
import { useId, useRef } from "react";
import { languageFor } from "../lib/languages";
import type { FieldSchema, ParamValue, Params } from "../scenarios/types";
import type { Issues } from "../validation/rules";

interface Props {
  field: FieldSchema;
  value: ParamValue | undefined;
  params: Params;
  issues?: Issues;
  onChange: (value: ParamValue) => void;
}

const MAX_UPLOAD_BYTES = 1024 * 1024;

const inputBase =
  "w-full rounded-md border bg-white px-3 py-1.5 text-sm text-slate-900 shadow-sm outline-none transition " +
  "placeholder:text-slate-400 focus:border-blue-500 focus:ring-2 focus:ring-blue-500/20";

export function Field({ field, value, params, issues, onChange }: Props) {
  const id = useId();
  const fileRef = useRef<HTMLInputElement>(null);
  const hasError = !!issues?.errors.length;
  const hasWarning = !hasError && !!issues?.warnings.length;
  const border = hasError ? "border-red-400" : hasWarning ? "border-amber-400" : "border-slate-300";
  const help = typeof field.help === "function" ? field.help(params) : field.help;
  const listId = field.suggestions ? `${id}-list` : undefined;

  const onUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    e.target.value = "";
    if (!file) return;
    if (file.size > MAX_UPLOAD_BYTES) {
      alert("File is larger than 1 MB.");
      return;
    }
    file.text().then((text) => onChange(text));
  };

  let control: React.ReactNode;
  switch (field.type) {
    case "boolean":
      control = (
        <label htmlFor={id} className="inline-flex cursor-pointer items-center gap-2 text-sm text-slate-800">
          <input
            id={id}
            type="checkbox"
            className="h-4 w-4 rounded border-slate-300 accent-blue-600"
            checked={value === true}
            onChange={(e) => onChange(e.target.checked)}
          />
          {field.label}
        </label>
      );
      break;
    case "select":
      control = (
        <select id={id} className={`${inputBase} ${border}`} value={String(value ?? "")} onChange={(e) => onChange(e.target.value)}>
          {field.options?.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>
      );
      break;
    case "multiselect": {
      const selected = new Set(Array.isArray(value) ? value : []);
      control = (
        <div className="flex flex-col gap-1">
          {field.options?.map((o) => (
            <label key={o.value} className="inline-flex items-center gap-2 text-sm text-slate-800">
              <input
                type="checkbox"
                className="h-4 w-4 accent-blue-600"
                checked={selected.has(o.value)}
                onChange={(e) => {
                  const next = new Set(selected);
                  if (e.target.checked) next.add(o.value);
                  else next.delete(o.value);
                  onChange(field.options!.map((x) => x.value).filter((x) => next.has(x)));
                }}
              />
              {o.label}
            </label>
          ))}
        </div>
      );
      break;
    }
    case "list":
      control = (
        <textarea
          id={id}
          rows={Math.min(6, Math.max(2, (Array.isArray(value) ? value.length : 0) + 1))}
          className={`${inputBase} ${border} font-mono text-[13px]`}
          placeholder={field.placeholder ?? "One per line"}
          value={Array.isArray(value) ? value.join("\n") : ""}
          onChange={(e) => onChange(e.target.value.split("\n"))}
          spellCheck={false}
        />
      );
      break;
    case "textarea":
      control = (
        <textarea
          id={id}
          rows={3}
          className={`${inputBase} ${border}`}
          value={String(value ?? "")}
          onChange={(e) => onChange(e.target.value)}
        />
      );
      break;
    case "number":
      control = (
        <input
          id={id}
          type="number"
          className={`${inputBase} ${border}`}
          min={field.min}
          max={field.max}
          step={1}
          value={value === "" || value === undefined ? "" : Number(value)}
          onChange={(e) => onChange(e.target.value === "" ? "" : Number(e.target.value))}
        />
      );
      break;
    case "code":
      control = (
        <div className={`overflow-hidden rounded-md border ${border}`}>
          <div className="flex items-center justify-between border-b border-slate-200 bg-slate-50 px-2 py-1 text-xs text-slate-500">
            <span>{field.language === "sql" ? "BigQuery SQL" : "JSON"}</span>
            {field.accept && (
              <>
                <button
                  type="button"
                  className="rounded px-2 py-0.5 font-medium text-blue-700 hover:bg-blue-50"
                  onClick={() => fileRef.current?.click()}
                >
                  Upload {field.accept}
                </button>
                <input ref={fileRef} type="file" accept={field.accept} className="hidden" onChange={onUpload} />
              </>
            )}
          </div>
          <CodeMirror
            value={String(value ?? "")}
            height="220px"
            extensions={languageFor(field.language ?? "sql")}
            onChange={(v) => onChange(v)}
            basicSetup={{ foldGutter: false, highlightActiveLine: false }}
            className="text-[13px]"
          />
        </div>
      );
      break;
    default:
      control = (
        <>
          <input
            id={id}
            type={field.type === "date" ? "date" : "text"}
            className={`${inputBase} ${border} ${field.type === "text" ? "font-mono text-[13px]" : ""}`}
            placeholder={field.placeholder}
            value={String(value ?? "")}
            list={listId}
            onChange={(e) => onChange(e.target.value)}
            spellCheck={false}
          />
          {field.suggestions && (
            <datalist id={listId}>
              {field.suggestions.map((s) => (
                <option key={s} value={s} />
              ))}
            </datalist>
          )}
        </>
      );
  }

  return (
    <div className="flex flex-col gap-1">
      {field.type !== "boolean" && (
        <label htmlFor={id} className="flex items-center gap-1.5 text-[13px] font-medium text-slate-700">
          {field.label}
          {field.required && <span className="text-red-500">*</span>}
          {field.envSpecific && (
            <span
              className="rounded bg-violet-50 px-1.5 py-px text-[10px] font-semibold tracking-wide text-violet-700 uppercase"
              title="Env-specific: stored in the per-env config / Airflow Variable"
            >
              env
            </span>
          )}
        </label>
      )}
      {control}
      {help && <p className="text-xs leading-snug text-slate-500">{help}</p>}
      {issues?.errors.map((m) => (
        <p key={m} className="text-xs font-medium text-red-600">
          {m}
        </p>
      ))}
      {issues?.warnings.map((m) => (
        <p key={m} className="text-xs text-amber-700">
          ⚠ {m}
        </p>
      ))}
    </div>
  );
}
