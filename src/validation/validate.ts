/** Form-level validation: runs every visible field's rules plus cross-field checks. */
import { allFields, isVisible, validateCommon } from "../scenarios/common";
import type { FieldSchema, ParamValue, Params, Scenario } from "../scenarios/types";
import { type Issues, merge, noJinja, ok } from "./rules";

export type ValidationResult = Record<string, Issues>;

function isEmpty(v: ParamValue | undefined): boolean {
  if (v === undefined || v === null) return true;
  if (Array.isArray(v)) return v.filter((x) => x.trim() !== "").length === 0;
  return typeof v === "string" && v.trim() === "";
}

export function validateField(f: FieldSchema, value: ParamValue | undefined, p: Params): Issues {
  if (isEmpty(value)) return f.required ? { errors: [`${f.label} is required.`], warnings: [] } : ok();
  const parts: Issues[] = [];
  if (f.type === "number") {
    const n = Number(value);
    if (!Number.isInteger(n)) return { errors: ["Enter a whole number."], warnings: [] };
    if (f.min !== undefined && n < f.min) parts.push({ errors: [`Minimum is ${f.min}.`], warnings: [] });
    if (f.max !== undefined && n > f.max) parts.push({ errors: [`Maximum is ${f.max}.`], warnings: [] });
  }
  if (f.type === "select" && f.options && !f.options.some((o) => o.value === value)) {
    parts.push({ errors: ["Choose one of the listed options."], warnings: [] });
  }
  const items = Array.isArray(value) ? value.map((x) => x.trim()).filter(Boolean) : null;
  if (f.type === "multiselect" && f.options && items) {
    const bad = items.filter((x) => !f.options!.some((o) => o.value === x));
    if (bad.length) parts.push({ errors: [`Unknown option(s): ${bad.join(", ")}`], warnings: [] });
  }
  if (!f.allowJinja) {
    const texts = items ?? (typeof value === "string" ? [value] : []);
    for (const t of texts) {
      const j = noJinja(t);
      if (j.errors.length) {
        parts.push(j);
        break;
      }
    }
  }
  if (items && f.validateItem) items.forEach((it) => parts.push(f.validateItem!(it, p)));
  if (f.validate) parts.push(f.validate(value as ParamValue, p));
  const m = merge(...parts);
  return { errors: [...new Set(m.errors)], warnings: [...new Set(m.warnings)] };
}

export function validateParams(s: Scenario, p: Params): ValidationResult {
  const out: ValidationResult = {};
  for (const f of allFields(s)) {
    if (!isVisible(f, p)) continue;
    out[f.key] = validateField(f, p[f.key], p);
  }
  const cross = [validateCommon(p), s.validate?.(p) ?? {}];
  for (const c of cross) {
    for (const [k, i] of Object.entries(c)) out[k] = out[k] ? merge(out[k], i) : i;
  }
  return out;
}

export function countIssues(r: ValidationResult): { errors: number; warnings: number } {
  let errors = 0;
  let warnings = 0;
  for (const i of Object.values(r)) {
    errors += i.errors.length;
    warnings += i.warnings.length;
  }
  return { errors, warnings };
}
