/**
 * Field validators. Each returns { errors, warnings }:
 *  - errors   = breaks an official GCP / Airflow rule (blocks download)
 *  - warnings = breaks the team convention in src/config/naming.ts (informational)
 */
import { ENVIRONMENTS, OFFICIAL, TEAM, teamRegex } from "../config/naming";

export interface Issues {
  errors: string[];
  warnings: string[];
}

export const ok = (): Issues => ({ errors: [], warnings: [] });
export const err = (...errors: string[]): Issues => ({ errors, warnings: [] });
export const warn = (...warnings: string[]): Issues => ({ errors: [], warnings });

export function merge(...all: Issues[]): Issues {
  return {
    errors: all.flatMap((i) => i.errors),
    warnings: all.flatMap((i) => i.warnings),
  };
}

const utf8Bytes = (s: string) => new TextEncoder().encode(s).length;

// ---------------------------------------------------------------------------
// Jinja safety
// ---------------------------------------------------------------------------

const JINJA_RE = /\{\{|\{%|\{#|%\}|#\}/;

/** Values flowing into templated operator fields must not inject Jinja. */
export function noJinja(value: string): Issues {
  return JINJA_RE.test(value)
    ? err("Jinja delimiters ({{ }}, {% %}, {# #}) are not allowed in this field.")
    : ok();
}

// ---------------------------------------------------------------------------
// Airflow
// ---------------------------------------------------------------------------

export function dagId(value: string): Issues {
  if (!value) return err("dag_id is required.");
  if (value.length > OFFICIAL.airflowKey.maxLength) return err("dag_id must be at most 250 characters.");
  if (!OFFICIAL.airflowKey.re.test(value)) {
    return err("dag_id may only contain letters, digits, '_', '.', and '-'.");
  }
  if (!teamRegex("dagId").test(value)) {
    return warn(`Team convention: ${TEAM.patterns.dagId} (e.g. sales_gcs_to_bq_daily).`);
  }
  return ok();
}

export function taskId(value: string): Issues {
  if (!value) return err("task_id is required.");
  if (value.length > OFFICIAL.airflowKey.maxLength || !OFFICIAL.airflowKey.re.test(value)) {
    return err("task_id may only contain letters, digits, '_', '.', and '-' (max 250).");
  }
  if (!teamRegex("taskId").test(value)) {
    return warn(`Team convention: ${TEAM.patterns.taskId} in snake_case (e.g. load_orders_to_staging).`);
  }
  return ok();
}

// ---------------------------------------------------------------------------
// Cron
// ---------------------------------------------------------------------------

const MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];
const DAYS = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

interface CronField {
  name: string;
  min: number;
  max: number;
  names?: string[];
  namesOffset?: number;
}

const CRON_FIELDS: CronField[] = [
  { name: "minute", min: 0, max: 59 },
  { name: "hour", min: 0, max: 23 },
  { name: "day-of-month", min: 1, max: 31 },
  { name: "month", min: 1, max: 12, names: MONTHS, namesOffset: 1 },
  { name: "day-of-week", min: 0, max: 7, names: DAYS, namesOffset: 0 },
];

function cronValue(token: string, f: CronField): number | null {
  if (/^\d+$/.test(token)) {
    const n = Number(token);
    return n >= f.min && n <= f.max ? n : null;
  }
  const idx = f.names?.indexOf(token.toUpperCase()) ?? -1;
  return idx >= 0 ? idx + (f.namesOffset ?? 0) : null;
}

function cronPart(part: string, f: CronField): string | null {
  const [rangePart, step, ...rest] = part.split("/");
  if (rest.length) return `invalid step in "${part}"`;
  if (step !== undefined && (!/^\d+$/.test(step) || Number(step) < 1)) {
    return `step must be a positive integer in "${part}"`;
  }
  if (rangePart === "*") return null;
  if (f.name === "day-of-month" && rangePart === "L") return null;
  if (f.name === "day-of-week" && /^[0-7]#[1-5]$/.test(rangePart)) return null;
  const bounds = rangePart.split("-");
  if (bounds.length > 2) return `invalid range "${rangePart}"`;
  const vals = bounds.map((b) => cronValue(b, f));
  if (vals.some((v) => v === null)) return `"${rangePart}" is out of range ${f.min}-${f.max}`;
  if (vals.length === 2 && vals[0]! > vals[1]!) return `range "${rangePart}" is reversed`;
  return null;
}

export function cron(value: string): Issues {
  const v = value.trim();
  if (!v) return err("Cron expression is required.");
  const fields = v.split(/\s+/);
  if (fields.length !== 5) return err("Cron must have 5 fields: minute hour day-of-month month day-of-week.");
  const problems: string[] = [];
  fields.forEach((field, i) => {
    const spec = CRON_FIELDS[i];
    for (const part of field.split(",")) {
      const p = cronPart(part, spec);
      if (p) problems.push(`${spec.name}: ${p}`);
    }
  });
  return problems.length ? { errors: problems, warnings: [] } : ok();
}

// ---------------------------------------------------------------------------
// GCS
// ---------------------------------------------------------------------------

export function gcsBucket(value: string): Issues {
  const r = OFFICIAL.gcsBucket;
  if (!value) return err("Bucket name is required.");
  if (value.startsWith("gs://")) return err("Enter the bucket name only, without gs://.");
  const errors: string[] = [];
  const hasDots = value.includes(".");
  const max = hasDots ? r.maxLengthWithDots : r.maxLength;
  if (value.length < r.minLength || value.length > max) {
    errors.push(`Bucket names must be ${r.minLength}-${max} characters.`);
  }
  if (!r.re.test(value)) {
    errors.push("Use lowercase letters, digits, '-', '_' or '.'; start and end with a letter or digit.");
  }
  if (hasDots && value.split(".").some((c) => c.length === 0 || c.length > r.maxComponentLength)) {
    errors.push("Each dot-separated part must be 1-63 characters.");
  }
  if (/\.\.|\.-|-\./.test(value)) errors.push("Bucket names cannot contain '..', '.-' or '-.'.");
  if (/^\d{1,3}(\.\d{1,3}){3}$/.test(value)) errors.push("Bucket names cannot look like an IP address.");
  if (value.startsWith(r.forbiddenPrefix)) errors.push(`Bucket names cannot start with "${r.forbiddenPrefix}".`);
  if (r.forbiddenContains.some((w) => value.includes(w))) errors.push('Bucket names cannot contain "google".');
  if (errors.length) return { errors, warnings: [] };
  if (!teamRegex("gcsBucket").test(value)) {
    return warn(`Team convention: ${TEAM.patterns.gcsBucket} (e.g. acme-prd-sales-landing).`);
  }
  return ok();
}

/** Object name or prefix inside a bucket. Wildcards (*) and Jinja are allowed. */
export function gcsObject(value: string): Issues {
  if (!value) return err("Object path is required.");
  if (value.startsWith("gs://")) return err("Enter the path inside the bucket, without gs://bucket/.");
  if (/[\r\n]/.test(value)) return err("Object names cannot contain line breaks.");
  if (utf8Bytes(value) > OFFICIAL.gcsObject.maxBytes) return err("Object names are limited to 1024 bytes.");
  if (value === "." || value === "..") return err('Object names cannot be "." or "..".');
  if (value.startsWith(".well-known/acme-challenge/")) return err("Reserved object prefix.");
  if (value.startsWith("/")) return warn("Object paths normally do not start with '/'.");
  return ok();
}

/** Full gs://bucket/object URI. */
export function gsUri(value: string): Issues {
  const m = /^gs:\/\/([^/]+)\/(.+)$/.exec(value);
  if (!m) return err("Must be a gs:// URI like gs://my-bucket/path/file.json.");
  const b = gcsBucket(m[1]);
  const o = gcsObject(m[2]);
  return { errors: [...b.errors, ...o.errors], warnings: [...b.warnings, ...o.warnings] };
}

export function parseGsUri(value: string): { bucket: string; object: string } | null {
  const m = /^gs:\/\/([^/]+)\/(.+)$/.exec(value);
  return m ? { bucket: m[1], object: m[2] } : null;
}

// ---------------------------------------------------------------------------
// BigQuery
// ---------------------------------------------------------------------------

export function bqProject(value: string): Issues {
  if (!value) return err("Project ID is required.");
  if (!OFFICIAL.bqProject.re.test(value)) {
    return err("Project IDs are 6-30 characters: lowercase letters, digits, hyphens; start with a letter; no trailing hyphen.");
  }
  return ok();
}

export function bqDataset(value: string): Issues {
  if (!value) return err("Dataset is required.");
  if (value.length > OFFICIAL.bqDataset.maxLength || !OFFICIAL.bqDataset.re.test(value)) {
    return err("Datasets may only contain letters, digits and underscores (max 1024).");
  }
  if (!teamRegex("bqDataset").test(value)) {
    return warn(`Team convention: ${TEAM.patterns.bqDataset} in snake_case (e.g. raw_sales).`);
  }
  return ok();
}

export function bqTable(value: string): Issues {
  if (!value) return err("Table is required.");
  if (utf8Bytes(value) > OFFICIAL.bqTable.maxBytes || !OFFICIAL.bqTable.re.test(value)) {
    return err("Tables may contain letters, marks, digits, '_', '-' and spaces (max 1024 bytes). No '$' — partition decorators are added automatically.");
  }
  if (!teamRegex("bqTable").test(value)) {
    return warn("Team convention: snake_case noun (e.g. orders, customer_daily_snapshot).");
  }
  return ok();
}

export function bqColumn(value: string): Issues {
  if (!value) return ok();
  if (value.length > OFFICIAL.bqColumn.maxLength || !OFFICIAL.bqColumn.re.test(value)) {
    return err("Column names start with a letter or '_' and contain only letters, digits and '_'.");
  }
  return ok();
}

/** Validate a BigQuery JSON schema (array of field objects), as exported by `bq show --schema`. */
export function bqSchemaJson(value: string): Issues {
  if (!value.trim()) return err("Paste or upload a BigQuery JSON schema.");
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch (e) {
    return err(`Invalid JSON: ${(e as Error).message}`);
  }
  if (!Array.isArray(parsed) || parsed.length === 0) {
    return err('Schema must be a non-empty JSON array, e.g. [{"name": "id", "type": "INT64"}].');
  }
  const TYPES = new Set([
    "STRING", "BYTES", "INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC",
    "BOOLEAN", "BOOL", "TIMESTAMP", "DATE", "TIME", "DATETIME", "GEOGRAPHY", "JSON",
    "RECORD", "STRUCT", "INTERVAL", "RANGE",
  ]);
  const errors: string[] = [];
  const walk = (fields: unknown[], path: string) => {
    fields.forEach((f, i) => {
      const at = `${path}[${i}]`;
      if (typeof f !== "object" || f === null || Array.isArray(f)) {
        errors.push(`${at} must be an object.`);
        return;
      }
      const o = f as Record<string, unknown>;
      if (typeof o.name !== "string" || !OFFICIAL.bqColumn.re.test(o.name)) errors.push(`${at}.name is invalid.`);
      if (typeof o.type !== "string" || !TYPES.has(o.type.toUpperCase())) errors.push(`${at}.type is invalid.`);
      if (o.mode !== undefined && !["NULLABLE", "REQUIRED", "REPEATED"].includes(String(o.mode).toUpperCase())) {
        errors.push(`${at}.mode must be NULLABLE, REQUIRED or REPEATED.`);
      }
      if (o.fields !== undefined) {
        if (!Array.isArray(o.fields)) errors.push(`${at}.fields must be an array.`);
        else walk(o.fields, `${at}.fields`);
      }
    });
  };
  walk(parsed, "schema");
  return errors.length ? { errors: errors.slice(0, 5), warnings: [] } : ok();
}

// ---------------------------------------------------------------------------
// Misc
// ---------------------------------------------------------------------------

export function email(value: string): Issues {
  return OFFICIAL.email.re.test(value) ? ok() : err(`"${value}" is not a valid email address.`);
}

export function serviceAccount(value: string): Issues {
  if (!OFFICIAL.email.re.test(value)) return err(`"${value}" is not a valid service account email.`);
  if (!OFFICIAL.serviceAccount.re.test(value)) {
    return warn(`"${value}" is not a user-managed service account (*.iam.gserviceaccount.com).`);
  }
  return ok();
}

export function labelValue(value: string, field = "Value"): Issues {
  if (!OFFICIAL.labelValue.re.test(value)) {
    return err(`${field} must be lowercase letters, digits, '_' or '-' (max 63) to be used as a GCP label.`);
  }
  return ok();
}

export function labelKey(value: string): Issues {
  if (!OFFICIAL.labelKey.re.test(value)) {
    return err(`Label key "${value}" must start with a lowercase letter and use lowercase letters, digits, '_' or '-' (max 63).`);
  }
  return ok();
}

export function envCode(value: string): Issues {
  return (ENVIRONMENTS as readonly string[]).includes(value) ? ok() : err(`Environment must be one of ${ENVIRONMENTS.join(", ")}.`);
}

export function bqLocation(value: string): Issues {
  if (!value) return err("Location is required.");
  if (!/^(US|EU|[a-z]+(-[a-z]+)+\d+)$/.test(value)) {
    return err('Use a BigQuery location such as "US", "EU" or "northamerica-northeast1".');
  }
  return ok();
}

export function timezone(value: string): Issues {
  if (!value) return err("Timezone is required.");
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: value });
    return ok();
  } catch {
    return err(`"${value}" is not a valid IANA timezone (e.g. UTC, America/Toronto).`);
  }
}

export function isoDate(value: string): Issues {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (!m) return err("Use YYYY-MM-DD.");
  const d = new Date(Date.UTC(+m[1], +m[2] - 1, +m[3]));
  if (d.getUTCFullYear() !== +m[1] || d.getUTCMonth() !== +m[2] - 1 || d.getUTCDate() !== +m[3]) {
    return err("Not a real calendar date.");
  }
  return ok();
}

export function connId(value: string): Issues {
  if (!value) return err("Connection id is required.");
  return /^[A-Za-z0-9_.-]+$/.test(value) ? ok() : err("Connection ids use letters, digits, '_', '.', '-'.");
}
