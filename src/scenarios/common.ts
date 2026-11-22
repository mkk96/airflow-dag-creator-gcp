/**
 * Shared building blocks for every scenario:
 *  - the common parameter schema (DAG, schedule, retries, GCP, environment/labels)
 *  - DagContext: env-config references, imports, SQL files, labels
 *  - assembly of the final DAG file, variables JSON and bundle README
 *
 * Scenarios only describe their own fields and tasks; everything here is shared.
 */
import { DEPLOY_ENV_VAR, ENVIRONMENTS, type EnvCode, swapEnv, toLabelValue, TEAM } from "../config/naming";
import { GENERATOR_NAME, RUNTIME } from "../config/runtime";
import { SCHEDULE_PRESETS } from "../config/schedules";
import {
  assign,
  comment,
  pyBool,
  pyCall,
  pyDict,
  pyDocstring,
  pyFString,
  pyIdent,
  pyIndex,
  pyInt,
  pyList,
  pyName,
  pyNone,
  pyStr,
  pyTuple,
  textWidth,
  LINE_LENGTH,
  withHeader,
  type Py,
} from "../lib/python";
import * as v from "../validation/rules";
import type { DeployInfo, FieldSchema, GeneratedBundle, GeneratedFile, Params, Scenario, Section } from "./types";

// ---------------------------------------------------------------------------
// Param accessors
// ---------------------------------------------------------------------------

export const str = (p: Params, k: string): string => String(p[k] ?? "").trim();
export const num = (p: Params, k: string): number => Number(p[k] ?? 0);
export const bool = (p: Params, k: string): boolean => p[k] === true;
export const list = (p: Params, k: string): string[] =>
  Array.isArray(p[k]) ? (p[k] as string[]).map((x) => x.trim()).filter(Boolean) : [];

// ---------------------------------------------------------------------------
// template_ext guard
// ---------------------------------------------------------------------------

/** `template_ext` of BigQueryInsertJobOperator (providers-google 12.x). */
export const BQ_INSERT_JOB_TEMPLATE_EXT = [".json", ".sql"] as const;
/** `template_ext` of BigQueryCheckOperator / SQL check operators. */
export const SQL_TEMPLATE_EXT = [".sql"] as const;

/**
 * Airflow treats a templated string that ends with one of the operator's
 * `template_ext` (e.g. "gs://b/x/*.json") as a template FILE path and fails
 * with TemplateNotFound. Emit the extension as a Jinja literal instead:
 * "gs://b/x/*{{ '.json' }}" renders to the same value but is not a file path.
 */
export function guardTemplateExt(value: string, exts: readonly string[]): string {
  const ext = exts.find((e) => value.endsWith(e));
  return ext ? `${value.slice(0, -ext.length)}{{ '${ext}' }}` : value;
}

// ---------------------------------------------------------------------------
// Common schema
// ---------------------------------------------------------------------------

export const COMMON_SECTIONS: Section[] = [
  { id: "dag", title: "DAG" },
  {
    id: "env",
    title: "Environment, naming & labels",
    description: "Drives env-specific config, GCP cost labels and the “Suggest names” button.",
  },
  { id: "schedule", title: "Schedule" },
  { id: "reliability", title: "Retries, timeouts & alerts" },
  { id: "gcp", title: "GCP connection" },
];

const BQ_LOCATIONS = [
  "US", "EU", "northamerica-northeast1", "northamerica-northeast2", "us-central1", "us-east1",
  "us-east4", "us-west1", "europe-west1", "europe-west2", "europe-west3", "asia-northeast1",
  "asia-southeast1", "australia-southeast1",
];

export const COMMON_FIELDS: FieldSchema[] = [
  // DAG ---------------------------------------------------------------------
  {
    key: "dag_id", label: "DAG id", type: "text", section: "dag", default: "my_gcs_to_bq_daily",
    required: true, suggestable: true, validate: (x) => v.dagId(String(x)),
    help: "Also the file name (<dag_id>.py). Convention: {domain}_{source}_to_{target}_{frequency}.",
  },
  {
    key: "description", label: "Description", type: "text", section: "dag", default: "",
    required: true, allowJinja: true, help: "One line; shown in the Airflow UI and in doc_md.",
  },
  {
    key: "owner", label: "Owner", type: "text", section: "dag", default: "data-eng", required: true,
    help: "Airflow owner (team). Also applied as the `owner` GCP label (lowercased).",
  },
  {
    key: "email", label: "Alert emails", type: "list", section: "dag", default: [],
    validateItem: (x) => v.email(x), help: "One per line. Also used as the contact in doc_md.",
  },
  {
    key: "tags", label: "Tags", type: "list", section: "dag", default: [],
    validateItem: (x) => (x.length > 100 ? v.err("Tags are limited to 100 characters.") : v.ok()),
    help: "One per line.",
  },
  // Environment / naming / labels -----------------------------------------------
  {
    key: "config_mode", label: "Environment config source", type: "select", section: "env",
    default: "config_dict",
    options: [
      { value: "config_dict", label: "Per-env config dict inside the DAG (drop-in, no setup)" },
      { value: "variables", label: "Airflow Variables with env prefix (JSON, imported separately)" },
    ],
    help: `Either way the DAG picks dev/stg/prd from the ${DEPLOY_ENV_VAR} environment variable and values are resolved by Jinja at run time.`,
  },
  {
    key: "env", label: "Values below are for env", type: "select", section: "env", default: "dev",
    options: ENVIRONMENTS.map((e) => ({ value: e, label: e })),
    help: "Values for the other environments are derived by swapping this env code (e.g. acme-dev-x → acme-prd-x). Edit the generated config if they differ.",
  },
  {
    key: "org", label: "Org code", type: "text", section: "env", default: "acme",
    validate: (x) => (/^[a-z0-9]+$/.test(String(x)) ? v.ok() : v.warn("Use a short lowercase code, e.g. acme.")),
  },
  {
    key: "domain", label: "Domain", type: "text", section: "env", default: "sales",
    validate: (x) => (/^[a-z0-9]+$/.test(String(x)) ? v.ok() : v.warn("Use a short lowercase code, e.g. sales.")),
  },
  {
    key: "team", label: "Team label", type: "text", section: "env", default: "data-eng", required: true,
    validate: (x) => v.labelValue(String(x), "Team"),
  },
  {
    key: "cost_center", label: "Cost center label", type: "text", section: "env", default: "cc-0000",
    required: true, validate: (x) => v.labelValue(String(x), "Cost center"),
  },
  // Schedule ---------------------------------------------------------------------
  {
    key: "schedule_preset", label: "Schedule", type: "select", section: "schedule", default: "@daily",
    options: SCHEDULE_PRESETS.map((o) => ({ value: o.value, label: o.label })),
  },
  {
    key: "schedule_cron", label: "Cron expression", type: "text", section: "schedule",
    default: "0 6 * * *", required: true, placeholder: "m h dom mon dow",
    showIf: (p) => p.schedule_preset === "custom", validate: (x) => v.cron(String(x)),
    help: "5 fields: minute hour day-of-month month day-of-week (evaluated in the DAG timezone).",
  },
  {
    key: "start_date", label: "Start date", type: "date", section: "schedule", default: "2025-01-01",
    required: true, validate: (x) => v.isoDate(String(x)),
  },
  {
    key: "timezone", label: "Timezone", type: "text", section: "schedule", default: "UTC",
    required: true, suggestions: ["UTC", "America/Toronto", "America/Vancouver", "America/New_York", "Europe/London"],
    validate: (x) => v.timezone(String(x)),
  },
  {
    key: "catchup", label: "Catchup (backfill missed runs)", type: "boolean", section: "schedule", default: false,
  },
  {
    key: "max_active_runs", label: "Max active runs", type: "number", section: "schedule", default: 1, min: 1, max: 64,
  },
  {
    key: "dagrun_timeout_minutes", label: "DAG run timeout (minutes)", type: "number", section: "schedule",
    default: 180, min: 1, max: 10080,
  },
  // Reliability --------------------------------------------------------------------
  { key: "retries", label: "Retries", type: "number", section: "reliability", default: 2, min: 0, max: 10 },
  {
    key: "retry_delay_minutes", label: "Retry delay (minutes)", type: "number", section: "reliability",
    default: 5, min: 1, max: 1440,
  },
  {
    key: "retry_exponential_backoff", label: "Exponential retry backoff", type: "boolean", section: "reliability",
    default: true,
  },
  {
    key: "max_retry_delay_minutes", label: "Max retry delay (minutes)", type: "number", section: "reliability",
    default: 60, min: 1, max: 1440, showIf: (p) => p.retry_exponential_backoff === true,
  },
  {
    key: "execution_timeout_minutes", label: "Task execution timeout (minutes)", type: "number",
    section: "reliability", default: 60, min: 1, max: 1440, help: "Applied to every task via default_args.",
  },
  {
    key: "email_on_failure", label: "Email on failure", type: "boolean", section: "reliability", default: true,
    help: "Requires email (SendGrid/SMTP) to be configured in Composer.",
  },
  // GCP ---------------------------------------------------------------------------
  {
    key: "gcp_conn_id", label: "GCP connection id", type: "text", section: "gcp",
    default: "google_cloud_default", required: true, validate: (x) => v.connId(String(x)),
  },
  {
    key: "project_id", label: "Project id", type: "text", section: "gcp", default: "acme-dev-data",
    required: true, envSpecific: true, validate: (x) => v.bqProject(String(x)),
    help: "Project that runs the jobs and owns the destination. Env-specific.",
  },
  {
    key: "location", label: "Location / region", type: "text", section: "gcp", default: "US",
    required: true, suggestions: BQ_LOCATIONS, validate: (x) => v.bqLocation(String(x)),
    help: "BigQuery location (US, EU) or GCP region.",
  },
  {
    key: "impersonation_chain", label: "Impersonation chain (optional)", type: "list", section: "gcp",
    default: [], envSpecific: true, validateItem: (x) => v.serviceAccount(x),
    help: "Service account(s) to impersonate, one per line, last one is used. No keys are ever embedded.",
  },
  {
    key: "deferrable", label: "Use deferrable operators", type: "boolean", section: "gcp", default: true,
    help: "Frees worker slots while GCP jobs run. Requires the Airflow triggerer to be enabled in Composer.",
  },
];

/** Common checks that need more than one field. */
export function validateCommon(p: Params): Record<string, v.Issues> {
  const out: Record<string, v.Issues> = {};
  if (bool(p, "catchup")) {
    out.catchup = v.warn("Catchup will schedule a run for every interval since start_date.");
  }
  if (bool(p, "email_on_failure") && list(p, "email").length === 0) {
    out.email = v.warn("Email on failure is enabled but no alert email is set.");
  }
  if (num(p, "max_active_runs") > 1) {
    out.max_active_runs = v.warn("Standard is 1 to keep runs ordered and idempotent.");
  }
  if (str(p, "owner") && toLabelValue(str(p, "owner")) !== str(p, "owner")) {
    out.owner = v.warn(`Will be applied as label owner="${toLabelValue(str(p, "owner"))}".`);
  }
  return out;
}

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

export function allFields(s: Scenario): FieldSchema[] {
  return [...COMMON_FIELDS, ...s.fields];
}

export function allSections(s: Scenario): Section[] {
  return [...COMMON_SECTIONS, ...s.sections];
}

export function isVisible(f: FieldSchema, p: Params): boolean {
  return f.showIf ? f.showIf(p) : true;
}

function cloneValue<T>(x: T): T {
  return Array.isArray(x) ? (x.slice() as T) : x;
}

export function defaultParams(s: Scenario): Params {
  const p: Params = {};
  for (const f of allFields(s)) p[f.key] = cloneValue(f.default);
  for (const [k, val] of Object.entries(s.commonDefaults ?? {})) {
    if (val !== undefined) p[k] = cloneValue(val);
  }
  return p;
}

/** Defaults + a sample's overrides. */
export function sampleParams(s: Scenario, name: string): Params {
  const p = defaultParams(s);
  for (const [k, val] of Object.entries(s.samples[name] ?? {})) {
    if (val !== undefined) p[k] = cloneValue(val);
  }
  return p;
}

// ---------------------------------------------------------------------------
// DagContext
// ---------------------------------------------------------------------------

export type ConfigMode = "config_dict" | "variables";

export interface SqlFile {
  /** File name inside dags/sql/<dag_id>/, e.g. 01_check_orders.sql */
  name: string;
  content: string;
}

export interface AssembleOptions {
  /** Ordered pipeline steps for doc_md. */
  docSteps: string[];
  /** Module-level constants contributed by the scenario (already rendered). */
  constants?: string[];
  /** Body of `with DAG(...) as dag:` (already rendered at indent level 1). */
  body: string[];
  /** IAM roles the Composer / impersonated service account needs. */
  iamRoles: string[];
  /** Extra README notes. */
  readmeNotes?: string[];
}

/**
 * Per-generation helper handed to a scenario's generate(). It owns imports,
 * env-config references and SQL files so scenarios never hand-roll them.
 */
export class DagContext {
  readonly dagId: string;
  readonly mode: ConfigMode;
  readonly env: EnvCode;
  private readonly fromImports = new Map<string, Set<string>>();
  private readonly plainImports = new Set<string>(["logging", "os", "pendulum"]);
  private readonly sql: SqlFile[] = [];
  private readonly scripts: SqlFile[] = [];
  private codeBucketKey: string | null = null;

  constructor(
    readonly scenario: Scenario,
    readonly p: Params,
  ) {
    this.dagId = str(p, "dag_id");
    this.mode = str(p, "config_mode") === "variables" ? "variables" : "config_dict";
    this.env = (ENVIRONMENTS as readonly string[]).includes(str(p, "env")) ? (str(p, "env") as EnvCode) : "dev";
    this.importFrom("datetime", "timedelta");
    this.importFrom("airflow", "DAG");
  }

  importFrom(module: string, name: string): void {
    module.split(".").forEach(pyIdent);
    pyIdent(name);
    if (!this.fromImports.has(module)) this.fromImports.set(module, new Set());
    this.fromImports.get(module)!.add(name);
  }

  // -- env config --------------------------------------------------------------

  /** Env-specific keys: visible, non-empty envSpecific fields (common + scenario). */
  envKeys(): string[] {
    return allFields(this.scenario)
      .filter((f) => f.envSpecific && isVisible(f, this.p))
      .filter((f) => (Array.isArray(this.p[f.key]) ? list(this.p, f.key).length > 0 : str(this.p, f.key) !== ""))
      .map((f) => pyIdent(f.key));
  }

  envValues(env: EnvCode): Record<string, string | string[]> {
    const out: Record<string, string | string[]> = {};
    for (const k of this.envKeys()) {
      const val = this.p[k];
      out[k] = Array.isArray(val)
        ? list(this.p, k).map((x) => swapEnv(x, this.env, env))
        : swapEnv(str(this.p, k), this.env, env);
    }
    return out;
  }

  /**
   * Jinja expression for an env-specific value. ONLY use inside operator
   * fields listed in that operator's `template_fields`.
   */
  ref(key: string, index?: number): string {
    pyIdent(key);
    if (!this.envKeys().includes(key)) throw new Error(`"${key}" is not an env-specific key with a value`);
    if (index !== undefined && !Number.isSafeInteger(index)) throw new Error("index must be an integer");
    const sub = index === undefined ? "" : `[${index}]`;
    return this.mode === "variables"
      ? `{{ var.json.get(cfg_var).${key}${sub} }}`
      : `{{ cfg.${key}${sub} }}`;
  }

  /** Jinja expression for env values inside .sql files (after sqlPrelude()). */
  sqlRef(key: string): string {
    pyIdent(key);
    return `{{ cfg.${key} }}`;
  }

  /** Lines to put at the top of every .sql file so `cfg` works in both modes. */
  sqlPrelude(): string[] {
    return this.mode === "variables" ? ["{% set cfg = var.json.get(cfg_var) %}"] : [];
  }

  /** `deferrable=` value for operators/sensors that support it. */
  deferrable(): Py {
    return pyBool(bool(this.p, "deferrable"));
  }

  /** Python expression for impersonation_chain (templated field) or None. */
  impersonationChain(): Py {
    return pyName("IMPERSONATION_CHAIN");
  }

  /**
   * Sensor kwargs following the standards: deferrable when enabled, otherwise
   * mode="reschedule"; always a timeout and poke interval.
   */
  sensorKwargs(timeoutMinutes: number, pokeIntervalSeconds = 60): Array<[string, Py]> {
    const out: Array<[string, Py]> = bool(this.p, "deferrable")
      ? [["deferrable", pyBool(true)]]
      : [["mode", pyStr("reschedule")]];
    out.push(["poke_interval", pyInt(Math.max(1, Math.round(pokeIntervalSeconds)))]);
    out.push(["timeout", pyInt(Math.max(1, Math.round(timeoutMinutes * 60)))]);
    return out;
  }

  /**
   * Jinja template for a runtime-unique GCP resource name (Dataproc cluster,
   * Dataproc batch, Dataflow / Cloud Run job): lowercase dag_id with hyphens
   * plus a run-date suffix, within `maxLength` once rendered.
   *   suffix "ds" -> -YYYYMMDD       (one per day)
   *   suffix "ts" -> -YYYYMMDDtHHMMSS (one per run, e.g. hourly)
   */
  resourceName(opts: { prefix?: string; suffix?: "ds" | "ts"; maxLength?: number } = {}): string {
    const suffix = opts.suffix ?? "ds";
    const maxLength = opts.maxLength ?? 63;
    const suffixJinja = suffix === "ts" ? "{{ ts_nodash | lower }}" : "{{ ds_nodash }}";
    const suffixLen = (suffix === "ts" ? 15 : 8) + 1;
    let base = `${opts.prefix ? `${opts.prefix}-` : ""}${this.dagId}`
      .toLowerCase()
      .replace(/[^a-z0-9-]+/g, "-")
      .replace(/-+/g, "-")
      .replace(/^[^a-z]+/, "")
      .replace(/-+$/, "");
    if (!base) base = "airflow";
    base = base.slice(0, maxLength - suffixLen).replace(/-+$/, "");
    return `${base}-${suffixJinja}`;
  }

  // -- scripts ---------------------------------------------------------------------

  /**
   * Register a script (PySpark / Spark / Beam) shipped in the bundle under
   * scripts/<dag_id>/<name>; returns the gs:// URI (Jinja, templated fields only)
   * where it must be uploaded in the env-specific code bucket field `codeBucketKey`.
   */
  addScript(name: string, content: string, codeBucketKey: string): string {
    if (!/^[A-Za-z0-9_.-]+$/.test(name) || name.startsWith(".")) throw new Error(`Unsafe script name ${name}`);
    this.codeBucketKey = codeBucketKey;
    this.scripts.push({ name, content: content.endsWith("\n") ? content : content + "\n" });
    return `gs://${this.ref(codeBucketKey)}/scripts/${this.dagId}/${name}`;
  }

  // -- SQL ------------------------------------------------------------------------

  /** Register a SQL file; returns its template path relative to template_searchpath. */
  addSql(baseName: string, content: string): string {
    const name = `${String(this.sql.length + 1).padStart(2, "0")}_${baseName.replace(/[^a-z0-9_]/g, "_")}.sql`;
    this.sql.push({ name, content: content.endsWith("\n") ? content : content + "\n" });
    return name;
  }

  sqlHeader(purpose: string): string[] {
    return [
      `-- ${purpose}`,
      `-- Generated by ${GENERATOR_NAME} v${RUNTIME.generatorVersion} for DAG ${this.dagId.replace(/[\r\n]/g, "")}.`,
      ...this.sqlPrelude(),
    ];
  }

  // -- assembly --------------------------------------------------------------------

  assemble(opts: AssembleOptions): GeneratedBundle {
    const files: GeneratedFile[] = [];
    const dagPath = `dags/${this.dagId}.py`;
    files.push({ path: dagPath, content: this.renderDagFile(opts), language: "python", role: "dag" });
    for (const f of this.sql) {
      files.push({ path: `dags/sql/${this.dagId}/${f.name}`, content: f.content, language: "sql", role: "sql" });
    }
    for (const f of this.scripts) {
      files.push({ path: `scripts/${this.dagId}/${f.name}`, content: f.content, language: "python", role: "script" });
    }
    if (this.mode === "variables") {
      files.push({
        path: `config/${this.dagId}_variables.json`,
        content: JSON.stringify(this.variablesJson(), null, 2) + "\n",
        language: "json",
        role: "config",
      });
    }
    const needsZip = files.some((f) => f.role !== "dag");
    files.push({ path: "README.md", content: this.renderReadme(opts, files), language: "markdown", role: "readme" });
    return { dagId: this.dagId, files, needsZip, deploy: this.deployInfo(opts) };
  }

  private deployInfo(opts: AssembleOptions): DeployInfo {
    const byEnv = <T,>(f: (e: EnvCode) => T) => Object.fromEntries(ENVIRONMENTS.map((e) => [e, f(e)])) as Record<string, T>;
    const codeKey = this.codeBucketKey;
    return {
      deployEnvVar: DEPLOY_ENV_VAR,
      configMode: this.mode,
      variables:
        this.mode === "variables" ? byEnv((e) => ({ name: this.variableName(e), value: this.envValues(e) })) : null,
      codeBuckets:
        this.scripts.length && codeKey ? byEnv((e) => String(this.envValues(e)[codeKey] ?? "")) : null,
      iamRoles: opts.iamRoles,
      formEnv: this.env,
    };
  }

  variableName(env: EnvCode): string {
    return `${env}_${this.variableSuffix()}`;
  }

  private variableSuffix(): string {
    return this.dagId.replace(/[^A-Za-z0-9_]/g, "_");
  }

  private variablesJson(): Record<string, Record<string, string | string[]>> {
    const out: Record<string, Record<string, string | string[]>> = {};
    for (const e of ENVIRONMENTS) out[this.variableName(e)] = this.envValues(e);
    return out;
  }

  private renderImports(): string {
    const future = "from __future__ import annotations";
    const isStdlib = (m: string) => ["logging", "os", "datetime", "json", "re"].includes(m.split(".")[0]);
    const fromLine = (m: string, names: Set<string>) => {
      const sorted = [...names].sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()));
      const one = `from ${m} import ${sorted.join(", ")}`;
      if (textWidth(one) <= LINE_LENGTH) return one;
      return `from ${m} import (\n${sorted.map((n) => `    ${n},`).join("\n")}\n)`;
    };
    const section = (stdlib: boolean) => {
      const plain = [...this.plainImports].filter((m) => isStdlib(m) === stdlib).sort();
      const froms = [...this.fromImports.keys()].filter((m) => isStdlib(m) === stdlib).sort();
      return [...plain.map((m) => `import ${m}`), ...froms.map((m) => fromLine(m, this.fromImports.get(m)!))];
    };
    return [future, section(true).join("\n"), section(false).join("\n")].filter(Boolean).join("\n\n");
  }

  private renderDocstring(opts: AssembleOptions): string {
    const p = this.p;
    const emails = list(p, "email");
    const schedule = this.scheduleText();
    const envSource =
      this.mode === "variables"
        ? `Airflow Variable \`<env>_${this.variableSuffix()}\` (JSON), env from \`${DEPLOY_ENV_VAR}\``
        : `ENV_CONFIG dict in this file, env from \`${DEPLOY_ENV_VAR}\``;
    const lines = [
      `### ${this.dagId}`,
      "",
      str(p, "description") || this.scenario.title,
      "",
      `**Pipeline:** ${this.scenario.title}`,
      "",
      ...opts.docSteps.map((s, i) => `${i + 1}. ${s}`),
      "",
      "| | |",
      "|---|---|",
      `| Owner | ${str(p, "owner")} |`,
      `| Contact | ${emails.length ? emails.join(", ") : "n/a"} |`,
      `| Schedule | ${schedule} (${str(p, "timezone")}) |`,
      `| Env config | ${envSource} |`,
      `| Generated by | ${GENERATOR_NAME} v${RUNTIME.generatorVersion} (${this.scenario.id}) |`,
    ];
    return pyDocstring(lines.join("\n"));
  }

  private scheduleText(): string {
    const preset = str(this.p, "schedule_preset");
    if (preset === "none") return "None (manual)";
    if (preset === "custom") return `\`${str(this.p, "schedule_cron")}\``;
    return `\`${preset}\``;
  }

  private scheduleExpr(): Py {
    const preset = str(this.p, "schedule_preset");
    if (preset === "none") return pyNone;
    if (preset === "custom") return pyStr(str(this.p, "schedule_cron").replace(/\s+/g, " "));
    return pyStr(preset);
  }

  private startDateExpr(): Py {
    const [y, m, d] = str(this.p, "start_date").split("-").map(Number);
    return pyCall("pendulum.datetime", [pyInt(y), pyInt(m), pyInt(d)], [["tz", pyStr(str(this.p, "timezone"))]]);
  }

  private envConfigBlock(): string[] {
    const out: string[] = [];
    out.push(
      comment(`Deployment environment (${ENVIRONMENTS.join(" / ")}), set as an environment variable on the`),
      comment("Composer environment. Read once at parse time; no API or DB calls at parse time."),
      assign("ENV", pyCall("os.environ.get", [pyStr(DEPLOY_ENV_VAR), pyStr("dev")])),
      "if ENV not in ENVIRONMENTS:",
      `    raise ValueError(f"${DEPLOY_ENV_VAR} must be one of {ENVIRONMENTS}, got {ENV!r}")`,
      "",
    );
    if (this.mode === "config_dict") {
      const envDict = pyDict(
        ENVIRONMENTS.map((e) => [
          e,
          pyDict(
            Object.entries(this.envValues(e)).map(([k, val]) => [
              k,
              Array.isArray(val) ? pyList(val.map(pyStr)) : pyStr(val),
            ]),
            { explode: true },
          ),
        ]),
        { explode: true },
      );
      out.push(
        comment("Environment-specific values. Operators read them at run time via Jinja: {{ cfg.<key> }}"),
        assign("ENV_CONFIG", envDict),
        assign("CONFIG", pyIndex("ENV_CONFIG", "ENV")),
      );
    } else {
      out.push(
        comment(`Environment-specific values live in the JSON Airflow Variable "<env>_${this.variableSuffix()}"`),
        comment(`(import config/${this.dagId}_variables.json). Operators read them at run time via Jinja:`),
        comment("{{ var.json.get(cfg_var).<key> }} -- nothing is fetched at parse time."),
        assign("CONFIG_VARIABLE", pyFString([{ name: "ENV" }, { text: `_${this.variableSuffix()}` }])),
      );
    }
    return out;
  }

  private renderDagFile(opts: AssembleOptions): string {
    const p = this.p;
    const header = [
      "=".repeat(77),
      `Generated by ${GENERATOR_NAME} v${RUNTIME.generatorVersion}`,
      `Scenario : ${this.scenario.id} (${this.scenario.title})`,
      `Target   : ${RUNTIME.composerTargets}, Airflow ${RUNTIME.airflowVersion}, Python ${RUNTIME.pythonVersion},`,
      `           apache-airflow-providers-google ${RUNTIME.googleProviderVersion}`,
      "=".repeat(77),
    ];
    const emails = list(p, "email");
    const chain = list(p, "impersonation_chain");
    const owner = str(p, "owner");
    const hasSql = this.sql.length > 0;

    const labels = pyDict(
      [
        ["env", pyName("ENV")],
        ["team", pyStr(str(p, "team"))],
        ["owner", pyStr(toLabelValue(owner))],
        ["cost_center", pyStr(str(p, "cost_center"))],
        ["dag_id", pyStr(toLabelValue(this.dagId))],
      ],
      { explode: true },
    );

    const constants: string[] = [
      assign("DAG_ID", pyStr(this.dagId)),
      assign("ENVIRONMENTS", pyTuple(ENVIRONMENTS.map((e) => pyStr(e)))),
      "",
      ...this.envConfigBlock(),
      "",
      assign("GCP_CONN_ID", pyStr(str(p, "gcp_conn_id"))),
      assign("LOCATION", pyStr(str(p, "location"))),
      comment("Service account(s) to impersonate; None uses the connection's own identity."),
      assign(
        "IMPERSONATION_CHAIN",
        chain.length ? pyList(chain.map((_, i) => pyStr(this.ref("impersonation_chain", i)))) : pyNone,
      ),
      "",
      comment(`GCP labels for cost tracking on every job/resource (${TEAM.requiredLabels.join(", ")}).`),
      assign("LABELS", labels),
    ];
    if (hasSql) {
      constants.push(
        "",
        comment(`SQL files live next to this DAG in sql/<dag_id>/ (template_searchpath).`),
        assign(
          "SQL_DIR",
          pyCall("os.path.join", [
            pyCall("os.path.dirname", [pyCall("os.path.abspath", [pyName("__file__")])]),
            pyStr("sql"),
            pyName("DAG_ID"),
          ]),
        ),
      );
    }
    if (opts.constants?.length) constants.push("", ...opts.constants);

    const callback = [
      "def notify_failure(context):",
      '    """Log failed-task details. Extend here to alert Slack / Google Chat / PagerDuty."""',
      '    ti = context["task_instance"]',
      "    log.error(",
      '        "Task failed: dag_id=%s task_id=%s run_id=%s try=%s log_url=%s",',
      "        ti.dag_id,",
      "        ti.task_id,",
      '        context["run_id"],',
      "        ti.try_number,",
      "        ti.log_url,",
      "    )",
    ];

    const td = (minutes: number) => pyCall("timedelta", [], [["minutes", pyInt(minutes)]]);
    const defaultArgs = pyDict(
      [
        ["owner", pyStr(owner)],
        ["depends_on_past", pyBool(false)],
        ...(emails.length ? ([["email", pyList(emails.map(pyStr))]] as [string, Py][]) : []),
        ["email_on_failure", pyBool(bool(p, "email_on_failure") && emails.length > 0)],
        ["email_on_retry", pyBool(false)],
        ["retries", pyInt(num(p, "retries"))],
        ["retry_delay", td(num(p, "retry_delay_minutes"))],
        ["retry_exponential_backoff", pyBool(bool(p, "retry_exponential_backoff"))],
        ...(bool(p, "retry_exponential_backoff")
          ? ([["max_retry_delay", td(num(p, "max_retry_delay_minutes"))]] as [string, Py][])
          : []),
        ["execution_timeout", td(num(p, "execution_timeout_minutes"))],
        ["on_failure_callback", pyName("notify_failure")],
      ],
      { explode: true },
    );

    const macros =
      this.mode === "variables"
        ? pyDict([["cfg_var", pyName("CONFIG_VARIABLE")]])
        : pyDict([["cfg", pyName("CONFIG")]]);

    const dagCall = pyCall(
      "DAG",
      [],
      [
        ["dag_id", pyName("DAG_ID")],
        ["description", pyStr(str(p, "description") || this.scenario.title)],
        ["doc_md", pyName("__doc__")],
        ["schedule", this.scheduleExpr()],
        ["start_date", this.startDateExpr()],
        ["catchup", pyBool(bool(p, "catchup"))],
        ["max_active_runs", pyInt(num(p, "max_active_runs"))],
        ["dagrun_timeout", td(num(p, "dagrun_timeout_minutes"))],
        ["default_args", pyName("DEFAULT_ARGS")],
        ["tags", pyList(list(p, "tags").map(pyStr))],
        ["owner_links", emails.length ? pyDict([[owner, pyStr(`mailto:${emails[0]}`)]]) : null],
        ["template_searchpath", hasSql ? pyList([pyName("SQL_DIR")]) : null],
        ["user_defined_macros", macros],
      ],
      { explode: true },
    );

    const parts = [
      comment(header.join("\n")),
      this.renderDocstring(opts),
      "",
      this.renderImports(),
      "",
      assign("log", pyCall("logging.getLogger", [pyName("__name__")])),
      "",
      ...constants,
      "",
      "",
      ...callback,
      "",
      "",
      assign("DEFAULT_ARGS", defaultArgs),
      "",
      withHeader(dagCall, "dag"),
      ...opts.body,
    ];
    // Never more than two consecutive blank lines (ruff format / black rule).
    return parts.join("\n").replace(/\n{4,}/g, "\n\n\n") + "\n";
  }

  private renderReadme(opts: AssembleOptions, files: GeneratedFile[]): string {
    const dagFile = `${this.dagId}.py`;
    const varsFile = `${this.dagId}_variables.json`;
    const hasSql = files.some((f) => f.role === "sql");
    const hasScripts = files.some((f) => f.role === "script");
    const fileRows = files.map((f) => {
      const where =
        f.role === "dag" || f.role === "sql"
          ? "Composer bucket `dags/` folder"
          : f.role === "script"
            ? "Code bucket (referenced by gs:// URI)"
            : f.role === "config"
              ? "Import as Airflow Variables"
              : "This file";
      return `| \`${f.path}\` | ${where} |`;
    });
    const envRows = ENVIRONMENTS.map((e) => {
      const vals = this.envValues(e);
      return `| ${e} | ${Object.entries(vals)
        .map(([k, val]) => `\`${k}\`=\`${Array.isArray(val) ? val.join(", ") : val}\``)
        .join("<br>")} |`;
    });

    const lines = [
      `# ${this.dagId}`,
      "",
      `Generated by **${GENERATOR_NAME} v${RUNTIME.generatorVersion}** — scenario \`${this.scenario.id}\` (${this.scenario.title}).`,
      `Target: ${RUNTIME.composerTargets}, Airflow ${RUNTIME.airflowVersion}, Python ${RUNTIME.pythonVersion}, apache-airflow-providers-google ${RUNTIME.googleProviderVersion}.`,
      "",
      "## Files",
      "",
      "| Path | Upload to |",
      "|---|---|",
      ...fileRows,
      "",
      "## Deploy",
      "",
      "```bash",
      'COMPOSER_ENV="your-composer-env"',
      'COMPOSER_REGION="your-region"',
      'DAGS_PREFIX=$(gcloud composer environments describe "$COMPOSER_ENV" \\',
      '  --location "$COMPOSER_REGION" --format="value(config.dagGcsPrefix)")',
      "```",
      "",
      `### 1. Tell the environment which env it is (once per Composer environment)`,
      "",
      "```bash",
      'gcloud composer environments update "$COMPOSER_ENV" --location "$COMPOSER_REGION" \\',
      `  --update-env-variables=${DEPLOY_ENV_VAR}=${this.env}   # dev | stg | prd`,
      "```",
      "",
    ];
    let step = 2;
    if (this.mode === "variables") {
      lines.push(
        `### ${step++}. Import Airflow Variables`,
        "",
        `\`config/${varsFile}\` holds one JSON Variable per environment (\`${ENVIRONMENTS.map((e) => this.variableName(e)).join("`, `")}\`).`,
        "",
        "```bash",
        `gcloud storage cp config/${varsFile} "\${DAGS_PREFIX%/dags}/data/"`,
        'gcloud composer environments run "$COMPOSER_ENV" --location "$COMPOSER_REGION" \\',
        `  variables import -- /home/airflow/gcs/data/${varsFile}`,
        "```",
        "",
      );
    }
    lines.push(
      `### ${step++}. Upload the DAG${hasSql ? " and its SQL" : ""}`,
      "",
      "```bash",
      hasSql ? 'gcloud storage cp --recursive dags/* "$DAGS_PREFIX/"' : `gcloud storage cp dags/${dagFile} "$DAGS_PREFIX/"`,
      "```",
      "",
    );
    if (hasScripts) {
      lines.push(
        `### ${step}. Upload scripts to the code bucket`,
        "",
        "```bash",
        `gcloud storage cp --recursive scripts/${this.dagId} gs://${
          this.codeBucketKey ? String(this.envValues(this.env)[this.codeBucketKey] ?? "YOUR-CODE-BUCKET") : "YOUR-CODE-BUCKET"
        }/scripts/   # code bucket for ${this.env}; repeat per env`,
        "```",
        "",
      );
    }
    lines.push(
      "## Environment values",
      "",
      "Derived by swapping the env code; edit them if your environments differ.",
      "",
      "| Env | Values |",
      "|---|---|",
      ...envRows,
      "",
      "## Required IAM roles",
      "",
      "Grant these to the Composer environment service account (or the impersonated service account):",
      "",
      ...opts.iamRoles.map((r) => `- ${r}`),
      "",
    );
    if (opts.readmeNotes?.length) lines.push("## Notes", "", ...opts.readmeNotes.map((n) => `- ${n}`), "");
    return lines.join("\n");
  }
}
