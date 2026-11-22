/**
 * Naming rules — the ONE place to change naming behaviour.
 *
 *  - `OFFICIAL`  : hard GCP / Airflow rules. Violations are shown as ERRORS.
 *  - `TEAM`      : your team's convention. Violations are shown as WARNINGS.
 *                  Change patterns/tokens here; scenario code never hard-codes names.
 *
 * Team patterns use `{token}` placeholders. Each token has a regex (for
 * validation) and is filled from the naming context (for "Suggest names").
 */

// ---------------------------------------------------------------------------
// Environments & deployment
// ---------------------------------------------------------------------------

export const ENVIRONMENTS = ["dev", "stg", "prd"] as const;
export type EnvCode = (typeof ENVIRONMENTS)[number];

/** Composer environment variable that tells a DAG which env it runs in. */
export const DEPLOY_ENV_VAR = "DEPLOY_ENV";

// ---------------------------------------------------------------------------
// Official rules (errors)
// ---------------------------------------------------------------------------

export const OFFICIAL = {
  /** Airflow KEY_REGEX for dag_id / task_id, max 250 chars. */
  airflowKey: { re: /^[A-Za-z0-9_.-]+$/, maxLength: 250 },
  /** GCS bucket: https://cloud.google.com/storage/docs/buckets#naming */
  gcsBucket: {
    re: /^[a-z0-9][a-z0-9._-]*[a-z0-9]$/,
    minLength: 3,
    maxLength: 63,
    maxLengthWithDots: 222,
    maxComponentLength: 63,
    forbiddenPrefix: "goog",
    forbiddenContains: ["google", "g00gle"],
  },
  /** GCS object name: 1-1024 bytes UTF-8, no CR/LF. */
  gcsObject: { maxBytes: 1024 },
  /** Project ID: 6-30 chars, lowercase letters/digits/hyphens, starts with a letter, no trailing hyphen. */
  bqProject: { re: /^(?:[a-z][a-z0-9.-]*[a-z0-9]:)?[a-z][a-z0-9-]{4,28}[a-z0-9]$/ },
  /** BigQuery dataset: letters, digits, underscores, max 1024 chars. */
  bqDataset: { re: /^[A-Za-z0-9_]+$/, maxLength: 1024 },
  /** BigQuery table: Unicode letters, marks, numbers, connectors, dashes, spaces; max 1024 bytes. */
  bqTable: { re: /^[\p{L}\p{M}\p{N}\p{Pc}\p{Pd}\p{Zs}]+$/u, maxBytes: 1024 },
  /** BigQuery column name. */
  bqColumn: { re: /^[A-Za-z_][A-Za-z0-9_]*$/, maxLength: 300 },
  /** GCP labels: https://cloud.google.com/resource-manager/docs/labels-overview */
  labelKey: { re: /^[\p{Ll}\p{Lo}][\p{Ll}\p{Lo}\p{N}_-]{0,62}$/u },
  labelValue: { re: /^[\p{Ll}\p{Lo}\p{N}_-]{0,63}$/u },
  maxLabels: 64,
  /** Dataproc cluster / Dataflow job / Cloud Run job style resource names. */
  resourceName: { re: /^[a-z]([a-z0-9-]*[a-z0-9])?$/, maxLength: 63 },
  serviceAccount: { re: /^[a-z0-9-]+@[a-z0-9-]+\.iam\.gserviceaccount\.com$/ },
  email: { re: /^[^\s@]+@[^\s@]+\.[^\s@]+$/ },
} as const;

// ---------------------------------------------------------------------------
// Team convention (warnings) — edit freely
// ---------------------------------------------------------------------------

export const DATASET_LAYER_SETS = {
  classic: ["raw", "stg", "curated"],
  medallion: ["bronze", "silver", "gold"],
} as const;
export type LayerSet = keyof typeof DATASET_LAYER_SETS;

export const TEAM = {
  /** Which dataset layer vocabulary the team uses. */
  layerSet: "classic" as LayerSet,
  frequencies: ["hourly", "daily", "weekly", "monthly", "adhoc"],
  taskVerbs: [
    "load", "check", "extract", "export", "copy", "merge", "transform", "archive",
    "move", "wait", "sense", "create", "delete", "submit", "run", "trigger",
    "publish", "pull", "notify", "start", "end",
  ],
  patterns: {
    gcsBucket: "{org}-{env}-{domain}-{purpose}",
    bqDataset: "{layer}_{domain}",
    bqTable: "{noun}",
    dagId: "{domain}_{source}_to_{target}_{frequency}",
    taskId: "{verb}_{object}",
    /** Runtime-unique resources (Dataproc clusters, Dataflow / Cloud Run jobs). */
    resourceName: "{dag_id}-{run_date}",
  },
  tokens: {
    org: "[a-z0-9]+",
    env: "(?:dev|stg|prd)",
    domain: "[a-z0-9]+",
    purpose: "[a-z0-9-]+",
    layer: "(?:raw|stg|curated|bronze|silver|gold)",
    noun: "[a-z][a-z0-9_]*",
    source: "[a-z0-9]+",
    target: "[a-z0-9]+",
    frequency: "(?:hourly|daily|weekly|monthly|adhoc)",
    verb: "[a-z]+",
    object: "[a-z0-9_]+",
    dag_id: "[a-z0-9-]+",
    run_date: "[0-9]{8}",
  } as Record<string, string>,
  /** Short codes used in dag_ids, e.g. `gcs`, `bq`. */
  systemCodes: {
    gcs: "gcs",
    bigquery: "bq",
    dataflow: "df",
    dataproc: "dp",
    cloudsql: "sql",
    pubsub: "ps",
    cloudrun: "run",
    dataform: "dfm",
  } as Record<string, string>,
  /** Default bucket purpose per role. */
  bucketPurposes: { landing: "landing", archive: "archive", code: "code", temp: "temp" },
  /** Required label keys on every BigQuery job / Dataproc / Dataflow resource. */
  requiredLabels: ["env", "team", "owner", "cost_center", "dag_id"],
} as const;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

export type PatternName = keyof typeof TEAM.patterns;

/** Build an anchored regex for a team pattern (dataset layer tokens follow `TEAM.layerSet`). */
export function teamRegex(name: PatternName): RegExp {
  const tokens: Record<string, string> = {
    ...TEAM.tokens,
    layer: `(?:${DATASET_LAYER_SETS[TEAM.layerSet].join("|")})`,
    frequency: `(?:${TEAM.frequencies.join("|")})`,
    env: `(?:${ENVIRONMENTS.join("|")})`,
    verb: `(?:${TEAM.taskVerbs.join("|")})`,
  };
  const src = TEAM.patterns[name]
    .split(/(\{[a-z_]+\})/)
    .map((part) => {
      const m = /^\{([a-z_]+)\}$/.exec(part);
      if (m) return tokens[m[1]] ?? "[a-z0-9_-]+";
      return part.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    })
    .join("");
  return new RegExp(`^${src}$`);
}

/** Fill a team pattern from values; returns null if any token is missing. */
export function fillPattern(name: PatternName, values: Record<string, string | undefined>): string | null {
  let missing = false;
  const out = TEAM.patterns[name].replace(/\{([a-z_]+)\}/g, (_, token: string) => {
    const v = values[token];
    if (!v) missing = true;
    return v ?? "";
  });
  return missing ? null : out;
}

/** Normalise free text into a naming token (lowercase alphanumerics). */
export function toToken(text: string, sep: "" | "-" | "_" = ""): string {
  const joined = text.toLowerCase().replace(/[^a-z0-9]+/g, sep);
  return sep ? joined.split(sep).filter(Boolean).join(sep) : joined;
}

/** Make a value safe for a GCP label value (lowercase, [a-z0-9_-], max 63). */
export function toLabelValue(text: string): string {
  return text
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 63);
}

/**
 * Derive another environment's value by swapping the env token, e.g.
 * `acme-dev-sales-landing` -> `acme-prd-sales-landing`. Only whole tokens
 * delimited by start/end or one of `-_.@:` are replaced.
 */
export function swapEnv(value: string, from: string, to: string): string {
  if (from === to) return value;
  const re = new RegExp(`(^|[-_.@:])${from}(?=$|[-_.@:])`, "g");
  return value.replace(re, `$1${to}`);
}
