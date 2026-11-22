import type { Issues } from "../validation/rules";

export type ParamValue = string | number | boolean | string[];
export type Params = Record<string, ParamValue>;

export type FieldType =
  | "text"
  | "textarea"
  | "number"
  | "boolean"
  | "select"
  | "multiselect"
  | "list" // one value per line -> string[]
  | "date" // YYYY-MM-DD
  | "code"; // code editor (sql/json) with optional file upload

export interface FieldOption {
  value: string;
  label: string;
}

export interface FieldSchema {
  key: string;
  label: string;
  type: FieldType;
  /** Section id (see Scenario.sections / COMMON_SECTIONS). */
  section: string;
  default: ParamValue;
  help?: string | ((p: Params) => string);
  placeholder?: string;
  required?: boolean;
  options?: FieldOption[];
  /** Datalist suggestions for free-text fields. */
  suggestions?: string[];
  min?: number;
  max?: number;
  /** Code editor language for type "code". */
  language?: "sql" | "json";
  /** File extensions accepted by the upload button for type "code" (e.g. ".sql"). */
  accept?: string;
  showIf?: (p: Params) => boolean;
  /** Validate the whole value (string/number/bool fields). */
  validate?: (value: ParamValue, p: Params) => Issues;
  /** Validate each entry of a list / multiselect field. */
  validateItem?: (item: string, p: Params) => Issues;
  /**
   * Value differs per environment (project, buckets, datasets, service accounts).
   * It is emitted into the per-env config dict / Airflow Variable — never inline.
   */
  envSpecific?: boolean;
  /**
   * Jinja is allowed in this value. Default false: values that are inserted
   * into templated operator fields must not contain {{ }}, {% %} or {# #}.
   */
  allowJinja?: boolean;
  /** Field is filled by "Suggest names". */
  suggestable?: boolean;
}

export interface Section {
  id: string;
  title: string;
  description?: string;
}

export type FileLanguage = "python" | "sql" | "json" | "markdown";
export type FileRole = "dag" | "sql" | "script" | "config" | "readme";

export interface GeneratedFile {
  /** Path inside the bundle, mirroring the Composer bucket, e.g. dags/<dag_id>.py */
  path: string;
  content: string;
  language: FileLanguage;
  role: FileRole;
}

export interface GeneratedBundle {
  dagId: string;
  files: GeneratedFile[];
  /** True when the DAG needs files besides the .py (SQL, scripts, variables). */
  needsZip: boolean;
  /** Everything "Deploy to GCP" needs besides the files. */
  deploy: DeployInfo;
}

export interface DeployInfo {
  /** Environment variable the DAG reads to pick dev / stg / prd. */
  deployEnvVar: string;
  configMode: "config_dict" | "variables";
  /** env code -> the Airflow Variable to set (Variables mode only). */
  variables: Record<string, { name: string; value: Record<string, string | string[]> }> | null;
  /** env code -> code bucket that scripts/<dag_id>/ must be uploaded to (when there are scripts). */
  codeBuckets: Record<string, string> | null;
  /** IAM roles the Composer (or impersonated) service account needs at run time. */
  iamRoles: string[];
  /** The env code the form values were entered for. */
  formEnv: string;
}

export interface NamingContext {
  org: string;
  env: string;
  domain: string;
  source: string;
  target: string;
  frequency: string;
  layerSet: string;
}

export interface Scenario {
  /** snake_case id, also used in the generated header comment. */
  id: string;
  title: string;
  description: string;
  /** System codes for naming, e.g. { source: "gcs", target: "bq" }. */
  naming: { source: string; target: string };
  sections: Section[];
  /** Scenario-specific fields. Common fields are added automatically. */
  fields: FieldSchema[];
  /** Overrides for common field defaults (e.g. a sensible dag_id / tags). */
  commonDefaults?: Partial<Params>;
  /** Named sample parameter sets; used by `npm run generate` and the tests. */
  samples: Record<string, Partial<Params>>;
  /** Cross-field validation; returns issues keyed by field key. */
  validate?: (p: Params) => Record<string, Issues>;
  /** Fill scenario fields from the naming context ("Suggest names"). */
  suggest?: (ctx: NamingContext, p: Params) => Partial<Params>;
  generate: (p: Params) => GeneratedBundle;
}

export interface PlannedScenario {
  id: string;
  title: string;
  description: string;
}
