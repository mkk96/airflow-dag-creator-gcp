/**
 * Scenario — GCS sensor → BigQuery load → archive.
 *
 *   wait_for_<table>_files  GCSObjectExistenceSensor (use_glob=True): returns a
 *                           boolean only, so no file list goes through XCom
 *   load_<table>_to_bq      BigQueryInsertJobOperator `load` job, exactly like
 *                           gcs_to_bigquery (partition decorator, job labels)
 *   check_<table>_row_count optional BigQueryCheckOperator
 *   archive_<table>_files   GCSToGCSOperator(move_object=True) into a date-stamped
 *                           folder of the archive bucket
 *
 * Verified against apache-airflow-providers-google 12.0.0:
 *   - GCSObjectExistenceSensor.template_fields = bucket, object, impersonation_chain;
 *     `use_glob` and `deferrable` exist (GCSBlobTrigger honours use_glob).
 *   - GCSToGCSOperator.template_fields = source_bucket, source_object(s),
 *     destination_bucket, destination_object, delimiter, impersonation_chain.
 *     `match_glob` is NOT templated (static value only); a `*` in source_object(s)
 *     is deprecated, so the folder goes in source_objects and the pattern in match_glob.
 *     No template_ext and no deferrable mode.
 *
 * The landing-file, destination and archive helpers below are shared with
 * fullElt.ts and cloudsqlToBigquery.ts.
 */
import { DATASET_LAYER_SETS, fillPattern, TEAM } from "../config/naming";
import { frequencyFor } from "../config/schedules";
import {
  assign,
  comment,
  LINE_LENGTH,
  pyBool,
  pyCall,
  pyDict,
  pyFromJson,
  pyIdent,
  pyInt,
  pyList,
  pyName,
  pyStr,
  toSnakeIdentifier,
  withHeader,
  type Py,
} from "../lib/python";
import * as v from "../validation/rules";
import { BQ_INSERT_JOB_TEMPLATE_EXT, bool, DagContext, guardTemplateExt, list, num, str } from "./common";
import type { FieldSchema, Params, Scenario } from "./types";

// ---------------------------------------------------------------------------
// Shared helpers (also used by fullElt.ts and cloudsqlToBigquery.ts)
// ---------------------------------------------------------------------------

export const isCsv = (p: Params) => p.source_format === "CSV";
export const isCsvOrJson = (p: Params) => p.source_format === "CSV" || p.source_format === "NEWLINE_DELIMITED_JSON";
export const isPartitioned = (p: Params) => p.partition_type !== "NONE";
export const hasDateMacro = (s: string) => s.includes("{{");

/** Minimal set of BigQuery reserved words that must be quoted as column names. */
const BQ_RESERVED = new Set(
  (
    "ALL AND ANY ARRAY AS ASC ASSERT_ROWS_MODIFIED AT BETWEEN BY CASE CAST COLLATE CONTAINS CREATE " +
    "CROSS CUBE CURRENT DEFAULT DEFINE DESC DISTINCT ELSE END ENUM ESCAPE EXCEPT EXCLUDE EXISTS " +
    "EXTRACT FALSE FETCH FOLLOWING FOR FROM FULL GROUP GROUPING GROUPS HASH HAVING IF IGNORE IN " +
    "INNER INTERSECT INTERVAL INTO IS JOIN LATERAL LEFT LIKE LIMIT LOOKUP MERGE NATURAL NEW NO NOT " +
    "NULL NULLS OF ON OR ORDER OUTER OVER PARTITION PRECEDING PROTO QUALIFY RANGE RECURSIVE RESPECT " +
    "RIGHT ROLLUP ROWS SELECT SET SOME STRUCT TABLESAMPLE THEN TO TREAT TRUE UNBOUNDED UNION UNNEST " +
    "USING WHEN WHERE WINDOW WITH WITHIN"
  ).split(" "),
);
/** Column name for generated SQL (reserved words are back-quoted). */
export const sqlColumn = (c: string) => (BQ_RESERVED.has(c.toUpperCase()) ? `\`${c}\`` : c);

/** Parsed schema fields, or [] when the JSON is invalid (validation reports it). */
export function parseSchema(text: string): unknown[] {
  try {
    const parsed: unknown = JSON.parse(text);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

/** Lower-cased top-level column names of the explicit schema. */
export function schemaColumnNames(text: string): Set<string> {
  return new Set(
    parseSchema(text)
      .map((f) => (f && typeof f === "object" ? String((f as Record<string, unknown>).name ?? "") : ""))
      .filter(Boolean)
      .map((n) => n.toLowerCase()),
  );
}

/** `SCHEMA_FIELDS = [...]` module constant for an explicit schema. */
export function schemaConstants(schemaJson: string): string[] {
  return [
    comment("BigQuery schema for the loaded table (from the uploaded schema file)."),
    assign("SCHEMA_FIELDS", pyList(parseSchema(schemaJson).map((f) => pyFromJson(f)), { explode: true })),
  ];
}

const objectPath = (x: string) => x.replace(/^\/+/, "");

/** A folder inside a bucket: ends with "/", no wildcards. */
function folderPath(value: string): v.Issues {
  return v.merge(
    v.gcsObject(value),
    value.endsWith("/") ? v.ok() : v.err("End the folder path with '/'."),
    value.includes("*") ? v.err("No wildcards in the folder; put them in the file name pattern.") : v.ok(),
  );
}

function fileNamePattern(value: string): v.Issues {
  if (value.includes("/")) return v.err("File name only (no '/'); put sub-folders in the landing folder.");
  if (/[?[\]{}\\]/.test(value)) return v.err("Only the * wildcard is supported.");
  if ((value.match(/\*/g) ?? []).length > 1) return v.err("Use at most one * wildcard.");
  return v.ok();
}

export interface LandingDefaults {
  bucket: string;
  prefix: string;
  pattern: string;
}

/** Landing bucket/folder/pattern + sensor fields (section "source"). */
export function landingFields(d: LandingDefaults): FieldSchema[] {
  return [
    {
      key: "source_bucket", label: "Landing bucket", type: "text", section: "source", default: d.bucket,
      required: true, envSpecific: true, suggestable: true, validate: (x) => v.gcsBucket(String(x)),
      help: "Bucket where the files arrive. Name only (no gs://). Env-specific.",
    },
    {
      key: "source_prefix", label: "Landing folder", type: "text", section: "source", default: d.prefix,
      required: true, allowJinja: true, validate: (x) => folderPath(String(x)),
      help: "Folder inside the landing bucket, ending with '/'. Use an Airflow macro such as {{ ds }} or {{ ds_nodash }} so each run only picks up its own files.",
    },
    {
      key: "file_pattern", label: "File name pattern", type: "text", section: "source", default: d.pattern,
      required: true, validate: (x) => fileNamePattern(String(x)),
      help: "Which files in the landing folder to load and archive, e.g. *.csv or orders_*.json. One * wildcard at most; no macros here.",
    },
    {
      key: "source_format", label: "File format", type: "select", section: "source", default: "CSV",
      options: [
        { value: "CSV", label: "CSV" },
        { value: "NEWLINE_DELIMITED_JSON", label: "JSON (newline-delimited)" },
        { value: "PARQUET", label: "Parquet" },
        { value: "AVRO", label: "Avro" },
      ],
    },
    {
      key: "sensor_timeout_minutes", label: "Wait for files up to (minutes)", type: "number", section: "source",
      default: 120, min: 1, max: 10080,
      help: "The sensor fails (and the run stops) if no matching file shows up within this time.",
    },
    {
      key: "sensor_poke_interval_seconds", label: "Check for files every (seconds)", type: "number",
      section: "source", default: 300, min: 30, max: 3600,
    },
  ];
}

/** File format + schema fields (sections "format" and "schema"). */
export function formatFields(defaultSchema: string): FieldSchema[] {
  return [
    {
      key: "csv_skip_leading_rows", label: "Header rows to skip", type: "number", section: "format",
      default: 1, min: 0, max: 1000, showIf: isCsv,
    },
    {
      key: "csv_field_delimiter", label: "Field delimiter", type: "select", section: "format", default: ",",
      showIf: isCsv,
      options: [
        { value: ",", label: "Comma ( , )" },
        { value: "\t", label: "Tab" },
        { value: "|", label: "Pipe ( | )" },
        { value: ";", label: "Semicolon ( ; )" },
      ],
    },
    {
      key: "csv_quote", label: "Quote character", type: "text", section: "format", default: '"',
      showIf: isCsv, help: "Single character; leave empty if fields are never quoted.",
      validate: (x) => ([...String(x)].length > 1 ? v.err("Use a single character.") : v.ok()),
    },
    {
      key: "csv_encoding", label: "Encoding", type: "select", section: "format", default: "UTF-8", showIf: isCsv,
      options: ["UTF-8", "ISO-8859-1", "UTF-16BE", "UTF-16LE", "UTF-32BE", "UTF-32LE"].map((e) => ({ value: e, label: e })),
    },
    {
      key: "csv_null_marker", label: "Null marker (optional)", type: "text", section: "format", default: "",
      showIf: isCsv, help: 'Text that means NULL, e.g. "\\N". Empty = BigQuery default (empty field).',
    },
    {
      key: "csv_allow_quoted_newlines", label: "Allow quoted newlines", type: "boolean", section: "format",
      default: false, showIf: isCsv,
    },
    {
      key: "csv_allow_jagged_rows", label: "Allow jagged rows", type: "boolean", section: "format",
      default: false, showIf: isCsv, help: "Accept rows with missing trailing columns (filled with NULL).",
    },
    {
      key: "max_bad_records", label: "Max bad records", type: "number", section: "format", default: 0,
      min: 0, max: 1000000, showIf: isCsvOrJson, help: "Rows that may fail to parse before the load fails. 0 = strict.",
    },
    {
      key: "ignore_unknown_values", label: "Ignore unknown values", type: "boolean", section: "format",
      default: false, showIf: isCsvOrJson, help: "Skip extra columns / JSON keys that are not in the schema.",
    },
    {
      key: "parquet_enable_list_inference", label: "Enable list inference", type: "boolean", section: "format",
      default: true, showIf: (p) => p.source_format === "PARQUET",
    },
    {
      key: "avro_use_logical_types", label: "Use Avro logical types", type: "boolean", section: "format",
      default: true, showIf: (p) => p.source_format === "AVRO",
    },
    {
      key: "schema_mode", label: "Schema source", type: "select", section: "schema", default: "autodetect",
      showIf: isCsvOrJson,
      options: [
        { value: "autodetect", label: "Autodetect" },
        { value: "explicit", label: "Schema file (BigQuery JSON schema)" },
        { value: "existing", label: "Use the existing table's schema" },
      ],
      help: "Parquet and Avro are self-describing; their schema comes from the files.",
    },
    {
      key: "schema_json", label: "BigQuery JSON schema", type: "code", language: "json", accept: ".json",
      section: "schema", default: defaultSchema, required: true,
      showIf: (p) => isCsvOrJson(p) && p.schema_mode === "explicit",
      validate: (x) => v.bqSchemaJson(String(x)),
      help: "Paste or upload the output of `bq show --schema --format=prettyjson`. Embedded in the DAG as SCHEMA_FIELDS.",
    },
  ];
}

export interface ArchiveDefaults {
  bucket: string;
  prefix: string;
}

/** Archive bucket/folder/action (section "archive"). */
export function archiveFields(d: ArchiveDefaults): FieldSchema[] {
  return [
    {
      key: "archive_bucket", label: "Archive bucket", type: "text", section: "archive", default: d.bucket,
      required: true, envSpecific: true, suggestable: true, validate: (x) => v.gcsBucket(String(x)),
      help: "Bucket that keeps processed files. Name only (no gs://). Env-specific. May be the landing bucket if the archive folder is outside the landing folder.",
    },
    {
      key: "archive_prefix", label: "Archive folder", type: "text", section: "archive", default: d.prefix,
      required: true, allowJinja: true, validate: (x) => folderPath(String(x)),
      help: "Folder in the archive bucket, ending with '/'. Files keep their names (and sub-folders). Include {{ ds }} so each run's files are kept apart.",
    },
    {
      key: "archive_action", label: "Archive action", type: "select", section: "archive", default: "move",
      options: [
        { value: "move", label: "Move — copy to the archive, then delete from the landing folder" },
        { value: "copy", label: "Copy — keep the files in the landing folder too" },
      ],
    },
  ];
}

/** Cross-field checks for landing + archive fields. */
export function validateLanding(p: Params, add: (k: string, i: v.Issues) => void): void {
  const prefix = str(p, "source_prefix");
  const archivePrefix = str(p, "archive_prefix");
  if (prefix && !hasDateMacro(prefix)) {
    add(
      "source_prefix",
      v.warn(
        str(p, "archive_action") === "copy"
          ? "No date macro and files are only copied: every run loads the same files again."
          : "No date macro: each run loads whatever is in the folder. Re-running an old date loads today's files into that date.",
      ),
    );
  }
  if (archivePrefix && !hasDateMacro(archivePrefix)) {
    add("archive_prefix", v.warn("No date macro: archived files with the same name overwrite each other."));
  }
  if (str(p, "archive_bucket") === str(p, "source_bucket") && prefix && archivePrefix.startsWith(prefix)) {
    add("archive_prefix", v.err("The archive folder is inside the landing folder, so archived files would be loaded again."));
  }
  if (num(p, "sensor_timeout_minutes") > num(p, "dagrun_timeout_minutes")) {
    add("sensor_timeout_minutes", v.warn("Longer than the DAG run timeout; the run times out first."));
  }
}

/** Partition decorator for a time partitioning type (idempotent WRITE_TRUNCATE target). */
export function partitionDecorator(partition: string): string {
  switch (partition) {
    case "HOUR":
      return "${{ logical_date.strftime('%Y%m%d%H') }}";
    case "DAY":
      return "${{ ds_nodash }}";
    case "MONTH":
      return "${{ logical_date.strftime('%Y%m') }}";
    default:
      return "";
  }
}

export interface DestinationDefaults {
  dataset: string;
  table: string;
}

/** Destination table fields (section "destination"). */
export function destinationFields(d: DestinationDefaults): FieldSchema[] {
  return [
    {
      key: "dataset", label: "Dataset", type: "text", section: "destination", default: d.dataset,
      required: true, envSpecific: true, suggestable: true, validate: (x) => v.bqDataset(String(x)),
      help: "Env-specific. Convention: {layer}_{domain}.",
    },
    {
      key: "table", label: "Table", type: "text", section: "destination", default: d.table, required: true,
      validate: (x) => v.bqTable(String(x)), help: "snake_case noun, e.g. orders.",
    },
    {
      key: "partition_type", label: "Partitioning", type: "select", section: "destination", default: "DAY",
      options: [
        { value: "HOUR", label: "Hourly — load into table$YYYYMMDDHH (idempotent)" },
        { value: "DAY", label: "Daily — load into table$YYYYMMDD (idempotent)" },
        { value: "MONTH", label: "Monthly — load into table$YYYYMM (idempotent)" },
        { value: "NONE", label: "Not partitioned" },
      ],
      help: "Match the schedule: each run replaces its own partition.",
    },
    {
      key: "partition_field", label: "Partition column (optional)", type: "text", section: "destination",
      default: "", showIf: isPartitioned, validate: (x) => v.bqColumn(String(x)),
      help: "Empty = ingestion-time partitioning. If set (DATE/TIMESTAMP column), every row loaded by a run must fall in that run's partition.",
    },
    {
      key: "cluster_fields", label: "Clustering columns", type: "list", section: "destination", default: [],
      validateItem: (x) => v.bqColumn(x), help: "Up to 4, one per line.",
    },
    {
      key: "write_disposition", label: "Write disposition", type: "select", section: "destination",
      default: "WRITE_TRUNCATE",
      options: [
        { value: "WRITE_TRUNCATE", label: "WRITE_TRUNCATE — replace (partition or table)" },
        { value: "WRITE_APPEND", label: "WRITE_APPEND — append (not idempotent)" },
        { value: "WRITE_EMPTY", label: "WRITE_EMPTY — fail if data exists" },
      ],
    },
    {
      key: "create_disposition", label: "Create disposition", type: "select", section: "destination",
      default: "CREATE_IF_NEEDED",
      options: [
        { value: "CREATE_IF_NEEDED", label: "CREATE_IF_NEEDED — create the table on first load" },
        { value: "CREATE_NEVER", label: "CREATE_NEVER — the table must exist" },
      ],
    },
    {
      key: "schema_update_options", label: "Schema update options", type: "multiselect", section: "destination",
      default: [],
      options: [
        { value: "ALLOW_FIELD_ADDITION", label: "Allow field addition" },
        { value: "ALLOW_FIELD_RELAXATION", label: "Allow field relaxation (REQUIRED → NULLABLE)" },
      ],
      showIf: (p) =>
        p.write_disposition === "WRITE_APPEND" || (p.write_disposition === "WRITE_TRUNCATE" && isPartitioned(p)),
    },
  ];
}

const GRANULARITY: Record<string, number> = { hourly: 1, daily: 2, weekly: 3, monthly: 4, HOUR: 1, DAY: 2, MONTH: 4 };

/** Cross-field checks for the destination fields. */
export function validateDestination(p: Params, add: (k: string, i: v.Issues) => void): void {
  const wd = str(p, "write_disposition");
  const partition = str(p, "partition_type");
  if (wd === "WRITE_APPEND") {
    add("write_disposition", v.warn("Not idempotent: re-running a date appends duplicate rows. Prefer WRITE_TRUNCATE into a partition."));
  } else if (wd === "WRITE_EMPTY") {
    add("write_disposition", v.warn("Re-runs fail once the partition/table already has data."));
  } else if (!isPartitioned(p)) {
    add("write_disposition", v.warn("Not partitioned: every run replaces the whole table."));
  }
  const freq = frequencyFor(str(p, "schedule_preset"), str(p, "schedule_cron"));
  if (wd === "WRITE_TRUNCATE" && isPartitioned(p) && GRANULARITY[freq] && GRANULARITY[freq] < GRANULARITY[partition]) {
    add("partition_type", v.warn(`The DAG runs ${freq} but each run replaces a whole ${partition.toLowerCase()} partition, wiping earlier runs of that period. Use a finer partitioning.`));
  }
  if (list(p, "cluster_fields").length > 4) add("cluster_fields", v.err("BigQuery allows at most 4 clustering columns."));
}

/** destinationTable + dispositions + partitioning/clustering entries of a load configuration. */
export function destinationLoadOptions(ctx: DagContext, p: Params, datasetKey = "dataset"): Array<[string, Py]> {
  const partition = str(p, "partition_type");
  const partitionField = str(p, "partition_field");
  const out: Array<[string, Py]> = [
    [
      "destinationTable",
      pyDict(
        [
          ["projectId", pyStr(ctx.ref("project_id"))],
          ["datasetId", pyStr(ctx.ref(datasetKey))],
          ["tableId", pyStr(str(p, "table") + partitionDecorator(partition))],
        ],
        { explode: true },
      ),
    ],
    ["writeDisposition", pyStr(str(p, "write_disposition"))],
    ["createDisposition", pyStr(str(p, "create_disposition"))],
  ];
  if (partition !== "NONE") {
    const tp: Array<[string, Py]> = [["type", pyStr(partition)]];
    if (partitionField) tp.push(["field", pyStr(partitionField)]);
    out.push(["timePartitioning", pyDict(tp)]);
  }
  const clusters = list(p, "cluster_fields");
  if (clusters.length) out.push(["clustering", pyDict([["fields", pyList(clusters.map(pyStr))]])]);
  const wd = str(p, "write_disposition");
  const suo = list(p, "schema_update_options");
  if ((wd === "WRITE_APPEND" || (wd === "WRITE_TRUNCATE" && partition !== "NONE")) && suo.length) {
    out.push(["schemaUpdateOptions", pyList(suo.map(pyStr))]);
  }
  return out;
}

/** Effective schema mode: autodetect | explicit | existing | self_describing. */
export const schemaModeOf = (p: Params) => (isCsvOrJson(p) ? str(p, "schema_mode") : "self_describing");

/** sourceFormat, schema and per-format options of a load configuration. */
export function formatLoadOptions(p: Params): Array<[string, Py]> {
  const format = str(p, "source_format");
  const schemaMode = schemaModeOf(p);
  const out: Array<[string, Py]> = [["sourceFormat", pyStr(format)]];
  if (schemaMode === "autodetect") out.push(["autodetect", pyBool(true)]);
  if (schemaMode === "explicit") out.push(["schema", pyDict([["fields", pyName("SCHEMA_FIELDS")]])]);
  if (format === "CSV") {
    out.push(
      ["skipLeadingRows", pyInt(num(p, "csv_skip_leading_rows"))],
      ["fieldDelimiter", pyStr(String(p.csv_field_delimiter ?? ","))],
      ["quote", pyStr(String(p.csv_quote ?? ""))],
      ["encoding", pyStr(str(p, "csv_encoding"))],
      ["allowQuotedNewlines", pyBool(bool(p, "csv_allow_quoted_newlines"))],
      ["allowJaggedRows", pyBool(bool(p, "csv_allow_jagged_rows"))],
    );
    if (String(p.csv_null_marker ?? "") !== "") out.push(["nullMarker", pyStr(String(p.csv_null_marker))]);
  }
  if (isCsvOrJson(p)) {
    out.push(
      ["maxBadRecords", pyInt(num(p, "max_bad_records"))],
      ["ignoreUnknownValues", pyBool(bool(p, "ignore_unknown_values"))],
    );
  }
  if (format === "PARQUET") {
    out.push(["parquetOptions", pyDict([["enableListInference", pyBool(bool(p, "parquet_enable_list_inference"))]])]);
  }
  if (format === "AVRO") out.push(["useAvroLogicalTypes", pyBool(bool(p, "avro_use_logical_types"))]);
  return out;
}

/** gs:// URI (Jinja) of the landing files, guarded against BigQueryInsertJobOperator.template_ext. */
export function landingSourceUri(ctx: DagContext, p: Params): string {
  return guardTemplateExt(
    `gs://${ctx.ref("source_bucket")}/${objectPath(str(p, "source_prefix"))}${str(p, "file_pattern")}`,
    BQ_INSERT_JOB_TEMPLATE_EXT,
  );
}

/** BigQueryInsertJobOperator running a load job, with job labels. */
export function bqLoadTask(ctx: DagContext, taskId: string, load: Array<[string, Py]>): Py {
  ctx.importFrom("airflow.providers.google.cloud.operators.bigquery", "BigQueryInsertJobOperator");
  return pyCall(
    "BigQueryInsertJobOperator",
    [],
    [
      ["task_id", pyStr(taskId)],
      ["project_id", pyStr(ctx.ref("project_id"))],
      ["location", pyName("LOCATION")],
      [
        "configuration",
        pyDict(
          [
            ["load", pyDict(load, { explode: true })],
            ["labels", pyName("LABELS")],
          ],
          { explode: true },
        ),
      ],
      ["gcp_conn_id", pyName("GCP_CONN_ID")],
      ["impersonation_chain", ctx.impersonationChain()],
      ["deferrable", ctx.deferrable()],
    ],
    { explode: true },
  );
}

/** Sensor that waits until at least one landing file matches (returns a bool, no file list in XCom). */
export function sensorTask(ctx: DagContext, p: Params, taskId: string): Py {
  ctx.importFrom("airflow.providers.google.cloud.sensors.gcs", "GCSObjectExistenceSensor");
  return pyCall(
    "GCSObjectExistenceSensor",
    [],
    [
      ["task_id", pyStr(taskId)],
      ["bucket", pyStr(ctx.ref("source_bucket"))],
      ["object", pyStr(`${objectPath(str(p, "source_prefix"))}${str(p, "file_pattern")}`)],
      ["use_glob", pyBool(true)],
      ["google_cloud_conn_id", pyName("GCP_CONN_ID")],
      ["impersonation_chain", ctx.impersonationChain()],
      ...ctx.sensorKwargs(num(p, "sensor_timeout_minutes"), num(p, "sensor_poke_interval_seconds")),
      // execution_timeout also caps a deferred wait: keep it just above the sensor timeout
      // so the sensor's own timeout (which fails without retrying) fires first.
      ["execution_timeout", pyCall("timedelta", [], [["minutes", pyInt(num(p, "sensor_timeout_minutes") + 10)]])],
    ],
    { explode: true },
  );
}

/** GCSToGCSOperator that moves (or copies) the processed landing files to the archive folder. */
export function archiveTask(ctx: DagContext, p: Params, taskId: string): Py {
  ctx.importFrom("airflow.providers.google.cloud.transfers.gcs_to_gcs", "GCSToGCSOperator");
  return pyCall(
    "GCSToGCSOperator",
    [],
    [
      ["task_id", pyStr(taskId)],
      ["source_bucket", pyStr(ctx.ref("source_bucket"))],
      ["source_objects", pyList([pyStr(objectPath(str(p, "source_prefix")))])],
      // match_glob is not templated: a static pattern, applied within the folder above.
      ["match_glob", pyStr(`**/${str(p, "file_pattern")}`)],
      ["destination_bucket", pyStr(ctx.ref("archive_bucket"))],
      ["destination_object", pyStr(objectPath(str(p, "archive_prefix")))],
      ["move_object", pyBool(str(p, "archive_action") !== "copy")],
      ["gcp_conn_id", pyName("GCP_CONN_ID")],
      ["impersonation_chain", ctx.impersonationChain()],
    ],
    { explode: true },
  );
}

/** Row-count BigQueryCheckOperator for the run's partition of `<datasetKey>.<table>`. */
export function rowCountCheckTask(ctx: DagContext, p: Params, taskId: string): Py {
  ctx.importFrom("airflow.providers.google.cloud.operators.bigquery", "BigQueryCheckOperator");
  const partition = str(p, "partition_type");
  const partitionField = str(p, "partition_field");
  const col = partitionField ? sqlColumn(partitionField) : "_PARTITIONTIME";
  const where: Record<string, string> = {
    HOUR: `WHERE TIMESTAMP_TRUNC(${col}, HOUR) = TIMESTAMP('{{ ts }}')`,
    DAY: `WHERE DATE(${col}) = DATE('{{ ds }}')`,
    MONTH: `WHERE DATE_TRUNC(DATE(${col}), MONTH) = DATE_TRUNC(DATE('{{ ds }}'), MONTH)`,
  };
  const sql = [
    ...ctx.sqlHeader(
      partition === "NONE"
        ? "Data quality: fail when the table is empty after the load."
        : "Data quality: fail when the partition loaded by this run is empty.",
    ),
    "SELECT COUNT(*) > 0 AS has_rows",
    `FROM \`${ctx.sqlRef("project_id")}.${ctx.sqlRef("dataset")}.{{ params.table }}\``,
    ...(where[partition] ? [where[partition]] : []),
  ].join("\n");
  return pyCall(
    "BigQueryCheckOperator",
    [],
    [
      ["task_id", pyStr(taskId)],
      ["sql", pyStr(ctx.addSql(taskId, sql))],
      ["params", pyDict([["table", pyStr(str(p, "table"))]])],
      ["use_legacy_sql", pyBool(false)],
      ["location", pyName("LOCATION")],
      ["labels", pyName("LABELS")],
      ["gcp_conn_id", pyName("GCP_CONN_ID")],
      ["impersonation_chain", ctx.impersonationChain()],
      // Not deferrable: the deferrable path of BigQueryCheckOperator drops labels (providers-google 12.0.0).
      ["deferrable", pyBool(false)],
    ],
    { explode: true },
  );
}

export const FORMAT_LABEL: Record<string, string> = {
  CSV: "CSV",
  NEWLINE_DELIMITED_JSON: "newline-delimited JSON",
  PARQUET: "Parquet",
  AVRO: "Avro",
};

/** `with TaskGroup(...) as <name>:` block with its tasks and a linear chain. */
export function taskGroupBlock(
  ctx: DagContext,
  groupId: string,
  tooltip: string,
  tasks: Array<[string, Py]>,
  chain: string[] = tasks.map(([id]) => id),
): string[] {
  ctx.importFrom("airflow.utils.task_group", "TaskGroup");
  const out = [
    withHeader(
      pyCall("TaskGroup", [], [
        ["group_id", pyStr(groupId)],
        ["tooltip", pyStr(tooltip)],
      ]),
      groupId,
      1,
    ),
  ];
  tasks.forEach(([id, task], i) => {
    if (i > 0) out.push("");
    out.push(assign(id, task, 2));
  });
  if (chain.length > 1) out.push("", dependencyChain(chain, 2));
  return out;
}

/** `a >> b >> c` statement, wrapped in parentheses one per line (as ruff formats it) when too long. */
export function dependencyChain(ids: Array<string | string[]>, level: number): string {
  const indent = "    ".repeat(level);
  const term = (x: string | string[]) => (Array.isArray(x) ? `[${x.map(pyIdent).join(", ")}]` : pyIdent(x));
  const one = `${indent}${ids.map(term).join(" >> ")}`;
  if (one.length <= LINE_LENGTH) return one;
  const inner = `${indent}    `;
  return [`${indent}(`, `${inner}${term(ids[0])}`, ...ids.slice(1).map((x) => `${inner}>> ${term(x)}`), `${indent})`].join("\n");
}

// ---------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------

const DEFAULT_SCHEMA = JSON.stringify(
  [
    { name: "sku", type: "STRING", mode: "REQUIRED" },
    { name: "warehouse_id", type: "STRING", mode: "REQUIRED" },
    { name: "quantity_on_hand", type: "INT64", mode: "NULLABLE" },
    { name: "snapshot_ts", type: "TIMESTAMP", mode: "NULLABLE" },
  ],
  null,
  2,
);

export const gcsSensorLoadArchive: Scenario = {
  id: "gcs_sensor_load_archive",
  title: "GCS sensor → load → archive",
  description:
    "Wait for files to land in a GCS folder, load them into a BigQuery partition, then move the processed files to a date-stamped archive folder.",
  naming: { source: TEAM.systemCodes.gcs, target: TEAM.systemCodes.bigquery },
  sections: [
    { id: "source", title: "Landing files (GCS)", description: "Where files arrive and how long to wait for them." },
    { id: "format", title: "File format options" },
    { id: "schema", title: "Schema" },
    { id: "destination", title: "Destination (BigQuery)" },
    { id: "quality", title: "Data quality" },
    { id: "archive", title: "Archive", description: "Where processed files go after a successful load." },
  ],
  commonDefaults: {
    dag_id: "inventory_gcs_to_bq_daily",
    description: "Wait for daily stock level files, load them into BigQuery and archive them.",
    domain: "inventory",
    tags: ["inventory", "gcs", "bigquery"],
  },
  fields: [
    ...landingFields({
      bucket: "acme-dev-inventory-landing",
      prefix: "incoming/stock_levels/dt={{ ds }}/",
      pattern: "*.csv",
    }),
    ...formatFields(DEFAULT_SCHEMA),
    ...destinationFields({ dataset: "raw_inventory", table: "stock_levels" }),
    {
      key: "dq_check", label: "Post-load check", type: "select", section: "quality", default: "row_count",
      options: [
        { value: "row_count", label: "Row count > 0 for the loaded partition" },
        { value: "none", label: "None" },
      ],
      help: "Files are archived only after the check passes, so a failed run can simply be re-run.",
    },
    ...archiveFields({ bucket: "acme-dev-inventory-archive", prefix: "stock_levels/dt={{ ds }}/" }),
  ],

  samples: {
    csv_daily_move: {
      dag_id: "inventory_gcs_to_bq_daily",
      email: ["inventory-alerts@example.com"],
      cluster_fields: ["warehouse_id"],
    },
    json_hourly_explicit_variables: {
      dag_id: "shipping_gcs_to_bq_hourly",
      description: "Hourly shipment events: wait, load into an hourly partition, archive.",
      domain: "shipping",
      config_mode: "variables",
      env: "stg",
      project_id: "acme-stg-data",
      impersonation_chain: ["etl-runner@acme-stg-data.iam.gserviceaccount.com"],
      deferrable: false,
      source_bucket: "acme-stg-shipping-landing",
      source_prefix: "events/shipments/{{ ds_nodash }}/{{ logical_date.strftime('%H') }}/",
      file_pattern: "shipments_*.json",
      source_format: "NEWLINE_DELIMITED_JSON",
      schema_mode: "explicit",
      schema_json: JSON.stringify(
        [
          { name: "shipment_id", type: "STRING", mode: "REQUIRED" },
          { name: "carrier", type: "STRING", mode: "NULLABLE" },
          { name: "status", type: "STRING", mode: "NULLABLE" },
          { name: "event_ts", type: "TIMESTAMP", mode: "REQUIRED" },
        ],
        null,
        2,
      ),
      ignore_unknown_values: true,
      dataset: "raw_shipping",
      table: "shipment_events",
      partition_type: "HOUR",
      partition_field: "event_ts",
      schedule_preset: "@hourly",
      dagrun_timeout_minutes: 55,
      execution_timeout_minutes: 30,
      sensor_timeout_minutes: 45,
      sensor_poke_interval_seconds: 120,
      dq_check: "none",
      archive_bucket: "acme-stg-shipping-archive",
      archive_prefix: "shipments/{{ ds_nodash }}/{{ logical_date.strftime('%H') }}/",
      tags: ["shipping", "gcs", "bigquery"],
    },
    parquet_monthly_copy: {
      dag_id: "crm_gcs_to_bq_monthly",
      description: "Monthly CRM account snapshot (Parquet), archived as a copy.",
      domain: "crm",
      source_bucket: "acme-dev-crm-landing",
      source_prefix: "exports/accounts/{{ logical_date.strftime('%Y-%m') }}/",
      file_pattern: "*.parquet",
      source_format: "PARQUET",
      dataset: "raw_crm",
      table: "accounts",
      partition_type: "MONTH",
      schedule_preset: "@monthly",
      dq_check: "none",
      archive_bucket: "acme-dev-crm-archive",
      archive_prefix: "accounts/month={{ logical_date.strftime('%Y-%m') }}/",
      archive_action: "copy",
      tags: ["crm", "gcs", "bigquery"],
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => {
      out[k] = out[k] ? v.merge(out[k], i) : i;
    };
    validateLanding(p, add);
    validateDestination(p, add);
    if (isCsvOrJson(p) && p.schema_mode === "existing" && p.create_disposition === "CREATE_IF_NEEDED") {
      add("schema_mode", v.warn("The table must already exist when using its existing schema (consider CREATE_NEVER)."));
    }
    return out;
  },

  suggest(ctx) {
    const layer = DATASET_LAYER_SETS[ctx.layerSet as keyof typeof DATASET_LAYER_SETS]?.[0] ?? "raw";
    const out: Partial<Params> = {};
    const landing = fillPattern("gcsBucket", { ...ctx, purpose: TEAM.bucketPurposes.landing });
    const archive = fillPattern("gcsBucket", { ...ctx, purpose: TEAM.bucketPurposes.archive });
    const dataset = fillPattern("bqDataset", { layer, domain: ctx.domain });
    if (landing) out.source_bucket = landing;
    if (archive) out.archive_bucket = archive;
    if (dataset) out.dataset = dataset;
    return out;
  },

  generate(p) {
    const ctx = new DagContext(gcsSensorLoadArchive, p);
    const table = str(p, "table");
    const obj = toSnakeIdentifier(table, "table");
    const partition = str(p, "partition_type");
    const move = str(p, "archive_action") !== "copy";

    const sensorId = `wait_for_${obj}_files`;
    const loadId = `load_${obj}_to_bq`;
    const archiveId = `archive_${obj}_files`;
    const load: Array<[string, Py]> = [
      ["sourceUris", pyList([pyStr(landingSourceUri(ctx, p))])],
      ...destinationLoadOptions(ctx, p),
      ...formatLoadOptions(p),
    ];
    const ingest: Array<[string, Py]> = [
      [sensorId, sensorTask(ctx, p, sensorId)],
      [loadId, bqLoadTask(ctx, loadId, load)],
    ];
    const checkId = str(p, "dq_check") === "row_count" ? `check_${obj}_row_count` : null;
    if (checkId) ingest.push([checkId, rowCountCheckTask(ctx, p, checkId)]);

    const body = [
      ...taskGroupBlock(ctx, "ingest", "Wait for the landing files and load them into BigQuery", ingest),
      "",
      assign(archiveId, archiveTask(ctx, p, archiveId), 1),
      "",
      `    ingest >> ${pyIdent(archiveId)}`,
    ];

    const constants = schemaModeOf(p) === "explicit" ? schemaConstants(String(p.schema_json ?? "[]")) : [];

    const files = `\`${str(p, "source_prefix")}${str(p, "file_pattern")}\``;
    const target = `\`<project_id>.<dataset>.${table}\``;
    const docSteps = [
      `\`${sensorId}\`: wait up to ${num(p, "sensor_timeout_minutes")} min for a file matching ${files} in the \`source_bucket\`.`,
      `\`${loadId}\`: load the ${FORMAT_LABEL[str(p, "source_format")] ?? ""} files into ${target}` +
        (partition === "NONE" ? "" : ` partition \`${partitionDecorator(partition)}\``) +
        ` with ${str(p, "write_disposition")}.`,
      ...(checkId ? [`\`${checkId}\`: fail if the loaded ${partition === "NONE" ? "table" : "partition"} has no rows.`] : []),
      `\`${archiveId}\`: ${move ? "move" : "copy"} the processed files to \`${str(p, "archive_prefix")}\` in the \`archive_bucket\`.`,
    ];

    const iamRoles = [
      "`roles/bigquery.jobUser` on the project that runs the jobs",
      "`roles/bigquery.dataEditor` on the destination dataset",
      move
        ? "`roles/storage.objectUser` on the landing bucket (read, list and delete the processed files)"
        : "`roles/storage.objectViewer` on the landing bucket",
      "`roles/storage.objectUser` on the archive bucket (create / overwrite archived files)",
    ];
    if (list(p, "impersonation_chain").length) {
      iamRoles.push("`roles/iam.serviceAccountTokenCreator` for the Composer service account on the impersonated service account");
    }

    return ctx.assemble({
      docSteps,
      constants,
      body,
      iamRoles,
      readmeNotes: [
        "The load job is idempotent when it writes with WRITE_TRUNCATE into the run's partition decorator; re-running a date replaces that partition only.",
        "Files are archived only after the load (and check) succeed. Once archived, re-running the date finds no files: move them back from the archive folder to reprocess.",
        "The sensor matches files directly inside the landing folder; the load and archive steps also include files in its sub-folders.",
        "Labels (env, team, owner, cost_center, dag_id) are set on every BigQuery job for cost tracking.",
        ...(bool(p, "deferrable")
          ? ["The sensor and BigQuery jobs run in deferrable mode: make sure the Composer environment has a triggerer (Composer 2: set triggerer count >= 1)."]
          : ["The sensor runs in reschedule mode, so it frees its worker slot between checks."]),
      ],
    });
  },
};
