/** CodeMirror language support, including a BigQuery SQL dialect. */
import { json } from "@codemirror/lang-json";
import { markdown } from "@codemirror/lang-markdown";
import { python } from "@codemirror/lang-python";
import { sql, SQLDialect } from "@codemirror/lang-sql";
import type { Extension } from "@codemirror/state";
import type { FileLanguage } from "../scenarios/types";

export const BigQuerySQL = SQLDialect.define({
  keywords:
    "select from where group by having order limit offset qualify window partition over rows range " +
    "between unbounded preceding following current row with recursive as join inner left right full " +
    "outer cross on using union intersect except distinct all and or not in is null like exists case " +
    "when then else end cast safe_cast struct array unnest insert into values update set delete merge " +
    "matched target source create replace table view materialized temp temporary if declare begin " +
    "commit rollback transaction call options cluster true false interval extract date datetime " +
    "timestamp time asc desc nulls first last respect ignore tablesample system_time of for",
  types:
    "string bytes int64 integer float64 float numeric bignumeric bool boolean date datetime time " +
    "timestamp geography json interval struct array range",
  builtin:
    "count countif sum avg min max array_agg string_agg any_value approx_count_distinct coalesce " +
    "ifnull nullif if current_date current_timestamp date_trunc timestamp_trunc date_add date_sub " +
    "date_diff format_date parse_date safe_divide lower upper trim regexp_contains regexp_extract " +
    "split concat length row_number rank dense_rank lag lead generate_uuid to_json_string",
  identifierQuotes: "`",
  backslashEscapes: true,
  doubleQuotedStrings: true,
  hashComments: true,
  spaceAfterDashes: false,
});

export function languageFor(lang: FileLanguage): Extension[] {
  switch (lang) {
    case "python":
      return [python()];
    case "sql":
      return [sql({ dialect: BigQuerySQL })];
    case "json":
      return [json()];
    case "markdown":
      return [markdown()];
  }
}
