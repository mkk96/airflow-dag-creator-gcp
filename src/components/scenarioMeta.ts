/** Visual metadata for scenarios (UI only): category, icon and accent colour. */
import {
  Boxes,
  Braces,
  Copy,
  Database,
  FileUp,
  FolderOutput,
  GitBranch,
  type LucideIcon,
  Radar,
  RadioTower,
  Rocket,
  Server,
  ShieldCheck,
  Waves,
  Workflow,
  Zap,
} from "lucide-react";

export type Category = "Ingestion" | "BigQuery" | "Processing" | "Pipelines" | "Triggers & messaging" | "Data quality";

export const CATEGORIES: Category[] = ["Ingestion", "BigQuery", "Processing", "Pipelines", "Triggers & messaging", "Data quality"];

export interface ScenarioMeta {
  category: Category;
  icon: LucideIcon;
  /** Tailwind gradient classes for the icon tile. */
  tile: string;
}

const TILES: Record<Category, string> = {
  Ingestion: "from-sky-500 to-blue-600",
  BigQuery: "from-indigo-500 to-violet-600",
  Processing: "from-amber-500 to-orange-600",
  Pipelines: "from-fuchsia-500 to-pink-600",
  "Triggers & messaging": "from-emerald-500 to-teal-600",
  "Data quality": "from-lime-500 to-green-600",
};

const META: Record<string, { category: Category; icon: LucideIcon }> = {
  gcs_to_bigquery: { category: "Ingestion", icon: FileUp },
  gcs_sensor_load_archive: { category: "Ingestion", icon: Radar },
  cloudsql_to_bigquery: { category: "Ingestion", icon: Database },
  bigquery_sql_transform: { category: "BigQuery", icon: Braces },
  bigquery_to_gcs: { category: "BigQuery", icon: FolderOutput },
  bigquery_table_copy: { category: "BigQuery", icon: Copy },
  dataflow_template: { category: "Processing", icon: Waves },
  dataproc_ephemeral: { category: "Processing", icon: Server },
  dataproc_serverless: { category: "Processing", icon: Zap },
  full_elt: { category: "Pipelines", icon: Workflow },
  dataform_workflow: { category: "Pipelines", icon: GitBranch },
  cloud_run_function: { category: "Triggers & messaging", icon: Rocket },
  pubsub: { category: "Triggers & messaging", icon: RadioTower },
  bigquery_dq_checks: { category: "Data quality", icon: ShieldCheck },
};

export function scenarioMeta(id: string): ScenarioMeta {
  const m = META[id] ?? { category: "Pipelines" as Category, icon: Boxes };
  return { ...m, tile: TILES[m.category] };
}
