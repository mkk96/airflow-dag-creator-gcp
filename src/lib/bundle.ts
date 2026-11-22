/** Browser downloads: single .py or a .zip mirroring the Composer bucket layout. */
import JSZip from "jszip";
import type { GeneratedBundle } from "../scenarios/types";

export function downloadBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

export function downloadText(text: string, filename: string, type = "text/plain"): void {
  downloadBlob(new Blob([text], { type: `${type};charset=utf-8` }), filename);
}

export function dagFile(bundle: GeneratedBundle) {
  return bundle.files.find((f) => f.role === "dag")!;
}

/**
 * <dag_id>.zip
 * ├── dags/<dag_id>.py
 * ├── dags/sql/<dag_id>/*.sql
 * ├── scripts/<dag_id>/*        (code bucket)
 * ├── config/<dag_id>_variables.json
 * └── README.md
 */
export async function buildZip(bundle: GeneratedBundle): Promise<Blob> {
  const zip = new JSZip();
  for (const f of bundle.files) {
    if (f.path.split("/").some((seg) => seg === ".." || seg === "")) {
      throw new Error(`Refusing unsafe path in bundle: ${f.path}`);
    }
    zip.file(f.path, f.content);
  }
  return zip.generateAsync({ type: "blob", compression: "DEFLATE" });
}
