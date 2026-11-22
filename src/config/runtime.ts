/**
 * Target runtime. The single source of truth is /runtime.config.json, which is
 * also read by CI (to pick the Airflow constraints file) — change versions there.
 */
import runtimeJson from "../../runtime.config.json";

export interface RuntimeConfig {
  generatorVersion: string;
  airflowVersion: string;
  pythonVersion: string;
  googleProviderVersion: string;
  composerTargets: string;
  constraintsUrlTemplate: string;
}

export const RUNTIME: RuntimeConfig = runtimeJson;

export const GENERATOR_NAME = "Airflow DAG Creator for GCP";

export function constraintsUrl(rt: RuntimeConfig = RUNTIME): string {
  return rt.constraintsUrlTemplate
    .replace("{airflowVersion}", rt.airflowVersion)
    .replace("{pythonVersion}", rt.pythonVersion);
}

export function runtimeLabel(rt: RuntimeConfig = RUNTIME): string {
  return `${rt.composerTargets} · Airflow ${rt.airflowVersion} · Python ${rt.pythonVersion}`;
}
