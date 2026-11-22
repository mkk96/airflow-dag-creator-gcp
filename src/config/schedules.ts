/** Schedule presets offered in the UI. `custom` reveals a cron input. */
export const SCHEDULE_PRESETS = [
  { value: "none", label: "None (manual / externally triggered)" },
  { value: "@once", label: "@once" },
  { value: "@hourly", label: "@hourly" },
  { value: "@daily", label: "@daily (midnight)" },
  { value: "@weekly", label: "@weekly" },
  { value: "@monthly", label: "@monthly" },
  { value: "custom", label: "Custom cron expression" },
] as const;

/** Frequency token (for dag_id naming) implied by a schedule. */
export function frequencyFor(preset: string, cron: string): string {
  switch (preset) {
    case "@hourly":
      return "hourly";
    case "@daily":
      return "daily";
    case "@weekly":
      return "weekly";
    case "@monthly":
      return "monthly";
    case "custom": {
      const f = cron.trim().split(/\s+/);
      if (f.length !== 5) return "daily";
      const [, hour, dom, , dow] = f;
      if (hour === "*" || hour.includes("/")) return "hourly";
      if (dow !== "*") return "weekly";
      if (dom !== "*") return "monthly";
      return "daily";
    }
    default:
      return "adhoc";
  }
}
