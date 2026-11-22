import {
  CircleCheck,
  CircleX,
  ClipboardCopy,
  CloudUpload,
  Download,
  ExternalLink,
  Info,
  KeyRound,
  ListChecks,
  LoaderCircle,
  LogIn,
  Rocket,
  ShieldCheck,
  Terminal,
  TriangleAlert,
  X,
} from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { ENVIRONMENTS } from "../config/naming";
import { downloadText } from "../lib/bundle";
import { type ComposerEnvironment, deploy, getEnvironment, listEnvironments, type LogLevel } from "../lib/deploy/gcp";
import { type AccessToken, BUILT_IN_CLIENT_ID, signIn, signOut } from "../lib/deploy/googleAuth";
import { buildPlan, COMPOSER_REGIONS, defaultTarget, type DeployTarget, validateTarget } from "../lib/deploy/plan";
import { renderDeployScript } from "../lib/deploy/script";
import type { GeneratedBundle } from "../scenarios/types";

interface Props {
  bundle: GeneratedBundle;
  onClose: () => void;
  notify: (text: string, kind?: "ok" | "error") => void;
}

const TARGET_KEY = "dag-creator:v1:deploy-target";
const CLIENT_ID_KEY = "dag-creator:v1:oauth-client-id";

function load(key: string): string | null {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}
function save(key: string, value: string) {
  try {
    localStorage.setItem(key, value);
  } catch {
    /* ignore */
  }
}

const input =
  "w-full rounded-lg border border-slate-200 bg-white px-3 py-2 font-mono text-[13px] text-slate-900 shadow-xs outline-none transition " +
  "placeholder:font-sans placeholder:text-slate-400 focus:border-indigo-500 focus:ring-4 focus:ring-indigo-500/10";

function Toggle({ checked, onChange, children }: { checked: boolean; onChange: (v: boolean) => void; children: React.ReactNode }) {
  return (
    <label className="flex cursor-pointer items-start gap-2.5 rounded-lg border border-slate-200 bg-white p-2.5 text-[13px] text-slate-700 hover:border-slate-300">
      <input type="checkbox" className="mt-0.5 h-4 w-4 accent-indigo-600" checked={checked} onChange={(e) => onChange(e.target.checked)} />
      <span>{children}</span>
    </label>
  );
}

export function DeployDialog({ bundle, onClose, notify }: Props) {
  const [tab, setTab] = useState<"now" | "script">("now");
  const [target, setTarget] = useState<DeployTarget>(() => {
    const base = defaultTarget(bundle);
    try {
      const saved = JSON.parse(load(TARGET_KEY) ?? "{}") as Partial<DeployTarget>;
      return { ...base, project: saved.project ?? "", region: saved.region ?? "", environment: saved.environment ?? "" };
    } catch {
      return base;
    }
  });
  const [clientId, setClientId] = useState(() => BUILT_IN_CLIENT_ID || load(CLIENT_ID_KEY) || "");
  const [token, setToken] = useState<AccessToken | null>(null);
  const [env, setEnv] = useState<ComposerEnvironment | null>(null);
  const [envChoices, setEnvChoices] = useState<string[]>([]);
  const [busy, setBusy] = useState<string | null>(null);
  const [log, setLog] = useState<{ level: LogLevel; text: string }[]>([]);
  const [result, setResult] = useState<{ dagUrl: string } | null>(null);

  useEffect(() => {
    save(TARGET_KEY, JSON.stringify({ project: target.project, region: target.region, environment: target.environment }));
  }, [target.project, target.region, target.environment]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && !busy && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [busy, onClose]);

  const set = <K extends keyof DeployTarget>(k: K, v: DeployTarget[K]) => {
    setTarget((t) => {
      const next = { ...t, [k]: v };
      if (k === "targetEnv" && bundle.deploy.codeBuckets) next.codeBucket = bundle.deploy.codeBuckets[String(v)] ?? t.codeBucket;
      return next;
    });
    if (k === "project" || k === "region" || k === "environment") setEnv(null);
  };

  const errors = validateTarget(target, bundle);
  const valid = Object.keys(errors).length === 0;
  const plan = useMemo(() => (valid ? buildPlan(bundle, target) : []), [bundle, target, valid]);
  const script = useMemo(() => (valid ? renderDeployScript(bundle, target) : ""), [bundle, target, valid]);
  const envVar = bundle.deploy.deployEnvVar;
  const variable = bundle.deploy.variables?.[target.targetEnv];
  const tokenValid = !!token && token.expiresAt > Date.now() + 60_000;
  const envMismatch = env && env.envVariables[envVar] !== target.targetEnv;

  const run = async (label: string, fn: () => Promise<void>) => {
    setBusy(label);
    try {
      await fn();
    } catch (e) {
      const msg = (e as Error).message;
      setLog((l) => [...l, { level: "error", text: msg }]);
      notify(msg, "error");
    } finally {
      setBusy(null);
    }
  };

  const ensureToken = async (): Promise<string> => {
    if (tokenValid) return token!.token;
    const t = await signIn(clientId.trim());
    if (!BUILT_IN_CLIENT_ID) save(CLIENT_ID_KEY, clientId.trim());
    setToken(t);
    return t.token;
  };

  const onSignIn = () => run("signin", async () => void (await ensureToken()));

  const onFindEnvs = () =>
    run("list", async () => {
      const tk = await ensureToken();
      const names = await listEnvironments(tk, target.project, target.region);
      setEnvChoices(names);
      if (names.length === 0) notify("No Composer environments in that project/region.", "error");
      else if (!names.includes(target.environment)) set("environment", names[0]);
    });

  const onCheck = () =>
    run("check", async () => {
      const tk = await ensureToken();
      const e = await getEnvironment(tk, target);
      setEnv(e);
    });

  const onDeploy = () =>
    run("deploy", async () => {
      setLog([]);
      setResult(null);
      const tk = await ensureToken();
      const r = await deploy(tk, bundle, target, (level, text) => setLog((l) => [...l, { level, text }]));
      setResult({ dagUrl: r.dagUrl });
      notify(`Deployed ${bundle.dagId} to ${target.environment}`);
    });

  const copyScript = async () => {
    try {
      await navigator.clipboard.writeText(script);
      notify("Deploy script copied — paste it into Cloud Shell");
    } catch {
      notify("Clipboard is not available; use Download instead.", "error");
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-slate-950/50 p-3 backdrop-blur-sm sm:p-8" onMouseDown={() => !busy && onClose()}>
      <div
        role="dialog"
        aria-modal="true"
        aria-label="Deploy to Cloud Composer"
        className="animate-pop-in w-full max-w-5xl rounded-2xl bg-white shadow-2xl ring-1 ring-black/5"
        onMouseDown={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center gap-3 border-b border-slate-100 px-5 py-4">
          <div className="grid h-10 w-10 place-items-center rounded-xl bg-gradient-to-br from-indigo-500 to-violet-600 text-white shadow-md">
            <Rocket className="h-5 w-5" />
          </div>
          <div className="min-w-0">
            <h2 className="text-lg font-semibold text-slate-900">Deploy to Cloud Composer</h2>
            <p className="truncate text-sm text-slate-500">
              <span className="font-mono">{bundle.dagId}</span> · {bundle.files.filter((f) => f.role !== "readme" && f.role !== "config").length} file(s)
            </p>
          </div>
          <button onClick={onClose} disabled={!!busy} className="ml-auto rounded-lg p-2 text-slate-400 hover:bg-slate-100 hover:text-slate-700 disabled:opacity-40" aria-label="Close">
            <X className="h-5 w-5" />
          </button>
        </div>

        <div className="grid grid-cols-1 gap-0 lg:grid-cols-[5fr_6fr]">
          {/* Left: what we need */}
          <div className="flex flex-col gap-4 border-slate-100 p-5 lg:border-r">
            <section>
              <h3 className="mb-2 flex items-center gap-2 text-[13px] font-semibold text-slate-800">
                <span className="grid h-5 w-5 place-items-center rounded-full bg-indigo-100 text-[11px] text-indigo-700">1</span>
                Where is your Composer environment?
              </h3>
              <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                <label className="flex flex-col gap-1 text-[12px] font-semibold text-slate-700">
                  Composer project ID
                  <input className={input} placeholder="e.g. acme-prd-composer" value={target.project} onChange={(e) => set("project", e.target.value.trim())} spellCheck={false} />
                  {target.project && errors.project && <span className="text-[11px] font-medium text-red-600">{errors.project}</span>}
                </label>
                <label className="flex flex-col gap-1 text-[12px] font-semibold text-slate-700">
                  Region
                  <input className={input} placeholder="e.g. northamerica-northeast1" list="composer-regions" value={target.region} onChange={(e) => set("region", e.target.value.trim())} spellCheck={false} />
                  <datalist id="composer-regions">
                    {COMPOSER_REGIONS.map((r) => (
                      <option key={r} value={r} />
                    ))}
                  </datalist>
                  {target.region && errors.region && <span className="text-[11px] font-medium text-red-600">{errors.region}</span>}
                </label>
                <label className="flex flex-col gap-1 text-[12px] font-semibold text-slate-700 sm:col-span-2">
                  Environment name
                  <div className="flex gap-2">
                    <input className={input} placeholder="e.g. composer-prd" list="composer-envs" value={target.environment} onChange={(e) => set("environment", e.target.value.trim())} spellCheck={false} />
                    <datalist id="composer-envs">
                      {envChoices.map((n) => (
                        <option key={n} value={n} />
                      ))}
                    </datalist>
                    {tab === "now" && (
                      <button
                        type="button"
                        onClick={onFindEnvs}
                        disabled={!!busy || !!errors.project || !!errors.region || !clientId}
                        className="shrink-0 rounded-lg border border-slate-200 px-3 text-xs font-semibold text-slate-700 hover:bg-slate-50 disabled:opacity-40"
                        title="Sign in and list the environments in this project/region"
                      >
                        {busy === "list" ? <LoaderCircle className="h-4 w-4 animate-spin" /> : "Find"}
                      </button>
                    )}
                  </div>
                  {target.environment && errors.environment && <span className="text-[11px] font-medium text-red-600">{errors.environment}</span>}
                </label>
              </div>
            </section>

            <section>
              <h3 className="mb-2 flex items-center gap-2 text-[13px] font-semibold text-slate-800">
                <span className="grid h-5 w-5 place-items-center rounded-full bg-indigo-100 text-[11px] text-indigo-700">2</span>
                Which environment is it?
              </h3>
              <div className="inline-flex rounded-lg border border-slate-200 bg-slate-50 p-0.5">
                {ENVIRONMENTS.map((e) => (
                  <button
                    key={e}
                    type="button"
                    onClick={() => set("targetEnv", e)}
                    className={`rounded-md px-4 py-1.5 text-xs font-semibold uppercase transition ${
                      target.targetEnv === e ? "bg-white text-indigo-700 shadow-sm ring-1 ring-slate-200" : "text-slate-500 hover:text-slate-800"
                    }`}
                  >
                    {e}
                  </button>
                ))}
              </div>
              <p className="mt-1.5 text-[11.5px] leading-snug text-slate-500">
                The DAG reads <code className="font-mono">{envVar}</code> on the Composer environment to pick this env's values. It must equal <b>{target.targetEnv}</b> there.
              </p>
            </section>

            <section className="flex flex-col gap-2">
              <h3 className="flex items-center gap-2 text-[13px] font-semibold text-slate-800">
                <span className="grid h-5 w-5 place-items-center rounded-full bg-indigo-100 text-[11px] text-indigo-700">3</span>
                Options
              </h3>
              {variable && (
                <Toggle checked={target.setVariable} onChange={(v) => set("setVariable", v)}>
                  Set Airflow Variable <code className="font-mono text-[12px]">{variable.name}</code> with this env's values
                </Toggle>
              )}
              {bundle.deploy.codeBuckets && (
                <div className="flex flex-col gap-1.5">
                  <Toggle checked={target.uploadScripts} onChange={(v) => set("uploadScripts", v)}>
                    Upload scripts to the code bucket
                  </Toggle>
                  {target.uploadScripts && (
                    <label className="flex flex-col gap-1 pl-1 text-[12px] font-semibold text-slate-700">
                      Code bucket
                      <input className={input} value={target.codeBucket} onChange={(e) => set("codeBucket", e.target.value.trim())} spellCheck={false} />
                      {errors.codeBucket && <span className="text-[11px] font-medium text-red-600">{errors.codeBucket}</span>}
                    </label>
                  )}
                </div>
              )}
              <Toggle checked={target.setDeployEnv} onChange={(v) => set("setDeployEnv", v)}>
                Set <code className="font-mono text-[12px]">{envVar}={target.targetEnv}</code> on the environment if it's missing or different
                <span className="mt-0.5 block text-[11px] text-amber-700">Restarts Airflow components (10–25 min). Usually a one-time step per environment.</span>
              </Toggle>
            </section>

            <details className="group rounded-xl border border-slate-200 bg-slate-50/60 p-3">
              <summary className="flex cursor-pointer list-none items-center gap-2 text-[13px] font-semibold text-slate-800">
                <ShieldCheck className="h-4 w-4 text-emerald-600" /> Permissions you'll need
              </summary>
              <div className="mt-2 space-y-2 text-[12px] leading-relaxed text-slate-600">
                <p className="font-semibold text-slate-700">For you (the person deploying):</p>
                <ul className="list-disc space-y-0.5 pl-5">
                  <li>
                    <code>roles/composer.environmentAndStorageObjectAdmin</code> on the Composer project, or <code>roles/composer.user</code> + <code>roles/storage.objectAdmin</code> on the environment's bucket
                  </li>
                  {bundle.deploy.codeBuckets && (
                    <li>
                      <code>roles/storage.objectAdmin</code> on the code bucket
                    </li>
                  )}
                  <li>Setting {envVar} needs permission to update the environment (included in the first role)</li>
                </ul>
                <p className="font-semibold text-slate-700">For the Composer service account (at run time):</p>
                <ul className="list-disc space-y-0.5 pl-5">
                  {bundle.deploy.iamRoles.map((r) => (
                    <li key={r}>
                      {r.split("`").map((part, i) => (i % 2 ? <code key={i}>{part}</code> : <span key={i}>{part}</span>))}
                    </li>
                  ))}
                </ul>
              </div>
            </details>
          </div>

          {/* Right: how */}
          <div className="flex min-w-0 flex-col gap-3 bg-slate-50/50 p-5">
            <div className="inline-flex self-start rounded-lg border border-slate-200 bg-white p-0.5 text-xs font-semibold">
              <button onClick={() => setTab("now")} className={`flex items-center gap-1.5 rounded-md px-3 py-1.5 ${tab === "now" ? "bg-indigo-600 text-white shadow" : "text-slate-600 hover:text-slate-900"}`}>
                <CloudUpload className="h-3.5 w-3.5" /> Deploy now
              </button>
              <button onClick={() => setTab("script")} className={`flex items-center gap-1.5 rounded-md px-3 py-1.5 ${tab === "script" ? "bg-indigo-600 text-white shadow" : "text-slate-600 hover:text-slate-900"}`}>
                <Terminal className="h-3.5 w-3.5" /> Cloud Shell script
              </button>
            </div>

            {/* Plan */}
            <div className="rounded-xl border border-slate-200 bg-white p-3">
              <h4 className="mb-1.5 flex items-center gap-1.5 text-[12px] font-semibold text-slate-700">
                <ListChecks className="h-4 w-4 text-indigo-600" /> What will happen
              </h4>
              {valid ? (
                <ol className="list-decimal space-y-0.5 pl-5 font-mono text-[11.5px] text-slate-600">
                  <li className="font-sans">Check the environment is running and {envVar} = {target.targetEnv}{target.setDeployEnv ? " (set it if not)" : ""}</li>
                  {plan.map((s, i) => (
                    <li key={i} className="break-all">
                      {s.label}
                    </li>
                  ))}
                </ol>
              ) : (
                <p className="text-[12px] text-slate-500">Fill in project, region and environment to see the plan.</p>
              )}
            </div>

            {tab === "now" ? (
              <div className="flex flex-col gap-3">
                {!clientId && (
                  <div className="rounded-xl border border-amber-200 bg-amber-50 p-3 text-[12px] leading-relaxed text-amber-900">
                    <p className="flex items-center gap-1.5 font-semibold">
                      <KeyRound className="h-4 w-4" /> Google sign-in isn't configured for this site yet
                    </p>
                    <p className="mt-1">
                      Paste an OAuth Client ID (type <i>Web application</i>) whose authorized JavaScript origin is <code className="font-mono">{window.location.origin}</code>. Or use the{" "}
                      <button className="font-semibold underline" onClick={() => setTab("script")}>
                        Cloud Shell script
                      </button>
                      , which needs no setup.
                    </p>
                  </div>
                )}
                {!BUILT_IN_CLIENT_ID && (
                  <label className="flex flex-col gap-1 text-[12px] font-semibold text-slate-700">
                    OAuth Client ID
                    <input className={input} placeholder="1234567890-abc….apps.googleusercontent.com" value={clientId} onChange={(e) => setClientId(e.target.value.trim())} spellCheck={false} />
                  </label>
                )}

                <div className="flex flex-wrap items-center gap-2">
                  {tokenValid ? (
                    <span className="inline-flex items-center gap-1.5 rounded-lg bg-emerald-50 px-3 py-2 text-xs font-semibold text-emerald-700 ring-1 ring-emerald-200">
                      <CircleCheck className="h-4 w-4" /> Signed in with Google
                      <button
                        className="ml-1 text-emerald-800 underline"
                        onClick={() => {
                          signOut(token!.token);
                          setToken(null);
                        }}
                      >
                        sign out
                      </button>
                    </span>
                  ) : (
                    <button onClick={onSignIn} disabled={!clientId || !!busy} className="inline-flex items-center gap-1.5 rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs font-semibold text-slate-700 shadow-xs hover:bg-slate-50 disabled:opacity-40">
                      {busy === "signin" ? <LoaderCircle className="h-4 w-4 animate-spin" /> : <LogIn className="h-4 w-4" />} Sign in with Google
                    </button>
                  )}
                  <button onClick={onCheck} disabled={!valid || !clientId || !!busy} className="inline-flex items-center gap-1.5 rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs font-semibold text-slate-700 shadow-xs hover:bg-slate-50 disabled:opacity-40">
                    {busy === "check" ? <LoaderCircle className="h-4 w-4 animate-spin" /> : <Info className="h-4 w-4" />} Check environment
                  </button>
                  <button
                    onClick={onDeploy}
                    disabled={!valid || !clientId || !!busy || (!!envMismatch && !target.setDeployEnv)}
                    className="ml-auto inline-flex items-center gap-1.5 rounded-lg bg-gradient-to-r from-indigo-600 to-violet-600 px-4 py-2 text-sm font-semibold text-white shadow-md shadow-indigo-500/25 hover:shadow-lg disabled:opacity-40"
                  >
                    {busy === "deploy" ? <LoaderCircle className="h-4 w-4 animate-spin" /> : <Rocket className="h-4 w-4" />} Deploy
                  </button>
                </div>

                {env && (
                  <div className="rounded-xl border border-slate-200 bg-white p-3 text-[12px] text-slate-600">
                    <div className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5">
                      <span className="text-slate-400">State</span>
                      <span className={env.state === "RUNNING" ? "font-semibold text-emerald-700" : "font-semibold text-amber-700"}>{env.state}</span>
                      <span className="text-slate-400">Image</span>
                      <span className="font-mono">{env.imageVersion}</span>
                      <span className="text-slate-400">DAG folder</span>
                      <span className="font-mono break-all">{env.dagGcsPrefix}</span>
                      <span className="text-slate-400">{envVar}</span>
                      <span className={envMismatch ? "font-semibold text-amber-700" : "font-semibold text-emerald-700"}>{env.envVariables[envVar] ?? "not set"}</span>
                    </div>
                    {envMismatch && !target.setDeployEnv && (
                      <p className="mt-2 flex items-start gap-1.5 text-amber-800">
                        <TriangleAlert className="mt-px h-3.5 w-3.5 shrink-0" />
                        This environment isn't set up as <b>{target.targetEnv}</b>. Tick “Set {envVar}” or choose the matching env.
                      </p>
                    )}
                  </div>
                )}

                {log.length > 0 && (
                  <div className="scroll-thin max-h-56 overflow-y-auto rounded-xl bg-[#16161e] p-3 font-mono text-[11.5px] leading-relaxed">
                    {log.map((l, i) => (
                      <div key={i} className={{ info: "text-slate-400", ok: "text-emerald-400", warn: "text-amber-300", error: "text-red-400" }[l.level]}>
                        {l.level === "error" ? <CircleX className="mr-1 inline h-3 w-3" /> : null}
                        {l.text}
                      </div>
                    ))}
                  </div>
                )}
                {result?.dagUrl && (
                  <a href={result.dagUrl} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1.5 self-start rounded-lg bg-emerald-600 px-3 py-2 text-xs font-semibold text-white shadow hover:bg-emerald-700">
                    Open the DAG in Airflow <ExternalLink className="h-3.5 w-3.5" />
                  </a>
                )}
                <p className="text-[11px] leading-snug text-slate-400">
                  Your Google access token stays in this tab's memory and is sent only to Google APIs (composer.googleapis.com, storage.googleapis.com). Nothing goes through a server.
                </p>
              </div>
            ) : (
              <div className="flex min-w-0 flex-col gap-2">
                <ol className="list-decimal space-y-0.5 pl-5 text-[12px] text-slate-600">
                  <li>
                    Open{" "}
                    <a className="font-semibold text-indigo-700 underline" href="https://shell.cloud.google.com/?show=terminal" target="_blank" rel="noreferrer">
                      Cloud Shell
                    </a>{" "}
                    (or any terminal with an authenticated <code className="font-mono">gcloud</code>).
                  </li>
                  <li>Copy the script below, paste it, and press Enter. Or download it and run <code className="font-mono">bash deploy_{bundle.dagId}.sh</code>.</li>
                </ol>
                <div className="flex gap-2">
                  <button onClick={copyScript} disabled={!valid} className="inline-flex items-center gap-1.5 rounded-lg bg-gradient-to-r from-indigo-600 to-violet-600 px-3 py-2 text-xs font-semibold text-white shadow disabled:opacity-40">
                    <ClipboardCopy className="h-4 w-4" /> Copy script
                  </button>
                  <button
                    onClick={() => downloadText(script, `deploy_${bundle.dagId}.sh`, "text/x-shellscript")}
                    disabled={!valid}
                    className="inline-flex items-center gap-1.5 rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs font-semibold text-slate-700 hover:bg-slate-50 disabled:opacity-40"
                  >
                    <Download className="h-4 w-4" /> Download .sh
                  </button>
                </div>
                <pre className="scroll-thin max-h-72 overflow-auto rounded-xl bg-[#16161e] p-3 font-mono text-[11px] leading-relaxed text-slate-300">
                  {valid ? script.split("\n").filter((l) => !/^[A-Za-z0-9+/=]{20,}$/.test(l)).join("\n").replace(/<<'B64'\nB64/g, "<<'B64' … (file contents, base64) … B64") : "Fill in the environment details first."}
                </pre>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
