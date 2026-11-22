/**
 * Pub/Sub publish / pull sensor.
 *
 * apache-airflow-providers-google 12.0.0:
 *   - PubSubPublishMessageOperator: template_fields = project_id, topic, messages,
 *     enable_message_ordering, impersonation_chain. Each message's `data` MUST be bytes
 *     (PubSubHook._validate_messages rejects str; base64 strings are deprecated), and
 *     bytes are not rendered by Jinja — so the body is static and run-specific values go
 *     into `attributes` (a dict of strings, rendered at run time). An `ordering_key`
 *     attribute is passed to the publisher as the message ordering key.
 *   - PubSubPullSensor: template_fields = project_id, subscription, impersonation_chain.
 *     Supports deferrable=True. Pulled messages (up to max_messages) are returned and so
 *     pushed to XCom — keep max_messages small, or turn the XCom push off.
 */
import { fillPattern, TEAM } from "../config/naming";
import {
  assign,
  comment,
  pyBool,
  pyCall,
  pyDict,
  pyFromJson,
  pyInt,
  pyList,
  pyName,
  pyStr,
  withHeader,
  type Py,
} from "../lib/python";
import * as v from "../validation/rules";
import { bool, DagContext, list, num, str } from "./common";
import type { Params, Scenario } from "./types";

const publishes = (p: Params) => p.pubsub_mode !== "wait";
const waits = (p: Params) => p.pubsub_mode === "wait" || p.pubsub_mode === "wait_then_publish";
const isJsonBody = (p: Params) => publishes(p) && p.message_format !== "text";
const isTextBody = (p: Params) => publishes(p) && p.message_format === "text";

const utf8Bytes = (s: string) => new TextEncoder().encode(s).length;

/** Pub/Sub topic / subscription ID (resource name rules). */
function pubsubId(value: string, what: string): v.Issues {
  if (!value) return v.err(`${what} is required.`);
  if (value.startsWith("projects/")) return v.err(`Enter the ${what.toLowerCase()} ID only, not projects/.../${what.toLowerCase()}s/...`);
  if (!/^[A-Za-z][A-Za-z0-9\-_.~+%]{2,254}$/.test(value)) {
    return v.err(`${what} IDs are 3-255 characters, start with a letter and use letters, digits or - _ . ~ + %.`);
  }
  if (value.toLowerCase().startsWith("goog")) return v.err(`${what} IDs cannot start with "goog".`);
  return v.ok();
}

/** "key=value" lines (message attributes). */
function parseAttribute(item: string): { key: string; value: string } | null {
  const i = item.indexOf("=");
  if (i <= 0) return null;
  return { key: item.slice(0, i).trim(), value: item.slice(i + 1) };
}

function attributeItem(item: string): v.Issues {
  const kv = parseAttribute(item);
  if (!kv || !kv.key) return v.err(`"${item}" must look like key=value.`);
  if (v.noJinja(kv.key).errors.length) return v.err("Templates are only allowed in the value, not in the key.");
  if (kv.key.toLowerCase().startsWith("goog")) return v.err(`Attribute keys cannot start with "goog".`);
  if (kv.key === "ordering_key") return v.err('Use the "Ordering key" field instead of an ordering_key attribute.');
  if (utf8Bytes(kv.key) > 256) return v.err("Attribute keys are limited to 256 bytes.");
  if (utf8Bytes(kv.value) > 1024) return v.err("Attribute values are limited to 1024 bytes.");
  return v.ok();
}

function parseJson(text: string): { ok: true; value: unknown } | { ok: false; error: string } {
  try {
    return { ok: true, value: JSON.parse(text) };
  } catch (e) {
    return { ok: false, error: (e as Error).message };
  }
}

const DEFAULT_MESSAGE = JSON.stringify({ event: "data_ready", dataset: "curated_notifications", table: "daily_digest" }, null, 2);

export const pubsub: Scenario = {
  id: "pubsub",
  title: "Pub/Sub publish / pull sensor",
  description:
    "Publish a message to a Pub/Sub topic, wait for messages on a subscription, or wait for a message and then publish one (e.g. relay an upstream event to downstream consumers).",
  naming: { source: "composer", target: TEAM.systemCodes.pubsub },
  sections: [
    { id: "mode", title: "What to do" },
    { id: "publish", title: "Publish message", description: "Published to the topic in the Project id set under GCP connection." },
    { id: "sensor", title: "Wait for messages", description: "Pulls from the subscription in the Project id set under GCP connection." },
  ],
  commonDefaults: {
    dag_id: "notifications_composer_to_ps_daily",
    description: "Announce that the daily notifications digest is ready.",
    domain: "notifications",
    tags: ["notifications", "pubsub"],
  },
  fields: [
    {
      key: "pubsub_mode", label: "Mode", type: "select", section: "mode", default: "publish",
      options: [
        { value: "publish", label: "Publish a message" },
        { value: "wait", label: "Wait for messages (pull sensor)" },
        { value: "wait_then_publish", label: "Wait for messages, then publish a message" },
      ],
    },
    // Publish -----------------------------------------------------------------------------
    {
      key: "topic", label: "Topic", type: "text", section: "publish", default: "acme-dev-notifications-events",
      required: true, envSpecific: true, suggestable: true, showIf: publishes,
      validate: (x) => pubsubId(String(x), "Topic"),
      help: "Topic ID only (not projects/.../topics/...). It must already exist. Env-specific.",
    },
    {
      key: "message_format", label: "Message body format", type: "select", section: "publish", default: "json",
      showIf: publishes,
      options: [
        { value: "json", label: "JSON" },
        { value: "text", label: "Plain text" },
      ],
    },
    {
      key: "message_json", label: "Message body (JSON)", type: "code", language: "json", accept: ".json",
      section: "publish", default: DEFAULT_MESSAGE, required: true, showIf: isJsonBody,
      validate: (x) => {
        const r = parseJson(String(x));
        return r.ok ? v.ok() : v.err(`Invalid JSON: ${r.error}`);
      },
      help: "Sent as UTF-8 bytes, the same in every run: Pub/Sub message data is not templated, so put run-specific values (dates, run ids) in the attributes below.",
    },
    {
      key: "message_text", label: "Message body (text)", type: "textarea", section: "publish", default: "",
      showIf: isTextBody,
      help: "Sent as UTF-8 bytes, the same in every run (not templated). May be empty when attributes are set.",
    },
    {
      key: "message_attributes", label: "Message attributes", type: "list", section: "publish",
      default: ["source_dag={{ dag.dag_id }}", "run_date={{ ds }}"], allowJinja: true, showIf: publishes,
      validateItem: (x) => attributeItem(x),
      help: "One key=value per line. Values may use Airflow macros such as {{ ds }} or {{ run_id }}; subscribers can filter or de-duplicate on them.",
    },
    {
      key: "ordering_key", label: "Ordering key (optional)", type: "text", section: "publish", default: "",
      allowJinja: true, showIf: publishes,
      help: "Set to deliver messages with the same key in order (the subscription must have message ordering enabled). Leave empty otherwise.",
    },
    // Sensor ------------------------------------------------------------------------------
    {
      key: "subscription", label: "Subscription", type: "text", section: "sensor",
      default: "acme-dev-notifications-events-composer", required: true, envSpecific: true, suggestable: true,
      showIf: waits, validate: (x) => pubsubId(String(x), "Subscription"),
      help: "Subscription ID only (not projects/.../subscriptions/...). Use a pull subscription dedicated to this DAG. Env-specific.",
    },
    {
      key: "max_messages", label: "Max messages per pull", type: "number", section: "sensor", default: 1,
      min: 1, max: 1000, showIf: waits,
      help: "The sensor succeeds as soon as at least one message arrives and returns up to this many. Pulled messages are stored in XCom, so keep it small.",
    },
    {
      key: "ack_messages", label: "Acknowledge pulled messages", type: "boolean", section: "sensor", default: true,
      showIf: waits,
      help: "Removes the pulled messages from the subscription. If off, they are redelivered and the next run sees them again.",
    },
    {
      key: "push_messages_xcom", label: "Store pulled messages in XCom", type: "boolean", section: "sensor",
      default: true, showIf: waits,
      help: "Lets downstream tasks read the messages (data is base64). Turn off when you only need to know that a message arrived.",
    },
    {
      key: "sensor_timeout_minutes", label: "Wait timeout (minutes)", type: "number", section: "sensor", default: 60,
      min: 1, max: 10080, showIf: waits,
      help: "The sensor fails if no message arrives within this time.",
    },
    {
      key: "poke_interval_seconds", label: "Check every (seconds)", type: "number", section: "sensor", default: 60,
      min: 10, max: 3600, showIf: waits,
    },
  ],

  samples: {
    publish_json: {
      dag_id: "notifications_composer_to_ps_daily",
      email: ["notifications-oncall@example.com"],
    },
    wait_then_publish_variables: {
      dag_id: "alerts_ps_to_ps_hourly",
      description: "Relay raw alert events to the escalation topic, in order.",
      domain: "alerts",
      config_mode: "variables",
      env: "prd",
      project_id: "acme-prd-alerts",
      impersonation_chain: ["alerts-relay@acme-prd-alerts.iam.gserviceaccount.com"],
      pubsub_mode: "wait_then_publish",
      subscription: "acme-prd-alerts-raw-composer",
      max_messages: 5,
      topic: "acme-prd-alerts-escalations",
      message_format: "text",
      message_text: "Raw alerts received; escalation check for O'Brien's \"tier-1\" queue.",
      message_attributes: ["source_dag={{ dag.dag_id }}", "window_start={{ data_interval_start }}", "severity=high"],
      ordering_key: "alerts-{{ ds }}",
      schedule_preset: "@hourly",
      dagrun_timeout_minutes: 55,
      sensor_timeout_minutes: 40,
      execution_timeout_minutes: 45,
      tags: ["alerts", "pubsub"],
    },
    wait_only_reschedule: {
      dag_id: "notifications_ps_to_composer_daily",
      description: "Wait for the upstream 'export finished' message before the day's processing.",
      pubsub_mode: "wait",
      deferrable: false,
      max_messages: 3,
      push_messages_xcom: false,
      sensor_timeout_minutes: 120,
      poke_interval_seconds: 300,
      schedule_preset: "custom",
      schedule_cron: "0 4 * * *",
      tags: ["notifications", "pubsub", "sensor"],
    },
  },

  validate(p) {
    const out: Record<string, v.Issues> = {};
    const add = (k: string, i: v.Issues) => {
      out[k] = out[k] ? v.merge(out[k], i) : i;
    };
    if (publishes(p)) {
      if (isTextBody(p)) {
        if (!str(p, "message_text") && !list(p, "message_attributes").length) {
          add("message_text", v.err("Enter a message body or at least one attribute."));
        }
        if (v.noJinja(String(p.message_text ?? "")).errors.length) {
          add("message_text", v.err("The message body is not templated; put {{ }} macros in the attributes instead."));
        }
      }
      if (isJsonBody(p) && v.noJinja(String(p.message_json ?? "")).errors.length) {
        add("message_json", v.err("The message body is not templated; put {{ }} macros in the attributes instead."));
      }
      const body = isJsonBody(p) ? String(p.message_json ?? "") : String(p.message_text ?? "");
      if (utf8Bytes(body) > 10 * 1024) {
        add(isJsonBody(p) ? "message_json" : "message_text", v.warn("Large message embedded in the DAG file: keep messages small (pass a gs:// path instead of the data)."));
      }
      const attrs = list(p, "message_attributes");
      if (attrs.length > 100) add("message_attributes", v.err("Pub/Sub allows at most 100 attributes per message."));
      const keys = attrs.map((a) => parseAttribute(a)?.key ?? "");
      const dupes = keys.filter((k, i) => k && keys.indexOf(k) !== i);
      if (dupes.length) add("message_attributes", v.err(`Duplicate key(s): ${[...new Set(dupes)].join(", ")}.`));
      const ok = String(p.ordering_key ?? "");
      if (utf8Bytes(ok) > 1024) add("ordering_key", v.err("Ordering keys are limited to 1024 bytes."));
    }
    if (waits(p)) {
      if (num(p, "max_messages") > 10 && bool(p, "push_messages_xcom")) {
        add("max_messages", v.warn("Pulled messages are stored in XCom (the Airflow database): keep this small or turn the XCom push off."));
      }
      if (!bool(p, "ack_messages")) {
        add("ack_messages", v.warn("Nothing in this DAG acknowledges the messages: they are redelivered and the next run succeeds on the same messages."));
      }
      const timeout = num(p, "sensor_timeout_minutes");
      if (bool(p, "deferrable") && timeout > num(p, "execution_timeout_minutes")) {
        add("sensor_timeout_minutes", v.warn("A deferred sensor is also stopped by the task execution timeout, which is shorter. Raise the execution timeout."));
      }
      if (timeout > num(p, "dagrun_timeout_minutes")) {
        add("sensor_timeout_minutes", v.warn("Longer than the DAG run timeout: the run times out before the sensor does."));
      }
    }
    return out;
  },

  suggest(ctx) {
    const out: Partial<Params> = {};
    // No topic pattern in naming.ts: reuse the team resource pattern {org}-{env}-{domain}-{purpose}.
    const topic = fillPattern("gcsBucket", { ...ctx, purpose: "events" });
    if (topic) {
      out.topic = topic;
      out.subscription = `${topic}-composer`;
    }
    return out;
  },

  generate(p) {
    const ctx = new DagContext(pubsub, p);
    const constants: string[] = [];
    const docSteps: string[] = [];
    const iamRoles: string[] = [];
    const notes: string[] = [];

    // --- sensor ------------------------------------------------------------------------
    const sensorId = "wait_for_messages";
    let sensorTask: Py | null = null;
    if (waits(p)) {
      ctx.importFrom("airflow.providers.google.cloud.sensors.pubsub", "PubSubPullSensor");
      sensorTask = pyCall(
        "PubSubPullSensor",
        [],
        [
          ["task_id", pyStr(sensorId)],
          ["project_id", pyStr(ctx.ref("project_id"))],
          ["subscription", pyStr(ctx.ref("subscription"))],
          ["max_messages", pyInt(num(p, "max_messages"))],
          ["ack_messages", pyBool(bool(p, "ack_messages"))],
          ["gcp_conn_id", pyName("GCP_CONN_ID")],
          ["impersonation_chain", ctx.impersonationChain()],
          ["do_xcom_push", bool(p, "push_messages_xcom") ? null : pyBool(false)],
          ...ctx.sensorKwargs(num(p, "sensor_timeout_minutes"), num(p, "poke_interval_seconds")),
        ],
        { explode: true },
      );
      docSteps.push(
        `\`${sensorId}\`: wait (up to ${num(p, "sensor_timeout_minutes")} min) until the \`subscription\` has at least one message, pull up to ${num(p, "max_messages")}` +
          (bool(p, "ack_messages") ? " and acknowledge them" : " without acknowledging them") +
          (bool(p, "push_messages_xcom") ? "; the messages are stored in XCom." : "."),
      );
      iamRoles.push("`roles/pubsub.subscriber` on the subscription (pull and acknowledge)");
      notes.push(
        bool(p, "push_messages_xcom")
          ? `The sensor stores up to ${num(p, "max_messages")} pulled message(s) in XCom (base64 data plus attributes). Keep messages and max_messages small; publish a gs:// path rather than the data itself.`
          : "Pulled messages are not stored in XCom; the sensor only signals that a message arrived.",
      );
      if (bool(p, "ack_messages")) {
        notes.push("Messages are acknowledged as soon as they are pulled: a failed downstream task does not get them back. Re-running only the downstream tasks is safe; re-running the sensor waits for a new message.");
      }
    }

    // --- publish -----------------------------------------------------------------------
    const publishId = "publish_message";
    let publishTask: Py | null = null;
    if (publishes(p)) {
      ctx.importFrom("airflow.providers.google.cloud.operators.pubsub", "PubSubPublishMessageOperator");
      let hasData = true;
      if (isJsonBody(p)) {
        ctx.importFrom("json", "dumps");
        const parsed = parseJson(String(p.message_json ?? "{}"));
        constants.push(
          comment("Message body. Pub/Sub data must be bytes and is not templated: run-specific values go in\nthe attributes, which are rendered at run time."),
          assign("MESSAGE", pyFromJson(parsed.ok ? parsed.value : {})),
          assign("MESSAGE_JSON", pyCall("dumps", [pyName("MESSAGE")])),
          assign("MESSAGE_DATA", pyCall("MESSAGE_JSON.encode")),
        );
      } else if (String(p.message_text ?? "") !== "") {
        constants.push(
          comment("Message body. Pub/Sub data must be bytes and is not templated: run-specific values go in\nthe attributes, which are rendered at run time."),
          assign("MESSAGE_TEXT", pyStr(String(p.message_text).replace(/\r\n?/g, "\n"))),
          assign("MESSAGE_DATA", pyCall("MESSAGE_TEXT.encode")),
        );
      } else {
        hasData = false;
      }
      const attrs: Array<[string, Py]> = list(p, "message_attributes")
        .map(parseAttribute)
        .filter((x): x is { key: string; value: string } => x !== null)
        .map((a) => [a.key, pyStr(a.value)]);
      const ordering = String(p.ordering_key ?? "").trim();
      if (ordering) attrs.push(["ordering_key", pyStr(ordering)]);
      const message: Array<[string, Py]> = [];
      if (hasData) message.push(["data", pyName("MESSAGE_DATA")]);
      if (attrs.length) message.push(["attributes", pyDict(attrs, { explode: true })]);
      publishTask = pyCall(
        "PubSubPublishMessageOperator",
        [],
        [
          ["task_id", pyStr(publishId)],
          ["project_id", pyStr(ctx.ref("project_id"))],
          ["topic", pyStr(ctx.ref("topic"))],
          ["messages", pyList([pyDict(message, { explode: true })])],
          ["enable_message_ordering", ordering ? pyBool(true) : null],
          ["gcp_conn_id", pyName("GCP_CONN_ID")],
          ["impersonation_chain", ctx.impersonationChain()],
        ],
        { explode: true },
      );
      const attrKeys = attrs.filter(([k]) => k !== "ordering_key").map(([k]) => `\`${k}\``);
      docSteps.push(
        `\`${publishId}\`: publish one ${isJsonBody(p) ? "JSON" : "text"} message to the \`topic\`` +
          (attrKeys.length ? ` with attributes ${attrKeys.join(", ")}` : "") +
          (ordering ? " (ordered by `ordering_key`)" : "") +
          ".",
      );
      iamRoles.push("`roles/pubsub.publisher` on the topic");
      notes.push(
        "Publishing is at-least-once and a retry or re-run publishes again: subscribers should de-duplicate, e.g. on the `source_dag` + `run_date` attributes.",
      );
      if (ordering) notes.push("Ordered delivery needs message ordering enabled on the subscription(s) reading the topic.");
    }

    // --- DAG body ----------------------------------------------------------------------
    const body: string[] = [];
    if (sensorTask && publishTask) {
      ctx.importFrom("airflow.utils.task_group", "TaskGroup");
      body.push(
        withHeader(
          pyCall("TaskGroup", [], [
            ["group_id", pyStr("relay")],
            ["tooltip", pyStr("Wait for a Pub/Sub message, then publish one")],
          ]),
          null,
          1,
        ),
        assign(sensorId, sensorTask, 2),
        "",
        assign(publishId, publishTask, 2),
        "",
        `    ${sensorId} >> ${publishId}`,
      );
    } else if (sensorTask) {
      body.push(assign(sensorId, sensorTask, 1));
    } else if (publishTask) {
      body.push(assign(publishId, publishTask, 1));
    }

    if (list(p, "impersonation_chain").length) {
      iamRoles.push("`roles/iam.serviceAccountTokenCreator` for the Composer service account on the impersonated service account");
    }
    if (waits(p) && bool(p, "deferrable")) {
      notes.push(
        "The sensor runs in deferrable mode: make sure the Composer environment has a triggerer (Composer 2: set triggerer count >= 1). The task execution timeout also caps the wait.",
      );
    }
    return ctx.assemble({ docSteps, constants, body, iamRoles, readmeNotes: notes });
  },
};
