import { describe, expect, it } from "vitest";
import { swapEnv, teamRegex, toLabelValue } from "../../src/config/naming";
import * as v from "../../src/validation/rules";

const errs = (i: v.Issues) => i.errors.length;
const warns = (i: v.Issues) => i.warnings.length;

describe("dag_id", () => {
  it("accepts the team convention", () => {
    expect(v.dagId("sales_gcs_to_bq_daily")).toEqual(v.ok());
  });
  it("warns on convention, errors on Airflow rules", () => {
    expect(warns(v.dagId("MyDag"))).toBe(1);
    expect(errs(v.dagId("my dag"))).toBe(1);
    expect(errs(v.dagId("a/b"))).toBe(1);
    expect(errs(v.dagId("x".repeat(251)))).toBe(1);
  });
});

describe("cron", () => {
  it.each(["0 6 * * *", "*/15 * * * *", "30 2 1 * *", "0 9 * * MON-FRI", "0 0 L * *", "0 0 * JAN,JUL 1#1", "0 0-23/2 * * 0,7"])(
    "accepts %s",
    (c) => expect(v.cron(c)).toEqual(v.ok()),
  );
  it.each(["", "* * * *", "60 * * * *", "* 24 * * *", "* * 0 * *", "* * * 13 *", "*/0 * * * *", "5-1 * * * *", "@daily"])(
    "rejects %s",
    (c) => expect(errs(v.cron(c))).toBeGreaterThan(0),
  );
});

describe("GCS", () => {
  it("bucket official rules", () => {
    expect(errs(v.gcsBucket("ab"))).toBeGreaterThan(0);
    expect(errs(v.gcsBucket("Upper-case"))).toBeGreaterThan(0);
    expect(errs(v.gcsBucket("-starts-with-dash"))).toBeGreaterThan(0);
    expect(errs(v.gcsBucket("goog-bucket"))).toBeGreaterThan(0);
    expect(errs(v.gcsBucket("my-google-bucket"))).toBeGreaterThan(0);
    expect(errs(v.gcsBucket("192.168.1.1"))).toBeGreaterThan(0);
    expect(errs(v.gcsBucket("gs://acme-prd-sales-landing"))).toBeGreaterThan(0);
  });
  it("bucket team convention is a warning", () => {
    expect(v.gcsBucket("acme-prd-sales-landing")).toEqual(v.ok());
    const i = v.gcsBucket("my-random-bucket");
    expect(errs(i)).toBe(0);
    expect(warns(i)).toBe(1);
  });
  it("gs:// URIs", () => {
    expect(errs(v.gsUri("gs://acme-prd-sales-landing/schemas/orders.json"))).toBe(0);
    expect(errs(v.gsUri("gs://bucket-only"))).toBe(1);
    expect(errs(v.gsUri("s3://x/y"))).toBe(1);
  });
  it("object paths", () => {
    expect(v.gcsObject("landing/{{ ds_nodash }}/*.csv")).toEqual(v.ok());
    expect(errs(v.gcsObject("a\nb"))).toBe(1);
    expect(errs(v.gcsObject("gs://b/o"))).toBe(1);
  });
});

describe("BigQuery", () => {
  it("project ids", () => {
    expect(v.bqProject("acme-prd-data")).toEqual(v.ok());
    expect(errs(v.bqProject("short"))).toBe(1);
    expect(errs(v.bqProject("1starts-with-digit"))).toBe(1);
    expect(errs(v.bqProject("ends-with-dash-"))).toBe(1);
    expect(v.bqProject("example.com:my-project")).toEqual(v.ok());
  });
  it("datasets", () => {
    expect(v.bqDataset("raw_sales")).toEqual(v.ok());
    expect(warns(v.bqDataset("sales"))).toBe(1);
    expect(errs(v.bqDataset("raw-sales"))).toBe(1);
  });
  it("tables", () => {
    expect(v.bqTable("customer_daily_snapshot")).toEqual(v.ok());
    expect(warns(v.bqTable("Customer Orders"))).toBe(1);
    expect(errs(v.bqTable("orders$20240101"))).toBe(1);
    expect(errs(v.bqTable("a`b"))).toBe(1);
  });
  it("schema json", () => {
    expect(v.bqSchemaJson('[{"name":"id","type":"INT64"}]')).toEqual(v.ok());
    expect(errs(v.bqSchemaJson("{}"))).toBe(1);
    expect(errs(v.bqSchemaJson('[{"name":"bad name","type":"INT64"}]'))).toBe(1);
    expect(errs(v.bqSchemaJson('[{"name":"x","type":"WHAT"}]'))).toBe(1);
  });
});

describe("labels & jinja", () => {
  it("label values", () => {
    expect(v.labelValue("data-eng")).toEqual(v.ok());
    expect(errs(v.labelValue("Data Eng"))).toBe(1);
    expect(toLabelValue("Data Eng/Team")).toBe("data-eng-team");
  });
  it("blocks jinja injection", () => {
    expect(errs(v.noJinja("{{ var.value.secret }}"))).toBe(1);
    expect(errs(v.noJinja("{% raw %}"))).toBe(1);
    expect(v.noJinja("plain")).toEqual(v.ok());
  });
});

describe("naming helpers", () => {
  it("swaps whole env tokens only", () => {
    expect(swapEnv("acme-dev-sales-landing", "dev", "prd")).toBe("acme-prd-sales-landing");
    expect(swapEnv("etl@acme-dev-data.iam.gserviceaccount.com", "dev", "stg")).toBe("etl@acme-stg-data.iam.gserviceaccount.com");
    expect(swapEnv("devices-dev", "dev", "prd")).toBe("devices-prd");
    expect(swapEnv("raw_sales", "dev", "prd")).toBe("raw_sales");
  });
  it("team regexes come from patterns", () => {
    expect(teamRegex("dagId").test("sales_gcs_to_bq_daily")).toBe(true);
    expect(teamRegex("gcsBucket").test("acme-prd-sales-landing")).toBe(true);
    expect(teamRegex("bqDataset").test("curated_sales")).toBe(true);
    expect(teamRegex("taskId").test("load_orders_to_staging")).toBe(true);
  });
});
