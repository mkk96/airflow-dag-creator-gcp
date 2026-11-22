import { describe, expect, it } from "vitest";
import {
  assign,
  isPyIdentifier,
  pyCall,
  pyDict,
  pyDocstring,
  pyFString,
  pyIdent,
  pyInt,
  pyList,
  pyName,
  pyStr,
  pyStrCode,
  render,
  toSnakeIdentifier,
} from "../../src/lib/python";

describe("pyStrCode", () => {
  it("prefers double quotes", () => {
    expect(pyStrCode("hello")).toBe('"hello"');
  });

  it("switches to single quotes when there are more double quotes (like ruff)", () => {
    expect(pyStrCode('say "hi"')).toBe(`'say "hi"'`);
    expect(pyStrCode('"')).toBe(`'"'`);
  });

  it("escapes the chosen quote and backslashes", () => {
    expect(pyStrCode(`it's "x" and "y"`)).toBe(`'it\\'s "x" and "y"'`);
    expect(pyStrCode("C:\\temp")).toBe('"C:\\\\temp"');
    expect(pyStrCode(`O'Brien`)).toBe(`"O'Brien"`);
  });

  it("escapes control characters, newlines and invisible/bidi characters", () => {
    expect(pyStrCode("a\nb\tc\rd")).toBe('"a\\nb\\tc\\rd"');
    expect(pyStrCode("\u0000\u007f")).toBe('"\\x00\\x7f"');
    expect(pyStrCode("x\u202ey")).toBe('"x\\u202ey"');
    expect(pyStrCode("\u2028")).toBe('"\\u2028"');
  });

  it("keeps printable unicode as-is", () => {
    expect(pyStrCode("ünïcødé → 東京")).toBe('"ünïcødé → 東京"');
  });

  it("cannot be broken out of with injection attempts", () => {
    const attempts = ['"); import os; os.system("rm -rf /"); ("', '"""', "\\", '\\"', "'''", "\n__import__('os')"];
    for (const a of attempts) {
      const code = pyStrCode(a);
      // Exactly one opening and one closing unescaped quote of the chosen kind.
      const q = code[0];
      const inner = code.slice(1, -1);
      expect(code.endsWith(q)).toBe(true);
      expect(inner.replace(/\\./g, "")).not.toContain(q);
      expect(inner).not.toContain("\n");
    }
  });
});

describe("identifiers", () => {
  it("rejects non-identifiers and keywords", () => {
    expect(() => pyIdent("ok_name")).not.toThrow();
    expect(() => pyIdent("1abc")).toThrow();
    expect(() => pyIdent("a-b")).toThrow();
    expect(() => pyIdent("class")).toThrow();
    expect(() => pyIdent("x; import os")).toThrow();
  });

  it("toSnakeIdentifier always yields a valid identifier", () => {
    expect(toSnakeIdentifier("Customer Daily-Snapshot")).toBe("customer_daily_snapshot");
    expect(toSnakeIdentifier("2024 orders")).toBe("item_2024_orders");
    expect(toSnakeIdentifier("class")).toBe("class_");
    expect(toSnakeIdentifier("Ünïcode")).toBe("unicode");
    expect(isPyIdentifier(toSnakeIdentifier("Ünïcødé"))).toBe(true);
    expect(toSnakeIdentifier("!!!")).toBe("item");
  });

  it("pyFString only accepts safe literal parts", () => {
    expect(render(pyFString([{ name: "ENV" }, { text: "_orders" }]))).toBe('f"{ENV}_orders"');
    expect(() => pyFString([{ text: '"}{os.system("x")}' }])).toThrow();
  });
});

describe("layout (ruff/black compatible)", () => {
  it("keeps short calls on one line", () => {
    expect(assign("X", pyCall("timedelta", [], [["minutes", pyInt(5)]]))).toBe("X = timedelta(minutes=5)");
  });

  it("explodes long or forced containers with magic trailing commas", () => {
    const out = assign("D", pyDict([["a", pyInt(1)], ["b", pyList([pyStr("x")])]], { explode: true }));
    expect(out).toBe('D = {\n    "a": 1,\n    "b": ["x"],\n}');
  });

  it("explodes when the flat form exceeds 88 columns", () => {
    const long = pyList(Array.from({ length: 12 }, (_, i) => pyStr(`item_number_${i}`)));
    const out = assign("ITEMS", long);
    expect(out.split("\n").length).toBe(14);
    expect(out.split("\n").every((l) => l.length <= 88)).toBe(true);
  });

  it("pyName validates dotted names", () => {
    expect(render(pyName("os.path.join"))).toBe("os.path.join");
    expect(() => pyName("os.path.join()")).toThrow();
  });
});

describe("pyDocstring", () => {
  it("cannot terminate early and escapes backslashes", () => {
    const d = pyDocstring('Title\n\nHas """ quotes and a \\ backslash  \n');
    expect(d.startsWith('"""Title')).toBe(true);
    expect(d.endsWith('\n"""')).toBe(true);
    expect(d.slice(3, -3)).not.toMatch(/(?<!\\)"""/);
    expect(d).toContain("\\\\ backslash");
    expect(d).not.toMatch(/ +\n/);
  });
});
