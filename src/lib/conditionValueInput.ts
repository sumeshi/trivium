import type { ColumnKind, CompareOp } from "./types";
import type { ConditionValuesValidation, ValidateConditionValuesArgs } from "./backend";

export type ConditionValueCheckInput = ValidateConditionValuesArgs;
export type ConditionValueCheckResult = ConditionValuesValidation;
export type ConditionValueCheckState =
  | { status: "checking" }
  | { status: "checked"; result: ConditionValueCheckResult }
  | { status: "unavailable"; error: unknown };

export function isOrderedCondition(op: CompareOp): boolean {
  return op === "gt" || op === "ge" || op === "lt" || op === "le";
}

/** Formatting help follows the evaluator: equality/membership remain text. */
export function conditionValueHelp(kind: ColumnKind, op: CompareOp, exact = false) {
  if (op === "regex") return {
    placeholder: "e.g. (?i)error|failed",
    hint: "Regular expression (Rust syntax); (?i) ignores case.",
  };
  if (isOrderedCondition(op) && kind === "datetime") return {
    placeholder: "2026-11-12T09:30:00.123456789Z",
    hint: "Timestamp: YYYY-MM-DDTHH:mm:ss[.fraction]Z or ±HH:mm; date-only and YYYY-MM-DD HH:mm:ss also work. No timezone means UTC. Up to 9 fractional digits.",
  };
  if (isOrderedCondition(op) && kind === "int") return {
    placeholder: "e.g. 18446744073709551615",
    hint: "Whole integer; large values stay exact text.",
  };
  if (op === "in") return {
    placeholder: exact ? "One exact source value" : "a, b, c",
    hint: exact
      ? "One exact source value; commas are part of it. Editing switches to a comma-separated list."
      : "Comma-separated text values. Each value is matched literally.",
  };
  if (op === "eq" || op === "ne") return {
    placeholder: "Exact text value",
    hint: kind === "datetime"
      ? "Exact text comparison: timezone spelling and fractional digits must match the source. Use <, ≤, > or ≥ for time comparison."
      : "Exact text comparison; spelling is preserved.",
  };
  if (isOrderedCondition(op) && kind === "float") return {
    placeholder: "Comparison value",
    hint: "Decimal ordered comparisons are not supported by this evaluator. Check the engine guidance below before relying on the result.",
  };
  if (isOrderedCondition(op)) return {
    placeholder: "Integer or timestamp",
    hint: "Ordered comparisons accept integers or timestamps. The engine checks the value below.",
  };
  return { placeholder: "Text to match", hint: "Case-insensitive text match; dates and numbers are searched as text." };
}

/** Compose only explicit picker selections. Never parse/reformat evidence text. */
export function utcPickerValue(date: string, time: string): string | null {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || !/^\d{2}:\d{2}(?::\d{2})?$/.test(time)) return null;
  return `${date}T${time.length === 5 ? `${time}:00` : time}Z`;
}

/** Debounce parser-only IPC and discard every superseded/disposed response. */
export function createConditionValueChecker(options: {
  validate: (input: ConditionValueCheckInput) => Promise<ConditionValueCheckResult>;
  publish: (state: ConditionValueCheckState) => void;
  delay?: number;
}) {
  let generation = 0;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let disposed = false;
  const clear = () => { if (timer) clearTimeout(timer); timer = null; };
  return {
    schedule(input: ConditionValueCheckInput) {
      if (disposed) return;
      const ticket = ++generation;
      clear();
      const snapshot = { ...input, values: [...input.values] };
      options.publish({ status: "checking" });
      timer = setTimeout(async () => {
        timer = null;
        try {
          const result = await options.validate(snapshot);
          if (!disposed && ticket === generation) options.publish({ status: "checked", result });
        } catch (error) {
          if (!disposed && ticket === generation) options.publish({ status: "unavailable", error });
        }
      }, options.delay ?? 200);
    },
    dispose() { disposed = true; generation += 1; clear(); },
  };
}
