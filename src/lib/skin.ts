import type { Theme } from "./theme";
import type { FlagSymbol } from "./types";

// フラグ記号（記号の単一ソース）
export const FLAG_HINTS: Record<FlagSymbol, string> = {
  safe: "✓",
  suspicious: "?",
  critical: "!",
};

// 条件色パレット。name は colors.json に保存しない識別子で、
// app.css の CSS 変数 --rule-<name> と対応する。
// 両テーマの hex は src/app.css の --rule-<name> と同期させること。
export interface ConditionColorEntry {
  name: string;
  dark: string;
  light: string;
}

export const CONDITION_COLORS: ReadonlyArray<ConditionColorEntry> = [
  { name: "red", dark: "#4a2b2f", light: "#fbe4e4" },
  { name: "orange", dark: "#4a3323", light: "#fbe9db" },
  { name: "amber", dark: "#463d20", light: "#faf1d5" },
  { name: "green", dark: "#24402f", light: "#e2f3e8" },
  { name: "teal", dark: "#1f3b3b", light: "#dcf2f0" },
  { name: "blue", dark: "#26364f", light: "#e3ecfb" },
  { name: "purple", dark: "#362c4a", light: "#ece6fb" },
  { name: "pink", dark: "#472a3b", light: "#fbe4ef" },
];

// テーマに応じた条件色 hex を返す。未知の name は null。
export function resolveConditionColor(
  name: string,
  current: Theme
): string | null {
  const entry = CONDITION_COLORS.find((item) => item.name === name);
  if (!entry) {
    return null;
  }
  return current === "light" ? entry.light : entry.dark;
}

// 保存済み hex（dark/light のどちらで保存されたか不明）を UI 表示用の
// CSS 変数参照に変換する。パレット外の hex はそのまま返す。
// colors.json には hex のまま保存し、var(...) は保存しない。
export function displayRowColor(color: string, _current: Theme): string {
  const normalized = color.trim().toLowerCase();
  const entry = CONDITION_COLORS.find(
    (item) =>
      item.dark.toLowerCase() === normalized ||
      item.light.toLowerCase() === normalized
  );
  if (!entry) {
    return color;
  }
  return `var(--rule-${entry.name})`;
}
