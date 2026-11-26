import type { ImportOptions, ImportReport } from './types';

/** A new selection must never inherit a previous file's recovery choices. */
export const defaultImportOptions = (): ImportOptions => ({ encoding: 'auto', hasHeaders: true, recover: false });

export const importNeedsAttention = (report: ImportReport | null | undefined): boolean => Boolean(report && (
  report.skippedRecords || report.repairedRecords || report.generatedColumns || report.unreadableTail
));

export function importReportSummary(report: ImportReport): string {
  const parts: string[] = [];
  if (report.skippedRecords) parts.push(`${report.skippedRecords.toLocaleString()} skipped`);
  if (report.repairedRecords) parts.push(`${report.repairedRecords.toLocaleString()} repaired`);
  if (report.generatedColumns) parts.push(`${report.generatedColumns.toLocaleString()} ${report.generatedColumns === 1 ? 'column' : 'columns'} named`);
  if (report.unreadableTail) parts.push('unreadable remainder');
  return parts.join(' · ') || `${report.importedRecords.toLocaleString()} ${report.importedRecords === 1 ? 'row' : 'rows'} imported`;
}
