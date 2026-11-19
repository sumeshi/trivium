import type { ColumnKind } from './types';

/** The inferred source kind is independent of its current display format. */
export function columnKindLabel(kind: ColumnKind | undefined): string {
  switch (kind) {
    case 'string': return 'Text';
    case 'int': return 'Int';
    case 'float': return 'Float';
    case 'datetime': return 'Datetime';
    case 'null': return 'Null';
    default: return 'Unknown';
  }
}
