import { diffLines } from 'diff';

interface SchemaCodeLine {
  text: string;
  kind: 'unchanged' | 'added' | 'removed';
}

export function compareSchemaCode(older: string, newer: string) {
  const before: SchemaCodeLine[] = [];
  const after: SchemaCodeLine[] = [];

  for (const part of diffLines(older, newer)) {
    const lines = part.value.replace(/\n$/, '').split('\n');
    for (const text of lines) {
      if (!part.added) before.push({ text, kind: part.removed ? 'removed' : 'unchanged' });
      if (!part.removed) after.push({ text, kind: part.added ? 'added' : 'unchanged' });
    }
  }

  return { before, after };
}
