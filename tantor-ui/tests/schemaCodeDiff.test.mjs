import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { compareSchemaCode } from '../src/utils/schemaCodeDiff.ts';

test('highlights only inserted lines while retaining both schema definitions', () => {
  const older = 'message Event {\n  string id = 1;\n}';
  const newer = 'message Event {\n  string id = 1;\n  string description = 2;\n}';
  const { before, after } = compareSchemaCode(older, newer);
  assert.equal(before.map(line => line.text).join('\n'), older);
  assert.equal(after.map(line => line.text).join('\n'), newer);
  assert.ok(before.every(line => line.kind === 'unchanged'));
  assert.deepEqual(after.filter(line => line.kind === 'added').map(line => line.text), ['  string description = 2;']);
});

test('marks removals on the older side and replacements on both sides', () => {
  const { before, after } = compareSchemaCode('id: int\nobsolete: string\nend', 'id: string\nend');
  assert.deepEqual(before.filter(line => line.kind === 'removed').map(line => line.text), ['id: int', 'obsolete: string']);
  assert.deepEqual(after.filter(line => line.kind === 'added').map(line => line.text), ['id: string']);
  assert.equal(after.at(-1).kind, 'unchanged');
});

test('identical definitions have no highlights and preserve blank lines', () => {
  const source = '{\n\n  "fields": []\n}';
  const { before, after } = compareSchemaCode(source, source);
  assert.equal(before.map(line => line.text).join('\n'), source);
  assert.deepEqual(before, after);
  assert.ok(after.every(line => line.kind === 'unchanged'));
});

const source = readFileSync(new URL('../src/pages/SchemaRegistry.tsx', import.meta.url), 'utf8');
const openCompareBody = source.match(/const openCompare = \(\) => \{([\s\S]*?)\n  \};/)[1];
const runOpenCompare = (versions) => {
  const result = {};
  Function('comparableVersions', 'setError', 'setCompareVersionA', 'setCompareVersionB', 'setShowCompare', openCompareBody)(
    versions, value => { result.error = value; }, value => { result.a = value; },
    value => { result.b = value; }, value => { result.open = value; },
  );
  return result;
};

test('opening defaults to versions 1 and 2 even when latest is listed first', () => {
  const versions = [{ version: 3 }, { version: 2 }, { version: 1 }];
  assert.deepEqual(runOpenCompare(versions), { error: null, a: 1, b: 2, open: true });
  assert.deepEqual(versions.map(item => item.version), [3, 2, 1]);
});

test('missing early versions use the earliest available pair and one version stays blocked', () => {
  assert.deepEqual(runOpenCompare([{ version: 5 }, { version: 3 }]), { error: null, a: 3, b: 5, open: true });
  assert.equal(runOpenCompare([{ version: 1 }]).open, undefined);
});

test('field changes start collapsed and highlight mapping follows chronological versions', () => {
  const detailsTag = source.match(/<details[^>]*className="ds-compare-summary"[^>]*>/)[0];
  assert.doesNotMatch(detailsTag, /\bopen(?:=|\s|>)/);
  assert.match(source, /<summary className="ds-compare-summary-heading">/);
  assert.match(source, /comparedSchemaA === olderSchema \? comparedCode\?\.before : comparedCode\?\.after/);
  assert.match(source, /comparedSchemaB === olderSchema \? comparedCode\?\.before : comparedCode\?\.after/);
});
