import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { filterSchemaVersions } from '../src/utils/schemaSubjectFilters.ts';

const rows = [
  { version: 3, id: 13, deleted: false, schema: 'latest' },
  { version: 2, id: 12, deleted: true, schema: 'deleted' },
  { version: 1, id: 11, schema: 'original' },
];

test('64: checked shows only soft-deleted versions in a mixed history', () => {
  assert.deepEqual(filterSchemaVersions(rows, true), [rows[1]]);
});

test('64: unchecked shows active versions, including older responses without a deleted flag', () => {
  assert.deepEqual(filterSchemaVersions(rows, false), [rows[0], rows[2]]);
});

test('64: empty, all-active and all-deleted histories never leak the opposite state', () => {
  for (const checked of [true, false]) assert.deepEqual(filterSchemaVersions([], checked), []);
  assert.deepEqual(filterSchemaVersions([{ version: 1, deleted: false }], true), []);
  assert.deepEqual(filterSchemaVersions([{ version: 1, deleted: true }], false), []);
  assert.equal(filterSchemaVersions([{ version: 1, deleted: true }, { version: 2, deleted: true }], true).length, 2);
});

test('64: toggling does not mutate, reorder or lose any schema history', () => {
  const snapshot = structuredClone(rows);
  for (let i = 0; i < 3; i++) {
    assert.deepEqual(filterSchemaVersions(rows, true).map(row => row.version), [2]);
    assert.deepEqual(filterSchemaVersions(rows, false).map(row => row.version), [3, 1]);
  }
  assert.deepEqual(rows, snapshot);
  assert.equal(filterSchemaVersions(rows, true)[0], rows[1]);
});

test('64: filtering reflects server histories after deletion, recovery and permanent deletion', () => {
  const active = [{ version: 1, deleted: false }, { version: 2, deleted: false }, { version: 3, deleted: false }];
  const afterDeletion = active.map(row => ({ ...row, deleted: row.version === 2 }));
  assert.deepEqual(filterSchemaVersions(afterDeletion, true).map(row => row.version), [2]);
  assert.deepEqual(filterSchemaVersions(afterDeletion, false).map(row => row.version), [1, 3]);
  const afterRecovery = afterDeletion.map(row => ({ ...row, deleted: false }));
  assert.deepEqual(filterSchemaVersions(afterRecovery, true), []);
  assert.deepEqual(filterSchemaVersions(afterRecovery, false).map(row => row.version), [1, 2, 3]);
  const afterSubjectDeletion = active.map(row => ({ ...row, deleted: true }));
  assert.deepEqual(filterSchemaVersions(afterSubjectDeletion, false), []);
  const afterOneRecovery = afterSubjectDeletion.map(row => ({ ...row, deleted: row.version !== 1 }));
  assert.deepEqual(filterSchemaVersions(afterOneRecovery, true).map(row => row.version), [2, 3]);
  const afterPermanentDeletion = afterOneRecovery.filter(row => row.version !== 2);
  assert.deepEqual(filterSchemaVersions(afterPermanentDeletion, true).map(row => row.version), [3]);
});

test('64: Versions table uses exclusive filtering and still requests complete history', () => {
  const page = readFileSync(new URL('../src/pages/SchemaRegistry.tsx', import.meta.url), 'utf8');
  assert.match(page, /const visibleVersions = filterSchemaVersions\(details\?\.versions \|\| \[\], showDeletedVersions\)/);
  assert.match(page, /visibleVersions\.map\(version =>/);
  assert.match(page, /Show only soft-deleted versions/);
  assert.match(page, /includeDeleted=true/);
  assert.match(page, /No soft-deleted versions found/);
  assert.match(page, /No active versions found/);
});
