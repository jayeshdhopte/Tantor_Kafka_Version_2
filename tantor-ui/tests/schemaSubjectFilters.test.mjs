import assert from 'node:assert/strict';
import test from 'node:test';
import { filterSchemaSubjects, subjectCompatibilityLevel } from '../src/utils/schemaSubjectFilters.ts';
const subjects = [
  { subject: 'Orders-value', id: 12, schemaType: 'AVRO', compatibility: 'BACKWARD' },
  { subject: 'orders-key', id: 23, schemaType: 'PROTOBUF', compatibility: 'NONE' },
  { subject: 'payments-value', id: 34, schemaType: 'JSON', compatibility: 'DEFAULT' },
  { subject: 'archived-value', id: null, schemaType: 'AVRO' }
];
const names = rows => rows.map(row => row.subject);
test('searches names case-insensitively and schema IDs with trimmed text', () => {
  assert.deepEqual(names(filterSchemaSubjects(subjects, ' ORDERS ', [], [])), ['Orders-value', 'orders-key']);
  assert.deepEqual(names(filterSchemaSubjects(subjects, '23', [], [])), ['orders-key']);
  assert.deepEqual(filterSchemaSubjects(subjects, 'null', [], []), []);
});
test('combines selections within each filter and intersects search, type and compatibility', () => {
  assert.deepEqual(names(filterSchemaSubjects(subjects, '', ['AVRO', 'JSON'], ['BACKWARD', 'DEFAULT'])), ['Orders-value', 'payments-value']);
  assert.deepEqual(names(filterSchemaSubjects(subjects, 'orders', ['AVRO', 'PROTOBUF'], ['NONE'])), ['orders-key']);
  assert.equal(filterSchemaSubjects(subjects, '', [], []).length, 4);
});
test('keeps inheritance separate from explicit overrides and unknown metadata', () => {
  assert.deepEqual(names(filterSchemaSubjects(subjects, '', [], ['DEFAULT'])), ['payments-value']);
  assert.deepEqual(names(filterSchemaSubjects(subjects, '', [], ['UNKNOWN'])), ['archived-value']);
  assert.equal(subjectCompatibilityLevel(subjects[3]), 'UNKNOWN');
});
test('does not mutate rows and returns an empty result for conflicting filters', () => {
  const snapshot = structuredClone(subjects);
  assert.deepEqual(filterSchemaSubjects(subjects, 'orders', ['JSON'], ['NONE']), []);
  assert.deepEqual(subjects, snapshot);
});
