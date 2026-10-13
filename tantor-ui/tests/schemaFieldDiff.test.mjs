import assert from 'node:assert/strict';
import test from 'node:test';
import { compareSchemaFields } from '../src/utils/schemaFieldDiff.ts';

test('reports an Avro field type change in the older-to-newer direction', () => {
  const older = JSON.stringify({ type: 'record', name: 'Person', fields: [
    { name: 'id', type: 'int' }, { name: 'age', type: 'int' }
  ] });
  const newer = JSON.stringify({ type: 'record', name: 'Person', fields: [
    { name: 'id', type: 'int' }, { name: 'age', type: 'string' }
  ] });
  assert.deepEqual(compareSchemaFields(older, newer, 'AVRO', 'AVRO').changes, [
    { kind: 'type', field: 'age', before: 'int', after: 'string' }
  ]);
});

test('reports added and removed fields without repeating nested children', () => {
  const older = JSON.stringify({ type: 'record', fields: [
    { name: 'old', type: { type: 'record', fields: [{ name: 'child', type: 'int' }] } }
  ] });
  const newer = JSON.stringify({ type: 'record', fields: [
    { name: 'new', type: { type: 'record', fields: [{ name: 'child', type: 'string' }] } }
  ] });
  assert.deepEqual(compareSchemaFields(older, newer, 'AVRO', 'AVRO').changes, [
    { kind: 'removed', field: 'old', before: 'record' },
    { kind: 'added', field: 'new', after: 'record' }
  ]);
});

test('reports nested JSON Schema type and required changes', () => {
  const older = JSON.stringify({ type: 'object', properties: {
    person: { type: 'object', properties: { age: { type: 'integer' } } }
  } });
  const newer = JSON.stringify({ type: 'object', properties: {
    person: { type: 'object', required: ['age'], properties: { age: { type: 'string' } } }
  } });
  assert.deepEqual(compareSchemaFields(older, newer, 'JSON', 'JSON').changes, [
    { kind: 'type', field: 'person.age', before: 'integer', after: 'string' },
    { kind: 'required', field: 'person.age', before: 'optional', after: 'required' }
  ]);
});

test('ignores JSON formatting differences and avoids guessing for Protobuf', () => {
  const schema = { type: 'record', fields: [{ name: 'age', type: 'int' }] };
  const comparison = compareSchemaFields(JSON.stringify(schema), JSON.stringify(schema, null, 2), 'AVRO', 'AVRO');
  assert.equal(comparison.identical, true);
  assert.deepEqual(comparison.changes, []);
  assert.equal(compareSchemaFields('message P { int32 age = 1; }', 'message P { string age = 1; }', 'PROTOBUF', 'PROTOBUF').supported, false);
});
