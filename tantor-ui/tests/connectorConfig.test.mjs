import test from 'node:test';
import assert from 'node:assert/strict';
import { parseConnectorConfig, sameConnectorConfig } from '../src/utils/connectorConfig.ts';

test('accepts valid connector settings and preserves string values', () => {
  assert.deepEqual(parseConnectorConfig('{"connector.class":"FileSource","tasks.max":"2","name":"source"}', 'source'), { 'connector.class': 'FileSource', 'tasks.max': '2', name: 'source' });
});
test('rejects malformed settings, non-string values and connector rename', () => {
  for (const text of ['{', 'null', '[]', '{}', '{"connector.class":"Source","tasks.max":2}', '{"connector.class":"Source","name":"other"}', '{"connector.class":"Source","x":{}}']) {
    assert.throws(() => parseConnectorConfig(text, 'source'));
  }
});
test('compares configuration independently of JSON formatting and key order', () => {
  assert.equal(sameConnectorConfig({ a: '1', b: '2' }, { b: '2', a: '1' }), true);
  assert.equal(sameConnectorConfig({ a: '1' }, { a: '2' }), false);
  assert.equal(sameConnectorConfig({ a: '1' }, { a: '1', b: '2' }), false);
});
