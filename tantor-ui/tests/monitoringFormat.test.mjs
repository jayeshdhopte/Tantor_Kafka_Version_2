import test from 'node:test';
import assert from 'node:assert/strict';
import { formatMonitoringBytes, formatMonitoringNumber } from '../src/utils/monitoringFormat.ts';

test('large monitoring values use grouped decimal notation', () => {
  const formatted = formatMonitoringNumber(1234567890.5, 1);
  assert.match(formatted, /\d/);
  assert.doesNotMatch(formatted, /e[+-]?\d/i);
});

test('small nonzero rates keep useful precision', () => {
  assert.equal(formatMonitoringNumber(0.03, 1), '0.03');
  assert.equal(formatMonitoringNumber(0.000002, 2), '0.000002');
  assert.equal(formatMonitoringBytes(0.03), '0.03 B');
});

test('outgoing byte rates stay readable across small and large values', () => {
  assert.equal(formatMonitoringBytes(0.000002), '0.000002 B');
  const formatted = formatMonitoringBytes(2 ** 40);
  assert.match(formatted, /TB$/);
  assert.doesNotMatch(formatted, /e[+-]?\d/i);
});

test('missing and non-finite samples remain unavailable', () => {
  assert.equal(formatMonitoringNumber(null), 'N/A');
  assert.equal(formatMonitoringBytes(Infinity), 'N/A');
});
