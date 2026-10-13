import assert from 'node:assert/strict';
import test from 'node:test';
import { schemaSubmissionError } from '../src/utils/schemaSubmissionError.ts';

test('shows the validation reason and field supplied by the server', () => {
  assert.equal(schemaSubmissionError({
    message: 'The native REST API returned HTTP 409.',
    validationReason: 'Field type changed from int to string',
    affectedField: 'age'
  }, 'Failed to update schema.'), 'Field type changed from int to string Affected field: age.');
});

test('keeps a useful fallback when validation details are unavailable', () => {
  assert.equal(schemaSubmissionError({ message: 'The native REST API returned HTTP 409.' },
    'Failed to update schema.'), 'The native REST API returned HTTP 409.');
});
