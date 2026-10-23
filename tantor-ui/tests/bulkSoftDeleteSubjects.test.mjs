import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { bulkSoftDeleteSubjects } from '../src/utils/bulkSoftDeleteSubjects.ts';

test('bulk deletion uses only soft-delete requests, deduplicates and leaves unselected subjects untouched', async () => {
  const calls = [];
  const result = await bulkSoftDeleteSubjects(['orders/value', 'customers', 'customers'], name => `/subjects/${encodeURIComponent(name)}?connectionId=chosen`, async (url, options) => {
    calls.push([url, options]);
    return Response.json([1]);
  });
  assert.deepEqual(result, { deleted: ['orders/value', 'customers'], failed: [] });
  assert.deepEqual(calls, [
    ['/subjects/orders%2Fvalue?connectionId=chosen', { method: 'DELETE' }],
    ['/subjects/customers?connectionId=chosen', { method: 'DELETE' }],
  ]);
});

test('partial failures report exact outcomes without retrying successful deletions', async () => {
  const result = await bulkSoftDeleteSubjects(['good', 'denied', 'offline'], name => name, async url => {
    if (url === 'offline') throw new Error('Network unavailable');
    return url === 'denied' ? Response.json({ message: 'Access denied' }, { status: 403 }) : Response.json([1]);
  });
  assert.deepEqual(result, { deleted: ['good'], failed: [
    { subject: 'denied', message: 'Access denied' }, { subject: 'offline', message: 'Network unavailable' },
  ] });
});

test('empty selection sends no requests', async () => {
  assert.deepEqual(await bulkSoftDeleteSubjects([], name => name, async () => assert.fail('Unexpected request')), { deleted: [], failed: [] });
});

test('selection is opt-in, context-scoped, and confirmed before deletion', () => {
  const page = readFileSync(new URL('../src/pages/SchemaRegistry.tsx', import.meta.url), 'utf8');
  assert.match(page, /names: \[\], enabled: false/);
  assert.match(page, /subjectSelection.context === selectionContext/);
  assert.match(page, /selectionMode \? <span className="ds-schema-selectable-name"><input type="checkbox"/);
  assert.match(page, /Select all filtered subjects/);
  assert.match(page, /pendingBulkDelete\.context !== selectionContext/);
  assert.match(page, /Cancel selection/);
  assert.match(page, /<BulkSchemaDeleteDialog/);
  assert.match(page, /confirmSchemaRecovery/);
  assert.match(page, /onRecover=\{\(\) => void openSubject\(item\)\}/);
});

test('bulk confirmation stays centered within the viewport despite the global margin reset', () => {
  const css = readFileSync(new URL('../src/pages/DataServiceTabs.css', import.meta.url), 'utf8');
  const dialog = css.match(/\.ds-schema-bulk-dialog\s*\{([^}]+)\}/)?.[1] || '';
  assert.match(dialog, /position:\s*fixed/);
  assert.match(dialog, /inset:\s*0/);
  assert.match(dialog, /margin:\s*auto/);
  assert.match(dialog, /height:\s*fit-content/);
  assert.match(dialog, /overflow-y:\s*auto/);
});
