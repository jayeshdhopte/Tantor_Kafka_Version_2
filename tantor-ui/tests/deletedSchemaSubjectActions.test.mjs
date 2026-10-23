import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

test('deleted-only row menu reuses recovery navigation and permanent-delete confirmation', () => {
  const page = readFileSync(new URL('../src/pages/SchemaRegistry.tsx', import.meta.url), 'utf8');
  const menu = readFileSync(new URL('../src/components/DeletedSchemaSubjectActions.tsx', import.meta.url), 'utf8');
  assert.match(page, /item\.deleted \? <DeletedSchemaSubjectActions/);
  assert.match(page, /onRecover=\{\(\) => void openSubject\(item\)\}/);
  assert.match(page, /onDelete=\{\(\) => requestSchemaDelete\(item\.subject, true\)\}/);
  assert.match(page, /requestSchemaDelete\(item\.subject, false\)/);
  assert.equal((menu.match(/role="menuitem"/g) || []).length, 2);
  assert.match(menu, /<RotateCcw size=\{16\} \/> Recover/);
  assert.match(menu, /Permanently delete/);
  assert.doesNotMatch(menu, /Re-register|onReregister/);
  assert.match(menu, /<AnchoredMenu/);
  assert.match(menu, /aria-haspopup="menu"/);
  assert.match(menu, /event\.key === 'Escape'/);
});

test('deleted version menus preserve version-specific recovery and deletion', () => {
  const page = readFileSync(new URL('../src/pages/SchemaRegistry.tsx', import.meta.url), 'utf8');
  assert.match(page, /version\.deleted \? <DeletedSchemaSubjectActions/);
  assert.match(page, /onRecover=\{\(\) => \{ setRecoveryError\(null\); setPendingRecovery\(\{ subject: selected\.subject, version, connectionId: selectedConnectionId \}\); \}\}/);
  assert.match(page, /onDelete=\{\(\) => requestSchemaDelete\(selected\.subject, true, version\.version\)\}/);
  assert.match(page, /requestSchemaDelete\(selected\.subject, false, version\.version\)/);
});
