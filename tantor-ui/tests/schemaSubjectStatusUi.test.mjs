import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import test from 'node:test';
import vm from 'node:vm';
import { renderToStaticMarkup } from 'react-dom/server';
import ts from 'typescript';

const require = createRequire(import.meta.url);
const page = readFileSync(new URL('../src/pages/SchemaRegistry.tsx', import.meta.url), 'utf8');
const ast = ts.createSourceFile('SchemaRegistry.tsx', page, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
let table, note;
function visit(node) {
  if (ts.isJsxElement(node)) {
    const tag = node.openingElement.getText(ast);
    if (tag.includes('className="ds-table ds-sr-subjects-table"')) table = node.getText(ast);
  }
  if (ts.isJsxExpression(node) && node.expression?.getText(ast).startsWith('selected.deleted && canManage &&')) note = node.expression.getText(ast);
  ts.forEachChild(node, visit);
}
visit(ast);
assert.ok(table);
assert.ok(note);

function render(source, overrides = {}) {
  const defaults = {
    canManage: true, selectionMode: false, loading: false, summary: {}, saving: false,
    visibleSubjects: [], selectedSubjectNames: [], allSubjectsSelected: false, filtersActive: false,
    selected: { deleted: false }, appliedGlobalCompatibility: 'BACKWARD',
    subjectCompatibilityLevel: item => item.compatibility || 'BACKWARD',
    DeletedSchemaSubjectActions: () => null, Trash2: () => null, Info: () => null,
  };
  const compiled = ts.transpileModule(`exports.render = () => (${source});`, { compilerOptions: {
    target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX,
  } }).outputText;
  const context = vm.createContext({ exports: {}, require, ...defaults, ...overrides });
  vm.runInContext(compiled, context);
  return renderToStaticMarkup(context.exports.render());
}
const subject = { subject: 'orders-value', type: 'VALUE', version: 2, id: 3, schemaType: 'AVRO', compatibility: 'BACKWARD' };

test('subject rows show Active and Soft-deleted in a dedicated Status column with version badge styling', () => {
  for (const deleted of [false, true]) {
    const html = render(table, { visibleSubjects: [{ ...subject, deleted }] });
    assert.match(html, /<th>Status<\/th><th>Actions<\/th>/);
    assert.match(html, new RegExp(`<td><span class="ds-schema-version-status ${deleted ? 'deleted' : 'active'}">${deleted ? 'Soft-deleted' : 'Active'}</span></td>`));
    assert.equal((html.match(/<th>/g) || []).length, 8);
    assert.equal((html.match(/<td[ >]/g) || []).length, 8);
    assert.doesNotMatch(html, /ds-subject-deleted-tag/);
  }
  assert.match(render(table, { visibleSubjects: [subject] }), />Active<\/span>/);
});

test('status remains visible for read-only users and during opt-in subject selection', () => {
  const readOnly = render(table, { canManage: false, visibleSubjects: [subject] });
  assert.match(readOnly, /<th>Status<\/th>/);
  assert.doesNotMatch(readOnly, /<th>Actions<\/th>/);
  assert.equal((readOnly.match(/<td[ >]/g) || []).length, 7);
  const selecting = render(table, { selectionMode: true, visibleSubjects: [subject] });
  assert.match(selecting, /aria-label="Select orders-value"/);
  assert.match(selecting, />Active<\/span>/);
});

test('empty and loading rows span all columns for administrators and read-only users', () => {
  for (const canManage of [false, true]) {
    for (const loading of [false, true]) {
      const html = render(table, { canManage, loading, summary: null });
      assert.match(html, new RegExp(`colSpan="${canManage ? 8 : 7}"`, 'i'));
      assert.match(html, loading ? /Loading schemas/ : /No schemas found/);
    }
  }
});

test('version selection guidance appears only when a deleted subject can be recovered', () => {
  assert.match(render(note, { selected: { deleted: true } }), /role="note"/);
  assert.match(render(note, { selected: { deleted: true } }), /Choose the version whose schema you want to re-register\./);
  assert.equal(render(note, { selected: { deleted: false } }), '');
  assert.equal(render(note, { selected: { deleted: true }, canManage: false }), '');
  assert.match(page, /onRecover=\{\(\) => void openSubject\(item\)\}/);
  assert.match(page, /setPendingRecovery\(\{ subject: selected.subject, version, connectionId: selectedConnectionId \}\)/);
});
