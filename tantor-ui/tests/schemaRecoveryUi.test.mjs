import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import test from 'node:test';
import vm from 'node:vm';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import ts from 'typescript';

const require = createRequire(import.meta.url);
const source = readFileSync(new URL('../src/components/SchemaRecoveryDialog.tsx', import.meta.url), 'utf8');
const page = readFileSync(new URL('../src/pages/SchemaRegistry.tsx', import.meta.url), 'utf8');
const css = readFileSync(new URL('../src/pages/DataServiceTabs.css', import.meta.url), 'utf8');
const compiled = ts.transpileModule(source, {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS, jsx: ts.JsxEmit.ReactJSX },
}).outputText;

function load(react = React, document = { body: {} }, HTMLElement = class {}) {
  const context = vm.createContext({
    exports: {}, document, HTMLElement,
    require: name => name === 'react' ? react : name === 'react-dom' ? { createPortal: child => child } : require(name),
  });
  vm.runInContext(compiled, context);
  return context.exports.SchemaRecoveryDialog;
}

const props = {
  subject: 'orders-value', version: 2, schemaId: 42, schemaType: 'AVRO',
  schema: '{"type":"record","name":"Order","fields":[]}',
  saving: false, error: null, onCancel() {}, onConfirm() {},
};

test('recovery confirmation renders structured original metadata and an escaped schema preview', () => {
  const subject = 'long-subject-'.repeat(50) + '<script>bad()</script>';
  const html = renderToStaticMarkup(React.createElement(load(), { ...props, subject }));
  assert.match(html, /<dialog[^>]+aria-labelledby="schema-recovery-title"/);
  assert.match(html, /aria-describedby="schema-recovery-description"/);
  assert.match(html, /<dt>Original version<\/dt><dd>2<\/dd>/);
  assert.match(html, /<dt>Schema ID<\/dt><dd>42<\/dd>/);
  assert.match(html, /<dt>Type<\/dt><dd>AVRO<\/dd>/);
  assert.match(html, /&lt;script&gt;bad\(\)&lt;\/script&gt;/);
  assert.doesNotMatch(html, /<script>/);
  assert.match(html, /other deleted versions remain unchanged/);
  assert.match(html, /<pre tabindex="0">/);
  assert.match(html, /Recover schema/);
});

test('saving and error states remain clear without claiming successful recovery', () => {
  const busy = renderToStaticMarkup(React.createElement(load(), { ...props, saving: true }));
  assert.equal((busy.match(/disabled=""/g) || []).length, 2);
  assert.match(busy, /aria-busy="true"/);
  assert.match(busy, /ds-schema-recovery-spinner/);
  assert.match(busy, /Recovering\.\.\./);
  const failed = renderToStaticMarkup(React.createElement(load(), { ...props, schemaId: null, error: 'Permission denied' }));
  assert.match(failed, /role="alert">Permission denied/);
  assert.match(failed, /<dd>Unavailable<\/dd>/);
  assert.doesNotMatch(failed, /recovered successfully/);
});

function find(node, predicate) {
  if (!node || typeof node !== 'object') return undefined;
  if (predicate(node)) return node;
  for (const child of React.Children.toArray(node.props?.children)) {
    const match = find(child, predicate);
    if (match) return match;
  }
}

test('native modal opens, restores focus and guards Escape while recovery is busy', () => {
  let opened = 0, closed = 0, focused = 0, canceled = 0, confirmed = 0, cleanup;
  class Element { isConnected = true; focus() { focused += 1; } }
  const element = { showModal() { opened += 1; }, close() { closed += 1; } };
  const react = { ...React, useRef: () => ({ current: element }), useEffect: effect => { cleanup = effect(); } };
  const Dialog = load(react, { body: {}, activeElement: new Element() }, Element);
  const tree = Dialog({ ...props, onCancel: () => { canceled += 1; }, onConfirm: () => { confirmed += 1; } });
  assert.equal(opened, 1);
  const cancel = find(tree, node => node.type === 'button' && node.props.children === 'Cancel');
  assert.equal(cancel.props.autoFocus, true);
  cancel.props.onClick();
  const confirm = find(tree, node => node.type === 'button' && node.props.className.includes('primary'));
  confirm.props.onClick();
  tree.props.onCancel({ preventDefault() {} });
  assert.equal(canceled, 2);
  assert.equal(confirmed, 1);
  cleanup();
  assert.equal(closed, 1);
  assert.equal(focused, 1);
  const busy = Dialog({ ...props, saving: true, onCancel: () => { canceled += 1; } });
  busy.props.onCancel({ preventDefault() {} });
  assert.equal(canceled, 2);
  cleanup();
});

test('UI preserves the existing per-version recovery callback and schema content', () => {
  assert.match(page, /<SchemaRecoveryDialog subject=\{pendingRecovery.subject\}/);
  assert.match(page, /version=\{pendingRecovery.version.version\} schemaId=\{pendingRecovery.version.id\}/);
  assert.match(page, /schema=\{formatSchema\(pendingRecovery.version.schema, pendingRecovery.version.schemaType\)\}/);
  assert.match(page, /saving=\{saving\} error=\{recoveryError\} onCancel=\{\(\) => setPendingRecovery\(null\)\} onConfirm=\{confirmSchemaRecovery\}/);
  assert.doesNotMatch(source, /\bfetch\(|\/api\//);
  assert.match(page, /ds-schema-version-status \$\{version.deleted \? 'deleted' : 'active'\}/);
});

test('recovery styles constrain the viewport, wrap long names and respect reduced motion', () => {
  const dialog = css.match(/\.ds-schema-recovery-dialog\s*\{([^}]+)\}/)?.[1] || '';
  assert.match(dialog, /position:\s*fixed/);
  assert.match(dialog, /inset:\s*0/);
  assert.match(dialog, /margin:\s*auto/);
  assert.match(dialog, /width:\s*min\(640px, calc\(100vw - 32px\)\)/);
  assert.match(dialog, /max-height:\s*calc\(100dvh - 32px\)/);
  assert.match(dialog, /overflow-y:\s*auto/);
  assert.match(css, /\.ds-schema-recovery-facts dd\s*\{[^}]*overflow-wrap:\s*anywhere/);
  assert.match(css, /\.ds-schema-recovery-preview pre\s*\{[^}]*max-height:\s*220px;[^}]*overflow:\s*auto/);
  assert.match(css, /prefers-reduced-motion:\s*reduce\)\s*\{\s*\.ds-schema-recovery-spinner\s*\{\s*animation:\s*none/);
  assert.match(css, /\.ds-schema-recovery-facts\s*\{\s*grid-template-columns:\s*1fr/);
});
