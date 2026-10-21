import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import postcss from 'postcss';
import ts from 'typescript';

const source = readFileSync(new URL('../src/pages/SchemaRegistry.tsx', import.meta.url), 'utf8');
const ast = ts.createSourceFile('SchemaRegistry.tsx', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
const rules = [];
postcss.parse(readFileSync(new URL('../src/pages/DataServiceTabs.css', import.meta.url), 'utf8'))
  .walkRules(rule => rules.push(rule));
const declarations = selector => Object.fromEntries(rules.find(rule => rule.selectors.includes(selector)).nodes
  .filter(node => node.type === 'decl').map(node => [node.prop, node.value]));

test('certificate controls share the same height and start alignment', () => {
  const prefix = '.ds-sr-connection-modal .ds-sr-certificate-grid';
  assert.equal(declarations(prefix)['align-items'], 'start');
  assert.equal(declarations(prefix + ' > .ds-field')['align-content'], 'start');
  assert.equal(declarations(prefix + ' .ds-field > label:first-child')['line-height'], '20px');
  for (const control of ['.ds-custom-select-trigger', '.ds-upload-btn-label']) {
    assert.equal(declarations(prefix + ' ' + control).height, '40px');
  }
  assert.equal(declarations(prefix + ' .ds-file-upload-row')['margin-top'], '0');
});

test('paste mode toggle follows the certificate input instead of sharing its label row', () => {
  let field;
  function visit(node) {
    if (ts.isJsxElement(node) && node.openingElement.attributes.properties.some(attribute =>
      ts.isJsxAttribute(attribute) && attribute.name.text === 'className'
      && attribute.initializer?.text === 'ds-field ds-certificate-field')) field = node;
    ts.forEachChild(node, visit);
  }
  visit(ast);
  assert.ok(field);
  const children = field.children.filter(child => child.kind !== ts.SyntaxKind.JsxText);
  assert.equal(children[0].openingElement.tagName.getText(ast), 'label');
  assert.equal(children.at(-1).openingElement.tagName.getText(ast), 'button');
  assert.ok(children.at(-1).getText(ast).includes('ds-sr-certificate-mode'));
  assert.ok(field.getText(ast).includes('accept=".pem,.crt,.cer"'));
  assert.ok(source.includes('accept=".p12,.pfx"'));
});

test('certificate toggle styling is scoped to Schema Registry connection forms', () => {
  const button = declarations('.ds-sr-connection-modal .ds-sr-certificate-mode');
  assert.equal(button.background, '#3E1363');
  assert.equal(button.color, '#FFFFFF');
  assert.equal(button['justify-self'], 'end');
});

test('certificate upload and paste area use scoped brand colors', () => {
  const prefix = '.ds-sr-connection-modal .ds-sr-certificate-grid';
  const upload = declarations(prefix + ' .ds-upload-btn-label:hover');
  assert.equal(upload.background, '#ECE2F4');
  assert.equal(upload['border-color'], '#8E6DAA');
  const textarea = declarations(prefix + ' textarea');
  assert.equal(textarea['background-color'], '#F9F6FC');
  assert.equal(textarea.color, '#332849');
  assert.equal(declarations(prefix + ' textarea:focus')['border-color'], '#3E1363');
});
