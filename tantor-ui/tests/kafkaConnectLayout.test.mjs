import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import ts from 'typescript';
import postcss from 'postcss';

const source = readFileSync(new URL('../src/components/KafkaConnectorsTable.tsx', import.meta.url), 'utf8');
const page = readFileSync(new URL('../src/pages/KafkaConnect.tsx', import.meta.url), 'utf8');
const ast = ts.createSourceFile('KafkaConnectorsTable.tsx', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
const css = postcss.parse(readFileSync(new URL('../src/pages/DataServiceTabs.css', import.meta.url), 'utf8'));
const rules = [];
css.walkRules(rule => rules.push(rule));
const declarations = selector => Object.fromEntries(rules.find(rule => rule.selector === selector).nodes
  .filter(node => node.type === 'decl').map(node => [node.prop, node.value]));
const className = node => node.openingElement.attributes.properties
  .find(attribute => ts.isJsxAttribute(attribute) && attribute.name.text === 'className')?.initializer?.text || '';

test('connector filters and framed table are separate siblings', () => {
  let inventory;
  const visit = node => {
    if (ts.isJsxElement(node) && className(node) === 'ds-kc-inventory') inventory = node;
    ts.forEachChild(node, visit);
  };
  visit(ast);
  assert.ok(inventory);
  const children = inventory.children.filter(ts.isJsxElement);
  assert.equal(children.length, 2);
  assert.ok(className(children[0]).includes('ds-kc-connector-filters'));
  assert.ok(className(children[1]).includes('ds-panel ds-kc-panel ds-kc-list-scroll'));
  assert.equal(declarations('.ds-kc-inventory').gap, '16px');
  assert.match(page, /activeTab === 'connectors' \? 'ds-kc-inventory-container' : 'ds-panel ds-kc-panel'/);
});

test('connector filter borders are solid and scoped to the connector toolbar', () => {
  assert.equal(declarations('.ds-kc-connector-filters .ds-subject-filter-button').border, '1px solid #D2D2D7');
  assert.equal(declarations('.ds-kc-connector-filters').padding, '0');
});

test('action buttons have stable dimensions and vertical space inside rows', () => {
  assert.equal(declarations('.ds-kc-expanded-table td:last-child').padding, '12px 16px');
  assert.equal(declarations('.ds-kc-expanded-table .ds-inline-actions')['grid-template-columns'], 'repeat(2, 32px)');
  assert.equal(declarations('.ds-kc-expanded-table .ds-inline-actions').gap, '8px');
  const button = declarations('.ds-kc-expanded-table .ds-inline-actions .ds-button');
  assert.equal(button.height, '32px');
  assert.equal(button.width, '32px');
  assert.match(source, /disabled=\{saving\} onClick=\{\(\) => onAction\(row.name, action\)\}/);
});

test('tab indicators stay inside buttons and away from table borders', () => {
  const indicator = declarations('.ds-kc-tabs button.active::after');
  assert.equal(indicator.left, '12px');
  assert.equal(indicator.right, '12px');
  assert.equal(indicator.bottom, '0');
  assert.equal(indicator.background, '#3E1363');
  assert.equal(declarations('.ds-kc-tabs').margin, '8px 0 16px');
  assert.equal(declarations('.ds-kc-tabs button').margin, '0');
});

test('connector summary polish stays scoped and adapts to narrow viewports', () => {
  assert.equal(declarations('.ds-kc-detail-summary')['border-radius'], '8px');
  assert.equal(declarations('.ds-kc-detail-summary')['border-left'], '3px solid #3E1363');
  assert.equal(declarations('.ds-kc-detail-summary > div')['min-width'], '0');
  assert.equal(declarations('.ds-kc-detail-summary strong')['overflow-wrap'], 'anywhere');
  for (const width of ['800px', '480px']) {
    assert.ok(rules.some(rule => rule.selector === '.ds-kc-detail-summary'
      && rule.parent.type === 'atrule' && rule.parent.params === `(max-width: ${width})`));
  }
  const details = readFileSync(new URL('../src/components/KafkaConnectorDetails.tsx', import.meta.url), 'utf8');
  assert.match(details, /failed > 0 \? ' ds-kc-tasks-failed'/);
  assert.match(details, /<strong>\{running\}<\/strong>/);
  assert.match(details, /<strong>\{failed\}<\/strong>/);
});

test('configuration toolbar is separate from the framed JSON and Properties content', () => {
  assert.equal(declarations('.ds-kc-config-layout').gap, '16px');
  assert.equal(declarations('.ds-kc-config-layout > .ds-kc-config-toolbar').padding, '0');
  assert.equal(declarations('.ds-kc-detail-panel.ds-kc-config-panel').border, '0');
  assert.equal(declarations('.ds-kc-config-content').border, '1px solid var(--border-default)');
  const config = readFileSync(new URL('../src/components/ConnectorConfigPanel.tsx', import.meta.url), 'utf8');
  assert.ok(config.indexOf('className="ds-kc-config-toolbar"') < config.indexOf('className="ds-kc-config-content"'));
  assert.ok(config.indexOf('className="ds-kc-config-content"') < config.indexOf('<JsonEditor'));
  assert.match(config, /hidden=\{view !== 'json'\}/);
  assert.match(config, /view === 'properties' && <table/);
});

test('Kafka Connect connection form separates identity from settings without adding paste mode', () => {
  const pageAst = ts.createSourceFile('KafkaConnect.tsx', page, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  let modal;
  function visit(node) {
    if (ts.isJsxElement(node) && className(node).includes('ds-kc-connection-modal')) modal = node;
    ts.forEachChild(node, visit);
  }
  visit(pageAst);
  assert.ok(modal);
  const form = modal.children.find(child => ts.isJsxElement(child) && className(child) === 'ds-form');
  const cards = form.children.filter(child => ts.isJsxElement(child) && className(child).includes('ds-modal-inner-card'));
  assert.equal(cards.length, 2);
  assert.ok(cards[0].getText(pageAst).includes('Connection Name'));
  assert.ok(cards[0].getText(pageAst).includes('className="ds-kc-connection-status"'));
  assert.ok(!cards[0].getText(pageAst).includes('Host / IP'));
  assert.ok(cards[1].getText(pageAst).includes('Host / IP'));
  assert.ok(cards[1].getText(pageAst).includes('Certificate / Truststore'));
  assert.ok(!modal.getText(pageAst).includes('Paste text'));
  assert.ok(modal.getText(pageAst).includes("accept={certType === 'PEM' ? '.pem,.crt,.cer' : '.p12,.pfx'}"));
  assert.equal(declarations('.ds-kc-connection-modal .ds-kc-connection-settings').gap, '16px');
  assert.equal(declarations('.ds-kc-connection-modal .ds-kc-certificate-grid')['align-items'], 'start');
  const status = declarations('.ds-kc-connection-modal .ds-kc-connection-status');
  assert.equal(status.background, 'var(--bg-surface)');
  assert.equal(status.border, '1px solid #E8E6E1');
});
