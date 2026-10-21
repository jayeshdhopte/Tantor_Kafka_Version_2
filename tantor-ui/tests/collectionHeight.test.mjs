import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import postcss from 'postcss';

const css = postcss.parse(readFileSync(new URL('../src/index.css', import.meta.url), 'utf8'));
const rules = [];
css.walkRules(rule => rules.push(rule));
const rule = rules.find(item => item.selector.startsWith('body :is(.cluster-details-card,'));

test('collection pages fit their content rather than reserving viewport height', () => {
  assert.ok(rule);
  assert.deepEqual(rule.nodes.filter(node => node.type === 'decl').map(node => [node.prop, node.value]),
    [['min-height', '0']]);
  for (const selector of ['.cluster-details-card', '.clusters-surface', '.clusters-surface .full-empty-card',
    '.hosts-page', '.jobs-list-page', '.audit-page', '.alerts-container', '.artifacts-page',
    '.monitoring-white-box', '.external-wrapper', '.clusters-table-wrap', '.overview-dashboard',
    '.brokers-dashboard', '.data-services-page .ds-fetch-prompt']) {
    assert.ok(rule.selector.includes(selector), `Missing collection surface: ${selector}`);
  }
});

test('content sizing leaves table rows, charts, dialogs and scrolling unchanged', () => {
  for (const selector of ['tbody', ' td', ' th', '.recharts', '.ds-modal', '.topic-modal', 'input', 'button']) {
    assert.ok(!rule.selector.includes(selector), `Unexpected target: ${selector}`);
  }
  assert.ok(!rule.nodes.some(node => ['height', 'max-height', 'overflow', 'display', 'padding'].includes(node.prop)));
});
