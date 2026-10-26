import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import postcss from 'postcss';

const read = file => readFileSync(new URL(`../src/${file}`, import.meta.url), 'utf8');
const css = postcss.parse(read('pages/ConfigChange.css'));
function declarations(selector, media) {
  const result = {};
  css.walkRules(rule => {
    if (!rule.selectors.includes(selector)) return;
    const query = rule.parent.type === 'atrule' ? rule.parent.params : undefined;
    if (query !== media) return;
    rule.walkDecls(declaration => { result[declaration.prop] = declaration.value; });
  });
  return result;
}

test('configuration tables have rounded, fully framed scroll containers and continuous header edges', () => {
  const frame = declarations('.cc-page .cc-table-wrap');
  assert.equal(frame['border-radius'], '8px');
  assert.equal(frame['border-color'], '#D2D2D7');
  assert.equal(frame.isolation, 'isolate');
  assert.equal(declarations('.cc-table-wrap').overflow, 'auto');
  assert.equal(declarations('.cc-table-wrap')['max-height'], '540px');
  const tables = declarations('.cc-page :is(.cc-property-table, .cc-history-table, .cc-deletion-table)');
  assert.equal(tables['border-collapse'], 'separate');
  assert.equal(tables['border-spacing'], '0');
  assert.equal(declarations('.cc-property-table th').position, 'sticky');
  assert.equal(declarations('.cc-page .cc-property-table tbody tr:last-child td')['border-bottom'], '0');
  assert.match(read('styles/TableTheme.css'), /box-shadow: inset 3px 0 0 var\(--table-heading-color\)/);
});

test('role and target controls have selected, disabled and keyboard-focus treatments without replacing native inputs', () => {
  assert.match(declarations('.cc-page .cc-role-grid button.active')['border-color'], /--button-primary/);
  assert.equal(declarations('.cc-page .cc-scope label:has(input:checked)')['font-weight'], '600');
  assert.equal(declarations('.cc-page .cc-target-list label:has(input:checked)')['border-color'], '#D8C8E8');
  const controls = declarations(".cc-page input:is([type='radio'], [type='checkbox'])");
  assert.equal(controls.width, '17px');
  assert.match(controls['accent-color'], /--button-primary/);
  assert.equal(controls.appearance, undefined);
  assert.match(declarations('.cc-page :is(button, input, select):focus-visible').outline, /2px solid/);
  assert.equal(declarations('.cc-page :is(button, input):disabled').cursor, 'not-allowed');
  const page = read('pages/ConfigChange.tsx');
  assert.match(page, /type="radio" disabled=\{!!busy\} checked=\{scope === 'ALL'\}/);
  assert.match(page, /type="checkbox" checked=\{scope === 'ALL' \|\| selected.includes\(service.nodeId\)\}/);
  assert.match(page, /aria-label=\{`Select dynamic \$\{name\}`\}/);
});

test('configuration sections use compact unframed bands, with responsive controls and locally scrollable tables', () => {
  const section = declarations('.cc-page .cc-card');
  assert.equal(section.border, '0');
  assert.equal(section['box-shadow'], 'none');
  assert.equal(section['min-width'], '0');
  assert.equal(declarations('.cc-page .cc-section-heading')['margin-bottom'], '0');
  assert.equal(declarations('.cc-page .cc-config-toolbar')['margin-bottom'], '0');
  assert.equal(declarations('.cc-page .cc-config-toolbar input', '(max-width: 720px)')['flex-basis'], '100%');
  assert.equal(declarations('.cc-page .cc-picker', '(max-width: 720px)').width, '100%');
  assert.equal(declarations('.cc-page .cc-property-table')['min-width'], '760px');
  assert.equal(declarations('.cc-page .cc-role-grid button', '(prefers-reduced-motion: reduce)').transition, 'none');
});

test('history retains native disclosure, operation details, job navigation and the existing refresh handler', () => {
  assert.equal(declarations('.cc-page .cc-history-version summary')['line-height'], '24px');
  assert.equal(declarations('.cc-page .cc-history-node-heading')['line-height'], '20px');
  assert.equal(declarations('.cc-page .cc-history-tag')['border-radius'], '4px');
  assert.equal(declarations('.cc-page .cc-history-meta', '(max-width: 720px)')['flex-direction'], 'column');
  const history = read('components/ConfigChangeHistory.tsx');
  assert.match(history, /<details key=\{item.jobId\} open=\{index === 0\}/);
  assert.match(history, /onClick=\{\(\) => void load\(\)\}/);
  assert.match(history, /to=\{`\/jobs\/\$\{item.jobId\}`\}/);
  assert.match(history, /value\(change.newValue, node.mode === 'DELETE'\)/);
});
