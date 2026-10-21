import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import test from 'node:test';
import postcss from 'postcss';
import ts from 'typescript';

const theme = readFileSync(new URL('../src/styles/TableTheme.css', import.meta.url), 'utf8');
const css = postcss.parse(theme);
const rules = [];
css.walkRules(rule => rules.push(rule));
const ruleFor = selector => rules.find(rule => rule.selectors.includes(selector));
const declaration = (rule, property) => rule.nodes.find(node => node.prop === property);

test('theme uses the approved Overview colors and loads at app startup', () => {
  const root = ruleFor(':root');
  assert.equal(declaration(root, '--table-heading-background').value, '#EEF0F3');
  assert.equal(declaration(root, '--table-heading-color').value, '#3E1363');
  const main = readFileSync(new URL('../src/main.tsx', import.meta.url), 'utf8');
  assert.match(main, /import '\.\/styles\/TableTheme\.css'/);
});

test('table containers do not reserve unused scrollbar space and keep scrolling page-owned', () => {
  const rule = ruleFor('body :has(> table, > .figma-table)');
  assert.ok(rule);
  assert.deepEqual(rule.nodes.filter(node => node.type === 'decl').map(node => [node.prop, node.value]),
    [['scrollbar-gutter', 'auto']]);
  for (const container of ['overview-table-wrap', 'cluster-nodes-table-wrap', 'brokers-table-container',
    'topics-table-card', 'consumer-table-scroll', 'ds-table-wrapper', 'ds-kc-list-scroll', 'table-container']) {
    assert.ok(rule.selector.includes(`.${container}`), `Missing table container: ${container}`);
  }
  assert.ok(!rule.selector.includes('job-logs-container'));
  assert.ok(!rule.selector.includes('ds-version-schema-code'));
});

test('native table headers, including portal dialogs, use layered theme overrides', () => {
  const rule = ruleFor('body table > thead > tr > :is(th, td)');
  assert.equal(rule.parent.name, 'layer');
  assert.equal(rule.parent.params, 'tantor-table-headers');
  for (const prop of ['background', 'color', 'font-family', 'font-size', 'font-weight']) {
    assert.equal(declaration(rule, prop).important, true);
  }
  assert.equal(declaration(rule, 'background').value, 'var(--table-heading-background)');
  assert.equal(declaration(ruleFor('body table > thead > tr > :first-child'), 'box-shadow').value,
    'inset 3px 0 0 var(--table-heading-color)');
});

test('all existing native tables have headers covered by the shared selector', () => {
  const src = new URL('../src/', import.meta.url);
  let count = 0;
  for (const file of readdirSync(src, { recursive: true }).filter(file => file.endsWith('.tsx'))) {
    const source = ts.createSourceFile(file, readFileSync(new URL(file.replaceAll('\\', '/'), src), 'utf8'), ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
    const visit = node => {
      if (ts.isJsxElement(node) && node.openingElement.tagName.getText(source) === 'table') {
        count += 1;
        assert.ok(node.children.some(child => ts.isJsxElement(child) && child.openingElement.tagName.getText(source) === 'thead'), `Missing table header: ${file}`);
      }
      ts.forEachChild(node, visit);
    };
    visit(source);
  }
  assert.ok(count > 0);
});

test('grid-based tables share the background and inset accent', () => {
  for (const selector of [
    'body .figma-table-header', 'body .parcel-host-row.header',
    'body .service-selection-table > .table-header', 'body .cc-preview-table > div:first-child',
  ]) {
    assert.ok(rules.some(rule => rule.selectors.includes(selector) && declaration(rule, 'box-shadow')));
  }
});

test('table styling leaves layout, body states, scrolling and controls unchanged', () => {
  css.walkDecls(decl => {
    if (decl.parent.parent?.name !== 'layer') return;
    assert.ok(!['width', 'min-width', 'max-width', 'height', 'min-height', 'max-height', 'display', 'position', 'overflow', 'overflow-x', 'overflow-y', 'table-layout', 'border-collapse', 'border-spacing', 'pointer-events'].includes(decl.prop));
  });
  for (const rule of rules) {
    assert.ok(!rule.selector.includes('tbody'));
    assert.ok(!rule.selector.includes(':disabled'));
    assert.ok(!rule.selector.includes('pre') || rule.selector.includes('preview-table'));
  }
});

test('Monitoring themes section headings without touching charts or metric values', () => {
  assert.ok(ruleFor('body .monitoring-container .section-header-row'));
  assert.ok(ruleFor('body .monitoring-container .monitoring-section-title-custom'));
  assert.ok(!theme.includes('.recharts'));
  assert.ok(!theme.includes('.kpi-card-val'));
});
