import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import postcss from 'postcss';

const source = readFileSync(new URL('../src/pages/ClusterOverview.tsx', import.meta.url), 'utf8');
const css = postcss.parse(readFileSync(new URL('../src/pages/ClusterOverview.css', import.meta.url), 'utf8'));
const declarations = selector => {
  const result = {};
  css.walkRules(selector, rule => rule.walkDecls(({ prop, value }) => { result[prop] = value; }));
  return result;
};

test('all four identity directory values use the scoped wrapping rule without changing their data', () => {
  assert.equal((source.match(/className="overview-value overview-directory-value"/g) || []).length, 4);
  for (const field of ['installDirectory', 'configDirectory', 'dataDirectory', 'logDirectory']) {
    assert.ok(source.includes(`className="overview-value overview-directory-value">{overview.${field} || '-'}`));
  }
  assert.ok(source.includes('className="overview-value cluster-id-value">{overview.kafkaClusterId'));
  assert.ok(source.includes('className="overview-value">{overview.originType'));
});

test('directory text is width-constrained and can wrap even within an unbroken path component', () => {
  const rule = declarations('.overview-directory-value');
  assert.equal(rule.width, '100%');
  assert.equal(rule['max-width'], '100%');
  assert.equal(rule['min-width'], '0');
  assert.equal(rule['white-space'], 'normal');
  assert.equal(rule['overflow-wrap'], 'anywhere');
  assert.equal(rule.height, undefined);
  assert.equal(rule['max-height'], undefined);
  assert.equal(declarations('.overview-value')['white-space'], 'nowrap');
  const card = declarations('.overview-item');
  assert.equal(card.height, undefined);
  assert.equal(card['max-height'], undefined);
});
