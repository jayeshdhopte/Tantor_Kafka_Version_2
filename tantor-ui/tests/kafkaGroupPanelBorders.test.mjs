import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import postcss from 'postcss';

const read = path => readFileSync(new URL(path, import.meta.url), 'utf8');

test('group tables have a complete rounded frame without changing cell spacing or scrolling', () => {
  const css = postcss.parse(read('../src/components/KafkaGroupPanel.css'));
  const declarations = selector => {
    const result = {};
    css.walkRules(selector, rule => rule.walkDecls(declaration => { result[declaration.prop] = declaration.value; }));
    return result;
  };
  assert.deepEqual(declarations('.kafka-group-panel-scroll'), {
    'overflow-x': 'auto', border: '1px solid var(--table-heading-divider, #D2D2D7)', 'border-radius': '8px',
  });
  assert.deepEqual(declarations('.kafka-group-panel tbody tr:last-child td'), { 'border-bottom': '0' });
  assert.equal(declarations('.kafka-group-panel th, .kafka-group-panel td').padding, '10px');
  assert.equal(declarations('.kafka-group-panel table')['border-collapse'], 'collapse');
});

test('the framed coordination table remains in Schema Registry and Kafka Connect', () => {
  for (const [page, kind] of [['SchemaRegistry', 'schema'], ['KafkaConnect', 'connect']]) {
    assert.match(read(`../src/pages/${page}.tsx`), new RegExp(`<KafkaGroupPanel[^>]*kind="${kind}"`));
  }
  const component = read('../src/components/KafkaGroupPanel.tsx');
  assert.match(component, /<div className="kafka-group-panel-scroll"><table>/);
});
