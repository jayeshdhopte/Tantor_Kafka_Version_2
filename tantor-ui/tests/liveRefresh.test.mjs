import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import ts from 'typescript';
import * as live from '../src/utils/liveRefresh.ts';

const flush = async () => { for (let i = 0; i < 12; i++) await Promise.resolve(); };
function environment(t, beforeRestore = () => {}) {
  const previous = globalThis.window;
  const storage = new Map();
  t.mock.timers.enable({ apis: ['setInterval'] });
  globalThis.window = { setInterval, clearInterval, localStorage: {
    getItem: key => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, value),
  } };
  t.after(() => { beforeRestore(); globalThis.window = previous; });
  return storage;
}

test('live intervals match Topics and preferences default to Off, rejecting corrupt values', t => {
  const storage = environment(t);
  assert.deepEqual([...live.liveRefreshIntervals], [5, 10, 15, 30, 60]);
  assert.deepEqual(live.readLiveRefreshSettings('a'), { enabled: false, seconds: 15 });
  storage.set('a', '{bad');
  assert.deepEqual(live.readLiveRefreshSettings('a'), { enabled: false, seconds: 15 });
  storage.set('a', JSON.stringify({ enabled: true, seconds: 2 }));
  assert.deepEqual(live.readLiveRefreshSettings('a'), { enabled: false, seconds: 15 });
  storage.set('a', JSON.stringify({ enabled: true, seconds: 10 }));
  assert.deepEqual(live.readLiveRefreshSettings('a'), { enabled: true, seconds: 10 });
});

test('polling waits for the interval, skips overlapping requests and stops completely on cleanup', async t => {
  environment(t);
  let calls = 0, finish;
  let signal;
  const stop = live.startLivePolling(5, incoming => {
    calls++; signal = incoming;
    return new Promise(resolve => { finish = resolve; });
  }, () => true);
  t.mock.timers.tick(4999); assert.equal(calls, 0);
  t.mock.timers.tick(1); assert.equal(calls, 1);
  t.mock.timers.tick(15000); assert.equal(calls, 1);
  finish(); await flush();
  t.mock.timers.tick(5000); assert.equal(calls, 2);
  stop(); assert.equal(signal.aborted, true);
  t.mock.timers.tick(30000); assert.equal(calls, 2);
  finish(); await flush();
});

test('busy or hidden pages skip refresh and failures can retry on the next interval', async t => {
  environment(t);
  let allowed = false, calls = 0;
  const stop = live.startLivePolling(10, async () => { calls++; throw new Error('Unavailable'); }, () => allowed);
  t.mock.timers.tick(10000); assert.equal(calls, 0);
  allowed = true;
  t.mock.timers.tick(10000); await flush(); assert.equal(calls, 1);
  t.mock.timers.tick(10000); await flush(); assert.equal(calls, 2);
  stop();
});

function hookHarness(t) {
  let cleanup;
  const storage = environment(t, () => cleanup?.());
  const code = ts.transpileModule(readFileSync(new URL('../src/hooks/useLiveRefresh.ts', import.meta.url), 'utf8'), {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
  }).outputText;
  let selection = null, dependencies;
  const reference = { current: null };
  const document = { visibilityState: 'visible' };
  const context = vm.createContext({ exports: {}, window, document, require: name => name === 'react' ? {
    useMemo: factory => factory(), useRef: initial => { reference.current ??= initial; return reference; },
    useState: () => [selection, next => { selection = next; }],
    useLayoutEffect: effect => effect(),
    useEffect: (effect, next) => {
      if (!dependencies || next.some((value, index) => value !== dependencies[index])) {
        cleanup?.(); dependencies = next; cleanup = effect();
      }
    },
  } : live });
  vm.runInContext(code, context);
  return { storage, document, render: options => context.exports.useLiveRefresh(options) };
}

test('Pause, interval changes, page switches and connection switches clean up their timers', async t => {
  const h = hookHarness(t);
  let calls = 0;
  let props = { scope: 'cluster:partitions', context: 'page-1', refresh: async () => { calls++; } };
  let control = h.render(props);
  t.mock.timers.tick(15000); assert.equal(calls, 0);
  control.onChange(true, 5); control = h.render(props);
  t.mock.timers.tick(5000); await flush(); assert.equal(calls, 1);
  control.onChange(false, 5); control = h.render(props);
  t.mock.timers.tick(10000); assert.equal(calls, 1);
  control.onChange(true, 10); control = h.render(props);
  t.mock.timers.tick(9999); assert.equal(calls, 1);
  t.mock.timers.tick(1); await flush(); assert.equal(calls, 2);
  h.document.visibilityState = 'hidden';
  t.mock.timers.tick(10000); assert.equal(calls, 2);
  h.document.visibilityState = 'visible';
  h.render({ ...props, paused: true });
  t.mock.timers.tick(10000); assert.equal(calls, 2);
  props = { ...props, context: 'page-2' }; h.render(props);
  t.mock.timers.tick(10000); await flush(); assert.equal(calls, 3);
  props = { ...props, scope: 'other-cluster:partitions' }; control = h.render(props);
  assert.equal(control.enabled, false);
  t.mock.timers.tick(10000); assert.equal(calls, 3);
  assert.ok(h.storage.has('tantor:live:cluster:partitions'));
});

test('changing instance cancels old work and uses the latest callback without leaking requests', async t => {
  const h = hookHarness(t);
  let oldSignal, finish, newCalls = 0;
  const props = { scope: 'cluster:schema', context: 'instance-a', refresh: signal => {
    oldSignal = signal; return new Promise(resolve => { finish = resolve; });
  } };
  h.render(props).onChange(true, 5); h.render(props);
  t.mock.timers.tick(5000); assert.equal(oldSignal.aborted, false);
  h.render({ ...props, context: 'instance-b', refresh: async () => { newCalls++; } });
  assert.equal(oldSignal.aborted, true);
  finish(); await flush();
  t.mock.timers.tick(5000); await flush(); assert.equal(newCalls, 1);
});
