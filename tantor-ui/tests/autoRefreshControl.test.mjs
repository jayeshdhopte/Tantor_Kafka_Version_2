import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import ts from 'typescript';
const read=path=>readFileSync(new URL('../src/'+path,import.meta.url),'utf8');
const compiled=ts.transpileModule(read('hooks/useAutoRefresh.ts'),{compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022,jsx:ts.JsxEmit.ReactJSX}}).outputText;
const flush=async()=>{for(let i=0;i<20;i++)await Promise.resolve();};
function harness(t){
 t.mock.timers.enable({apis:['setTimeout']});
 const slots=[];let cursor=0;
 const document={visibilityState:'visible'};
 const context=vm.createContext({exports:{},document,window:{setTimeout,clearTimeout},require:name=>name==='react'?{
  useRef(value){return slots[cursor++]??={current:value};},
  useEffect(effect,deps){const i=cursor++;const previous=slots[i];if(!previous||deps.some((v,index)=>v!==previous.deps[index])){previous?.cleanup?.();slots[i]={deps,cleanup:effect()};}},
 }: {}});
 vm.runInContext(compiled,context);
 return {document,render(callback,seconds=5,enabled=true,key='cluster:connection'){cursor=0;context.exports.useAutoRefresh(callback,seconds,enabled,key);},unmount(){slots.forEach(slot=>slot?.cleanup?.());}};
}
test('auto refresh is off by default, avoids overlapping slow requests and stops on unmount',async t=>{
 const h=harness(t);let calls=0,finish;
 const slow=()=>{calls++;return new Promise(resolve=>{finish=resolve;});};
 h.render(slow,0);t.mock.timers.tick(10000);await flush();assert.equal(calls,0);
 h.render(slow);t.mock.timers.tick(5000);await flush();assert.equal(calls,1);
 t.mock.timers.tick(60000);await flush();assert.equal(calls,1);
 finish();await flush();t.mock.timers.tick(5000);await flush();assert.equal(calls,2);
 h.unmount();finish();await flush();t.mock.timers.tick(60000);await flush();assert.equal(calls,2);
});
test('hidden tabs and blocked editing pause refresh; interval and connection changes replace old timers',async t=>{
 const h=harness(t);let oldCalls=0,newCalls=0;
 h.render(()=>{oldCalls++;});h.document.visibilityState='hidden';t.mock.timers.tick(5000);await flush();assert.equal(oldCalls,0);
 h.document.visibilityState='visible';h.render(()=>{newCalls++;},10,true,'other:instance');
 t.mock.timers.tick(5000);await flush();assert.equal(newCalls,0);t.mock.timers.tick(5000);await flush();assert.equal(newCalls,1);
 h.render(()=>{newCalls++;},10,false);t.mock.timers.tick(30000);await flush();assert.equal(newCalls,1);h.unmount();
});
test('Consumers omit the auxiliary panel while Schema and Connect retain their coordination groups',()=>{
 assert.ok(!read('pages/Consumers.tsx').includes('KafkaGroupPanel'));
 assert.ok(read('pages/SchemaRegistry.tsx').includes('kind="schema"'));
 assert.ok(read('pages/KafkaConnect.tsx').includes('kind="connect"'));
 for(const page of ['KafkaConnect','SchemaRegistry','Partitions'])assert.ok(read('pages/'+page+'.tsx').includes('<AutoRefreshControl'));
});
