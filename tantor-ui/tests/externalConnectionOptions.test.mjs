import assert from 'node:assert/strict';
import test from 'node:test';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import vm from 'node:vm';
import React from 'react';
import {renderToStaticMarkup} from 'react-dom/server';
import ts from 'typescript';
import {emptyConnectionOptions, validateBootstrapServers, validateConnectionOptions} from '../src/utils/externalConnectionOptions.ts';

test('multiple bootstrap endpoints normalize and reject incomplete rows and URLs',()=>{
 assert.equal(validateBootstrapServers(' a:9092, b:9093,a:9092,[::1]:9092'),'a:9092,b:9093,[::1]:9092');
 for(const value of ['a:0','a:65536','a:9092,','http://a:9092','user@a:9092','a:9092/path'])assert.throws(()=>validateBootstrapServers(value));
});
test('service names must be unique and masking rules need fields and a topic scope',()=>{
 const options=emptyConnectionOptions();validateConnectionOptions(options);
 options.kafkaConnect=[{connectionName:'one',protocol:'http',host:'connect',port:8083},{connectionName:'one',protocol:'http',host:'other',port:8083}];
 assert.throws(()=>validateConnectionOptions(options),/unique/);options.kafkaConnect=[];
 options.settings.masking=[{type:'MASK',fields:[''],fieldsPattern:'',topicKeysPattern:'',topicValuesPattern:'.*',replacement:'***'}];
 assert.throws(()=>validateConnectionOptions(options),/field name/);
 options.settings.masking[0].fields=['password'];validateConnectionOptions(options);
 options.settings.masking[0].topicValuesPattern='';assert.throws(()=>validateConnectionOptions(options),/keys or values/);
});
const require=createRequire(import.meta.url);
const source=readFileSync(new URL('../src/components/ExternalConnectionOptionsForm.tsx',import.meta.url),'utf8');
const compiled=ts.transpileModule(source,{compilerOptions:{module:ts.ModuleKind.CommonJS,jsx:ts.JsxEmit.ReactJSX,target:ts.ScriptTarget.ES2022}}).outputText;
const context=vm.createContext({exports:{},require:name=>name.endsWith('.css')?{}:require(name)});vm.runInContext(compiled,context);
test('repeatable bootstrap rows are labelled and removable without hiding the last endpoint',()=>{
 const html=renderToStaticMarkup(React.createElement(context.exports.BootstrapServersInput,{value:'first:9092,second:9092',onChange(){}}));
 assert.match(html,/Bootstrap server 1/);assert.match(html,/Bootstrap server 2/);assert.match(html,/Add bootstrap server/);
 const single=renderToStaticMarkup(React.createElement(context.exports.BootstrapServersInput,{value:'first:9092',onChange(){}}));
 assert.match(single,/aria-label="Remove bootstrap server 1" disabled/);
});
test('form renders only the requested integrations and clearly identifies actual exporter transport',()=>{
 const options=emptyConnectionOptions();
 const html=renderToStaticMarkup(React.createElement(context.exports.ExternalConnectionOptionsForm,{value:options,onChange(){}}));
 for(const name of ['Schema Registry','Kafka Connect','Metrics','Masking','Native JMX/RMI is not used'])assert.ok(html.includes(name),name);
 assert.ok(!html.includes('Serdes'));assert.ok(!html.includes('KSQL'));
});

function splitInputs(value, onChange) {
 const tree=context.exports.BootstrapServersInput({value,onChange,separateFields:true});
 const row=React.Children.toArray(tree.props.children).find(child=>child.props.className?.includes('bootstrap-endpoint-row'));
 const fields=React.Children.toArray(row.props.children).find(child=>child.type===React.Fragment);
 return React.Children.toArray(fields.props.children).map(label=>React.Children.toArray(label.props.children).find(child=>child.type==='input'));
}

test('setup-only split fields render hostname and port without changing the default combined field',()=>{
 const html=renderToStaticMarkup(React.createElement(context.exports.BootstrapServersInput,{value:'broker.test:9092',onChange(){},separateFields:true}));
 assert.match(html,/IP \/ Hostname/);assert.match(html,/>Port</);
 assert.match(html,/aria-label="Bootstrap server 1 hostname"/);
 assert.match(html,/aria-label="Bootstrap server 1 port"/);
 assert.match(html,/value="broker.test"/);assert.match(html,/value="9092"/);
 const defaultHtml=renderToStaticMarkup(React.createElement(context.exports.BootstrapServersInput,{value:'broker.test:9092',onChange(){}}));
 assert.match(defaultHtml,/value="broker.test:9092"/);
 assert.doesNotMatch(defaultHtml,/IP \/ Hostname/);
});

test('split-field edits preserve comma-separated endpoints, bracketed IPv6 and incomplete user input',()=>{
 const changes=[];
 const fields=splitInputs('[2001:db8::1]:9092,second:9093',value=>changes.push(value));
 assert.equal(fields[0].props.value,'[2001:db8::1]');assert.equal(fields[1].props.value,'9092');
 fields[0].props.onChange({target:{value:'new.test'}});
 fields[1].props.onChange({target:{value:'9094'}});
 assert.deepEqual(changes,['new.test:9092,second:9093','[2001:db8::1]:9094,second:9093']);
 assert.equal(validateBootstrapServers(changes[1]),changes[1]);
 const pending=splitInputs('',value=>changes.push(value));
 assert.equal(pending[0].props.value,'');assert.equal(pending[1].props.value,'');
 pending[0].props.onChange({target:{value:'pending.test'}});
 assert.equal(changes.at(-1),'pending.test:');
 assert.throws(()=>validateBootstrapServers(changes.at(-1)));
 const ipv6HostOnly=splitInputs('[::1]',()=>{});
 assert.equal(ipv6HostOnly[0].props.value,'[::1]');assert.equal(ipv6HostOnly[1].props.value,'');
});

test('split inputs retain disabled controls and last-row removal protection',()=>{
 const html=renderToStaticMarkup(React.createElement(context.exports.BootstrapServersInput,{value:'one:9092',onChange(){},separateFields:true,disabled:true}));
 assert.match(html,/aria-label="Bootstrap server 1 hostname"[^>]*disabled/);
 assert.match(html,/aria-label="Bootstrap server 1 port"[^>]*disabled/);
 assert.match(html,/aria-label="Remove bootstrap server 1" disabled/);
 const changes=[];
 const tree=context.exports.BootstrapServersInput({value:'one:9092,two:9093',onChange:value=>changes.push(value),separateFields:true});
 const children=React.Children.toArray(tree.props.children);
 const row=children.find(child=>child.props.className?.includes('bootstrap-endpoint-row'));
 React.Children.toArray(row.props.children).find(child=>child.type==='button').props.onClick();
 children.find(child=>child.type==='button').props.onClick();
 assert.deepEqual(changes,['two:9093','one:9092,two:9093,']);
});
