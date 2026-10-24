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
