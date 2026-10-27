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
 for(const value of ['a:0','a:65536','a:9092,','http://a:9092','user@a:9092','a:9092/path','999.1.1.1:9092','bad..host:9092','-host:9092','[abc]:9092'])assert.throws(()=>validateBootstrapServers(value));
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

test('bootstrap host and port edits preserve other endpoints and serialize IPv6 correctly',()=>{
 let value='first:9092,[::1]:9093';
 const render=()=>context.exports.BootstrapServersInput({value,onChange(next){value=next;}});
 const inputs=tree=>{
  const found=[];
  const walk=node=>{if(!node||typeof node!=='object')return;if(node.type==='input')found.push(node);React.Children.forEach(node.props?.children,walk);};
  walk(tree);return found;
 };
 let fields=inputs(render());
 assert.equal(fields.length,4);assert.equal(fields[0].props.value,'first');assert.equal(fields[1].props.value,'9092');
 assert.equal(fields[2].props.value,'::1');assert.equal(fields[3].props.type,'number');
 fields[0].props.onChange({target:{value:'broker.example.com'}});
 assert.equal(value,'broker.example.com:9092,[::1]:9093');
 fields=inputs(render());fields[1].props.onChange({target:{value:'9094'}});
 assert.equal(value,'broker.example.com:9094,[::1]:9093');
 fields=inputs(render());fields[2].props.onChange({target:{value:'2001:db8::2'}});
 assert.equal(validateBootstrapServers(value),'broker.example.com:9094,[2001:db8::2]:9093');
 fields=inputs(render());fields[3].props.onChange({target:{value:''}});assert.throws(()=>validateBootstrapServers(value));
 const html=renderToStaticMarkup(React.createElement(context.exports.BootstrapServersInput,{value:'first:9092',onChange(){}}));
 assert.ok(html.includes('IP / Hostname'));assert.ok(html.includes('Bootstrap server 1 port'));
 assert.ok(html.includes('min="1" max="65535"'));
});
