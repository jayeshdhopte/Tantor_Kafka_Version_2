import test from 'node:test';
import assert from 'node:assert/strict';
import {connectorType, connectorCsv} from '../src/utils/kafkaConnect.ts';
const connector={name:'source',class:'FileStreamSource',state:'RUNNING',tasks:1,runningTasks:1,config:{},status:{type:'source'},topics:['orders'],topicsAvailable:true};
test('uses the native role and never guesses source/sink from the class name',()=>{
 assert.equal(connectorType(connector),'SOURCE');
 assert.equal(connectorType({...connector,status:{}}),'UNKNOWN');
});
test('CSV escapes quoted values and protects spreadsheet formulas',()=>{
 const csv=connectorCsv([{...connector,name:'=SUM(A1:A2)',class:'a,"quoted"',topicsAvailable:false}], 'production');
 assert.ok(csv.includes('"\'=SUM(A1:A2)"'));
 assert.ok(csv.includes('"a,""quoted"""'));
 assert.ok(csv.includes('"Unavailable"'));
 assert.ok(csv.includes('"production"'));
});
