// Fixture demonstration of the real React page. Does not claim live Kafka acceptance.
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
(async () => {
 const origin=process.env.ADD_NODE_TEST_URL||'http://127.0.0.1:5193';
 const output=process.env.EXTERNAL_ADD_NODE_DEMO_DIR||path.resolve(__dirname,'../../../ui-verification/external-add-node-phase2-demo');fs.mkdirSync(output,{recursive:true});
 const source=await(await fetch(origin+'/src/pages/ExternalClusterAddNode.tsx')).text();
 const dep=name=>source.match(new RegExp('"([^" ]*/'+name+'\\.js[^" ]*)"'))?.[1];
 const react=dep('react'),router=dep('react-router-dom');assert(react&&router);
 const browser=await chromium.launch({headless:true,...(process.env.CHROME_EXECUTABLE?{executablePath:process.env.CHROME_EXECUTABLE}:{})});
 const context=await browser.newContext({viewport:{width:1440,height:1100}});
 const results=[];let mode='FAIL',posts=0,otherMutations=0,body,preview;
 const check=(code,status,observed)=>({code,name:code==='C15'?'Package SHA-256':'Live readiness observation',status,expected:'Current measured evidence',observed,remedy:status==='PASS'?'':'Resolve the difference and rerun checks.'});
 const newPreview=()=>({id:'preview-fixture',contextHash:'a'.repeat(64),status:mode==='FAIL'?'BLOCKED':'PASS',expiresAt:new Date(Date.now()+(mode==='EXPIRED'?500:120000)).toISOString(),checks:[check('C01','PASS','Existing cluster ID verified'),check('C15',mode==='FAIL'?'FAIL':'PASS',mode==='FAIL'?'Checksum mismatch — TEST DATA':'Approved SHA-256 verified — TEST DATA')],deploymentEnabled:false});
 try {
  await context.route('**/src/main.tsx*',route=>route.fulfill({contentType:'application/javascript',body:`import '/src/index.css';import React from '${react}';import ReactDOM from '${react.replace('/react.js','/react-dom_client.js')}';import{BrowserRouter,Routes,Route}from '${router}';import{AuthContext}from '/src/contexts/authContextDef.ts';import{ExternalClusterAddNode}from '/src/pages/ExternalClusterAddNode.tsx';ReactDOM.createRoot(document.getElementById('root')).render(React.createElement(AuthContext.Provider,{value:{decodedToken:{roles:['admin']}}},React.createElement(BrowserRouter,null,React.createElement(Routes,null,React.createElement(Route,{path:'/external-clusters/:id/add-node',element:React.createElement(ExternalClusterAddNode)})))));`}));
  await context.route('**/api/**',async route=>{
   const request=route.request(),url=new URL(request.url());
   if(request.method()==='POST'&&url.pathname.endsWith('/add-node/readiness')) {
    posts++;body=request.postDataJSON();preview=newPreview();return route.fulfill({json:preview});
   }
   if(url.pathname.endsWith('/add-node/broker-review/preview-fixture'))return route.fulfill({json:{previewId:preview.id,contextHash:preview.contextHash,expiresAt:preview.expiresAt,proposedPlan:{nodeId:2,kafkaClusterId:'existing-kafka-id',kafkaVersion:'4.3.0'},planDigest:'c'.repeat(64),serviceName:'broker.service',configPath:'/opt/tantor/kafka/config/broker.properties',executionEnabled:false,blockers:['External execution release gate is held — TEST DATA']}});
   if(request.method()!=='GET'){otherMutations++;return route.fulfill({status:500,json:{message:'Unexpected mutation'}});}
   if(url.pathname.endsWith('/jobs/active-clusters'))return route.fulfill({json:[]});
   if(url.pathname.endsWith('/add-node/readiness/preview-fixture'))return route.fulfill({json:mode==='STALE'?{...preview,status:'STALE'}:preview});
   if(url.pathname.endsWith('/external-clusters/agents'))return route.fulfill({json:[{id:'candidate',hostname:'Candidate — TEST DATA',ipAddresses:'["192.0.2.10"]',version:'3.4.0-fixture-an.1',fresh:true,status:'ONLINE',lastHeartbeat:new Date().toISOString()}]});
   if(url.pathname.endsWith('/external-clusters'))return route.fulfill({json:[{id:'demo',name:'External Phase 2 — TEST DATA',clusterId:'existing-kafka-id',kafkaVersion:'4.3.0',kafkaMode:'KRaft'}]});
   return route.fulfill({status:500,json:{message:'Unexpected fixture route'}});
  });
  const page=await context.newPage();const errors=[];page.on('pageerror',error=>errors.push(error.message));
  await page.goto(origin+'/external-clusters/demo/add-node');await page.getByLabel('Target external host',{exact:true}).selectOption('candidate');await page.getByRole('button',{name:'Continue to checks',exact:true}).click();
  const next=page.getByRole('button',{name:'Continue to role',exact:true});assert(await next.isDisabled());results.push('Missing evidence blocks progression');
  await page.getByRole('button',{name:'Run readiness checks',exact:true}).click();await page.getByText('Checksum mismatch — TEST DATA',{exact:true}).waitFor();assert(await next.isDisabled());assert.equal(body.agentId,'candidate');assert.equal(body.hostIp,'192.0.2.10');assert.equal(body.deploymentSettings.brokerHeapXmx,'1G');results.push('Real API contract submitted; failed checksum displayed and blocks');
  await page.screenshot({path:path.join(output,'01-failed-checks-test-data.png'),fullPage:true});
  mode='PASS';await page.getByRole('button',{name:'Run readiness checks',exact:true}).click();await page.getByText('Approved SHA-256 verified — TEST DATA',{exact:true}).waitFor();assert(await next.isEnabled());results.push('Current PASS permits broker layout review');
  await page.getByLabel('Broker maximum heap',{exact:true}).fill('2G');assert(await next.isDisabled());await page.getByText('Readiness not run.',{exact:false}).waitFor();results.push('Changing a profile invalidates PASS immediately');
  await page.getByRole('button',{name:'Run readiness checks',exact:true}).click();await page.getByText('Approved SHA-256 verified — TEST DATA',{exact:true}).waitFor();assert.equal(body.deploymentSettings.brokerHeapXmx,'2G');await next.click();
  const radios=page.getByRole('radio');assert(await radios.nth(1).isDisabled());assert(await radios.nth(2).isDisabled());await page.getByRole('button',{name:'Review selected layout',exact:true}).click();assert(await page.getByRole('button',{name:'Deploy broker',exact:true}).isDisabled());results.push('Controller/separate layouts and broker execution stay disabled');
  await page.screenshot({path:path.join(output,'02-review-deployment-disabled-test-data.png'),fullPage:true});
  await page.getByRole('button',{name:'Back',exact:true}).click();await page.getByRole('button',{name:'Back',exact:true}).click();mode='EXPIRED';await page.getByRole('button',{name:'Run readiness checks',exact:true}).click();await page.waitForTimeout(1600);assert(await next.isDisabled());await page.getByText('Readiness: EXPIRED.',{exact:false}).waitFor();results.push('Elapsed expiry blocks without waiting for a server refresh');
  mode='PASS';await page.getByRole('button',{name:'Run readiness checks',exact:true}).click();await page.getByText('Readiness: PASS.',{exact:false}).waitFor();mode='STALE';await page.getByText('Readiness: STALE.',{exact:false}).waitFor({timeout:10000});assert(await next.isDisabled());results.push('Server context invalidation removes PASS');
  await page.getByRole('button',{name:'Refresh inventory',exact:true}).click();assert.equal(await page.getByLabel('Target external host',{exact:true}).inputValue(),'');results.push('Inventory refresh clears candidate and evidence');
  assert.equal(otherMutations,0);assert.equal(errors.length,0,errors.join('\n'));results.push('Only read-only readiness POSTs; no deployment requests or React errors');
  fs.writeFileSync(path.join(output,'acceptance.json'),JSON.stringify({fixtureOnly:true,liveAcceptance:false,passed:results,readinessPosts:posts,otherMutations},null,2));console.log(JSON.stringify({passed:results.length,readinessPosts:posts,otherMutations,output},null,2));
 }finally{await context.close();await browser.close();}
})().catch(error=>{console.error(error);process.exit(1);});
