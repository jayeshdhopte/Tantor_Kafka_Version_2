// Phase 1 navigation regression, adapted to the Phase 2 readiness gate. Test fixtures only.
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const assert = require('node:assert/strict');
const path = require('node:path');
const fs = require('node:fs');
(async () => {
  const origin = process.env.ADD_NODE_TEST_URL || 'http://127.0.0.1:5193';
  const output = process.env.EXTERNAL_ADD_NODE_DEMO_DIR || path.resolve(__dirname, '../../..', 'ui-verification/external-add-node-phase1-demo');
  fs.mkdirSync(output, { recursive: true });
  const source = await (await fetch(origin + '/src/pages/ExternalClusterAddNode.tsx')).text();
  const dependency = name => source.match(new RegExp('"([^" ]*/' + name + '\\.js[^" ]*)"'))?.[1];
  const react = dependency('react'), router = dependency('react-router-dom');
  assert(react && router); const reactDom = react.replace('/react.js', '/react-dom_client.js');
  const browser = await chromium.launch({ headless: true, ...(process.env.CHROME_EXECUTABLE ? { executablePath: process.env.CHROME_EXECUTABLE } : {}) });
  const context = await browser.newContext({ viewport: { width: 1400, height: 1100 }, recordVideo: { dir: output, size: { width: 1400, height: 1100 } } });
  let page;
  try {
    let clusters = [{ id: 'external-demo', name: 'External demo fixture — TEST DATA', clusterId: 'abcdefghijklmnopqrstuv', kafkaVersion: '4.3.0', kafkaMode: 'KRaft', environment: 'TEST', bootstrapServers: '192.0.2.20:9092' }];
    let hosts = [
      { id: 'candidate', hostname: 'candidate-fixture', ipAddresses: '["192.0.2.10"]', version: '3.4.0-fixture', fresh: true, status: 'ONLINE', lastHeartbeat: new Date().toISOString(), clusterId: null },
      { id: 'stale', hostname: 'stale-fixture', fresh: false, status: 'STALE', clusterId: null },
      { id: 'assigned', hostname: 'assigned-fixture', fresh: true, status: 'ONLINE', clusterId: 'other-cluster', lastHeartbeat: new Date().toISOString() },
    ];
    let failure = false, mutations = 0, inventoryReads = 0, readinessPosts = 0;
    const readiness = { id: 'navigation-fixture', contextHash: 'a'.repeat(64), status: 'PASS', expiresAt: new Date(Date.now() + 120000).toISOString(), checks: [{ code: 'C01', name: 'Fixture readiness', status: 'PASS', observed: 'TEST DATA' }], deploymentEnabled: false };
    const errors = [];
    await context.route('**/src/main.tsx*', route => route.fulfill({ contentType: 'application/javascript', body: `import '/src/index.css';import React from '${react}';import ReactDOM from '${reactDom}';import{BrowserRouter,Routes,Route}from '${router}';import{AuthContext}from '/src/contexts/authContextDef.ts';import{ExternalClusterAddNode}from '/src/pages/ExternalClusterAddNode.tsx';import{ClusterNodes}from '/src/pages/ClusterNodes.tsx';ReactDOM.createRoot(document.getElementById('root')).render(React.createElement(AuthContext.Provider,{value:{decodedToken:{roles:[sessionStorage.getItem('fixtureRole')||'admin']}}},React.createElement(BrowserRouter,null,React.createElement(Routes,null,React.createElement(Route,{path:'/external-clusters/:id/add-node',element:React.createElement(ExternalClusterAddNode)}),React.createElement(Route,{path:'/clusters/:id/nodes',element:React.createElement(ClusterNodes)})))));` }));
    await context.route('**/api/**', route => {
      const request = route.request(), pathname = new URL(request.url()).pathname;
      if (pathname.endsWith('/add-node/broker-review/navigation-fixture')) return route.fulfill({ json: { previewId: readiness.id, contextHash: readiness.contextHash, expiresAt: readiness.expiresAt, proposedPlan: { nodeId: 2, kafkaClusterId: 'abcdefghijklmnopqrstuv', kafkaVersion: '4.3.0' }, planDigest: 'c'.repeat(64), serviceName: 'broker.service', configPath: '/opt/tantor/kafka/config/broker.properties', executionEnabled: false, blockers: ['External execution release gate is held — TEST DATA'] } });
      if (request.method() === 'POST' && pathname.endsWith('/add-node/readiness')) { readinessPosts++; return route.fulfill({ json: readiness }); }
      if (pathname.endsWith('/add-node/readiness/navigation-fixture')) return route.fulfill({ json: readiness });
      if (request.method() !== 'GET') { mutations++; return route.fulfill({ status: 500, json: { message: 'Mutation forbidden in Phase 1' } }); }
      if (pathname.endsWith('/jobs/active-clusters')) return route.fulfill({ json: [] });
      if (pathname.endsWith('/external-demo/nodes')) return route.fulfill({ json: { cluster: { clusterUuid: 'external-demo', kafkaClusterId: 'abcdefghijklmnopqrstuv', type: 'EXTERNAL', mode: 'KRAFT' }, nodes: [] } });
      if (pathname === '/api/v1/ui/external-clusters') { inventoryReads++; return route.fulfill(failure ? { status: 503, json: { message: 'Fixture inventory unavailable' } } : { json: clusters }); }
      if (pathname === '/api/v1/ui/external-clusters/agents') { inventoryReads++; return route.fulfill({ json: hosts }); }
      return route.fulfill({ status: 500, json: { message: 'Unexpected GET ' + pathname } });
    });
    page = await context.newPage(); page.on('pageerror', error => errors.push(error.message));
    const capture = async name => { await page.evaluate(() => window.scrollTo(0, 0)); await page.waitForTimeout(800); await page.screenshot({ path: path.join(output, name), fullPage: true }); };
    await page.goto(origin + '/clusters/external-demo/nodes');
    await page.getByRole('button', { name: 'Add Node', exact: true }).click();
    assert(page.url().endsWith('/external-clusters/external-demo/add-node'));
    await page.getByRole('heading', { name: 'Select a registered host', exact: true }).waitFor();
    assert.equal(await page.getByRole('button', { name: 'Continue to checks', exact: true }).isEnabled(), false);
    assert.equal(await page.locator('option[value="stale"]').evaluate(option => option.disabled), true);
    assert.equal(await page.locator('option[value="assigned"]').evaluate(option => option.disabled), true);
    await page.getByLabel('Target external host', { exact: true }).selectOption('candidate');
    await capture('01-select-host-test-data.png');
    await page.getByRole('button', { name: 'Continue to checks', exact: true }).click();
    assert.equal(await page.getByText('Not run', { exact: true }).count(), 7);
    await capture('02-checks-not-run-test-data.png');
    assert.equal(await page.getByRole('button', { name: 'Continue to role', exact: true }).isDisabled(), true);
    await page.getByRole('button', { name: 'Run readiness checks', exact: true }).click();
    await page.getByText('Readiness: PASS.', { exact: false }).waitFor();
    await page.getByRole('button', { name: 'Continue to role', exact: true }).click();
    const radios = page.getByRole('radio'); assert.equal(await radios.count(), 3);
    assert.equal(await radios.nth(0).isChecked(), true);
    assert.equal(await radios.nth(1).isDisabled(), false); assert.equal(await radios.nth(2).isDisabled(), true);
    await capture('03-broker-only-test-data.png');
    await page.getByRole('button', { name: 'Review selected layout', exact: true }).click();
    await page.getByRole('heading', { name: 'Review selected layout', exact: true }).waitFor();
    assert.equal(await page.getByRole('button', { name: 'Deploy broker', exact: true }).isDisabled(), true);
    await page.getByText('2 · proposed; reserved on submission', { exact: true }).waitFor();
    await page.getByText('PASS — current read-only evidence', { exact: true }).waitFor();
    await capture('04-review-deploy-disabled-test-data.png');
    await page.getByRole('button', { name: 'Back', exact: true }).click();
    await page.getByRole('heading', { name: 'Choose the Kafka process layout', exact: true }).waitFor();
    hosts = [];
    await page.getByRole('button', { name: 'Refresh inventory', exact: true }).click();
    await page.getByText('No eligible external host is currently enrolled.', { exact: false }).waitFor();
    assert.equal(await page.getByLabel('Target external host', { exact: true }).inputValue(), '');
    assert.equal(await page.getByRole('button', { name: 'Continue to checks', exact: true }).isDisabled(), true);
    clusters = []; await page.getByRole('button', { name: 'Refresh inventory', exact: true }).click();
    await page.getByText('This external cluster is not in the current inventory.', { exact: false }).waitFor();
    await capture('05-empty-inventory.png');
    failure = true; await page.getByRole('button', { name: 'Refresh inventory', exact: true }).click();
    await page.getByRole('alert').filter({ hasText: 'Fixture inventory unavailable' }).waitFor();
    failure = false; await page.getByRole('button', { name: 'Refresh inventory', exact: true }).click();
    await page.getByText('This external cluster is not in the current inventory.', { exact: false }).waitFor();
    const before = inventoryReads;
    await page.evaluate(() => sessionStorage.setItem('fixtureRole', 'viewer')); await page.reload();
    await page.getByRole('alert').filter({ hasText: 'Administrator access is required' }).waitFor();
    assert.equal(inventoryReads, before); assert.equal(mutations, 0); assert.deepEqual(errors, []);
    await page.close(); const video = page.video(); await context.close();
    if (video) await video.saveAs(path.join(output, 'external-phase1-fixture-demo.webm'));
    const result = { status: 'PASS', data: 'EXPLICIT_TEST_FIXTURES', liveAcceptance: 'PENDING_NO_EXTERNAL_CLUSTER', mutations, readinessPosts, checks: ['External Nodes entry point', 'Four-screen navigation', 'Live-API-shaped inventory binding', 'Stale/assigned hosts blocked', 'Broker default; dynamic controller review available; separate-process remains disabled', 'Readiness required before role review', 'Deployment disabled', 'Refresh invalidation', 'Empty inventory', 'API error/retry', 'Viewer denied'], output };
    fs.writeFileSync(path.join(output, 'acceptance-fixture-results.json'), JSON.stringify(result, null, 2));
    console.log(JSON.stringify(result));
  } catch (error) { if (page && !page.isClosed()) console.error(await page.locator('body').innerText()); throw error; }
  finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
