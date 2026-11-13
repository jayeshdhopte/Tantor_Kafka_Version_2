// Run against the Vite dev server with Playwright available to Node.
// This fixture exercises the real wizard; it never contacts a Kafka cluster.
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const assert = require('node:assert/strict');
(async () => {
  const origin = process.env.ADD_NODE_TEST_URL || 'http://127.0.0.1:5193';
  const source = await (await fetch(origin + '/src/pages/ClusterAddNode.tsx')).text();
  const dependency = name => source.match(new RegExp('"([^" ]*/' + name + '\\.js[^" ]*)"'))?.[1];
  const react = dependency('react');
  const router = dependency('react-router-dom');
  assert(react && router, 'Vite dependency URLs must be discovered from the served module');
  const reactDom = react.replace('/react.js', '/react-dom_client.js');
  const browser = await chromium.launch({ headless: true, ...(process.env.CHROME_EXECUTABLE ? { executablePath: process.env.CHROME_EXECUTABLE } : {}) });
  let page;
  try {
    const context = await browser.newContext({ viewport: { width: 1400, height: 1100 } });
    const errors = [];
    const participantIds = ['00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002'];
    const expires = () => new Date(Date.now() + 600000).toISOString();
    let rack = '', inspections = 0, mutations = 0, previews = 0, commonTtl = 600000, commonExpires = expires(), validationStatus = 'BLOCKED', validationRequests = 0;
    const settings = { installPath: '/opt/tantor/kafka', configDirectory: '/opt/tantor/kafka/config', javaHome: '/opt/java-17', runtimeUser: 'kafka', brokerDataPaths: ['/data/broker'], brokerMetadataPath: '', brokerLogPath: '/var/log/broker', brokerHeapXmx: '1G', controllerDataPaths: ['/data/controller'], controllerMetadataPath: '/data/controller-metadata', controllerLogPath: '/var/log/controller', controllerHeapXmx: '1G' };
    const runtimeTasks = new Map();
    const checks = () => [
      ...['COMMON', 'BROKER'].flatMap(phase => Array.from({ length: phase === 'COMMON' ? 21 : 11 }, (_, i) => ({ code: `${phase[0]}${String(i + 1).padStart(2, '0')}`, phase, name: `${phase} check ${i + 1}`, status: 'PASS', expected: 'Required evidence', observed: 'Verified', remedy: '', observedAt: new Date().toISOString(), source: 'FIXTURE' }))),
      { code: 'B_RACK', phase: 'BROKER', name: 'Candidate rack configuration', status: rack ? 'PASS' : 'FAIL', expected: 'Explicit actual rack/zone for this candidate', observed: { existingRacks: ['rack1'], candidateRack: rack }, remedy: rack ? '' : 'Enter the actual candidate rack when the cluster uses racks, then rerun checks.', observedAt: new Date().toISOString(), source: 'FIXTURE' },
    ];
    const common = () => ({ previewId: '00000000-0000-0000-0000-' + String(30 + previews).padStart(12, '0'), clusterId: 'cluster', hostId: 'candidate', contextHash: (rack ? 'b' : 'a').repeat(64), checkedAt: new Date().toISOString(), expiresAt: commonExpires, status: rack ? 'PASS' : 'BLOCKED', checks: checks(), brokerPlan: { brokerRack: rack }, deploymentEnabled: false });
    await context.route('**/src/main.tsx*', route => route.fulfill({ contentType: 'application/javascript', body: `import React from '${react}';import ReactDOM from '${reactDom}';import{BrowserRouter,Routes,Route}from '${router}';import{AuthContext}from '/src/contexts/authContextDef.ts';import{ClusterAddNode}from '/src/pages/ClusterAddNode.tsx';ReactDOM.createRoot(document.getElementById('root')).render(React.createElement(AuthContext.Provider,{value:{decodedToken:{roles:['admin']}}},React.createElement(BrowserRouter,null,React.createElement(Routes,null,React.createElement(Route,{path:'/clusters/:id/add-node',element:React.createElement(ClusterAddNode)})))));` }));
    await context.route('**/api/**', async route => {
      const request = route.request(), path = new URL(request.url()).pathname;
      if (path === '/api/v1/ui/clusters/cluster') return route.fulfill({ json: { id: 'cluster', name: 'Separate layout test fixture', mode: 'KRAFT', status: 'SUCCESS', kafkaVersion: '4.3.0', kafkaClusterId: 'abcdefghijklmnopqrstuv', hosts: [] } });
      if (path.endsWith('/add-node/previews/deployment-settings')) return route.fulfill({ json: settings });
      if (path === '/api/v1/ui/hosts') return route.fulfill({ json: [{ id: 'candidate', hostname: 'candidate-fixture', ipAddress: '192.0.2.10', status: 'ONLINE', available: true, deployable: true }] });
      if (path.endsWith('/jobs/active-clusters')) return route.fulfill({ json: [] });
      if (path.endsWith('/add-node/previews') && request.method() === 'POST') { rack = request.postDataJSON().brokerRack; assert.equal(request.postDataJSON().deploymentSettings.brokerHeapXmx, '512M'); assert.equal(request.postDataJSON().deploymentSettings.configDirectory, '/srv/tantor-test/config'); previews++; commonExpires = new Date(Date.now() + commonTtl).toISOString(); return route.fulfill({ json: common() }); }
      if (/\/add-node\/previews\/[^/]+$/.test(path)) return route.fulfill({ json: common() });
      if (path.endsWith('/add-node/separate-plan')) return route.fulfill({ json: { schemaVersion: 'add-separate-review/v1', clusterId: 'cluster', hostId: 'candidate', kafkaClusterId: 'abcdefghijklmnopqrstuv', kafkaVersion: '4.3.0', quorumMode: 'STATIC', brokerRack: rack, expiresAt: expires(), executionEnabled: false, status: 'PLANNING_ONLY', maintenanceRequired: true, contextHash: 'c'.repeat(64), processes: ['controller', 'broker'].map((role, i) => ({ role, nodeId: i ? 2 : 102, processRoles: role, configurationFile: `${role}.properties`, serviceName: `${role}.service`, listenerPort: i ? 9092 : 9093, requiredDirectories: [`${role}/data`], evidenceStatus: 'NOT_INSPECTED' })), steps: [], checks: [], recoveryRules: [], blockers: [], participants: participantIds.map((serviceId, i) => ({ serviceId, hostId: 'existing', role: i ? 'controller' : 'broker', nodeId: i ? 101 : 1, serviceName: i ? 'controller' : 'broker', configurationPath: `/opt/kafka/${i ? 'controller' : 'broker'}.properties`, evidenceStatus: 'REQUIRED', inspectionAllowed: true, runtimeInspectionAllowed: true })) } });
      if (path.endsWith('/runtime-inspections') && request.method() === 'POST') {
        inspections++; const serviceId = request.postDataJSON().serviceId, i = participantIds.indexOf(serviceId);
        assert(i >= 0); const taskId = '00000000-0000-0000-0000-' + String(100 + inspections).padStart(12, '0');
        const role = i ? 'controller' : 'broker';
        const identity = { schemaVersion: 'add-node-process/v1', hostId: 'existing', nodeId: i ? 101 : 1, role, serviceName: role + '.service', configPath: '/opt/kafka/' + role + '.properties', configSha256: 'a'.repeat(64), unitSha256: 'b'.repeat(64), pid: 100 + i, identityVerified: true, serviceActive: true, versionVerified: false, executionReady: false, observedAt: new Date().toISOString(), blockers: [] };
        const report = { schemaVersion: 'add-node-runtime-inspection/v1', identity, runtime: { schemaVersion: 'add-node-runtime/v1', kafkaVersion: '4.3.0', archiveSha256: 'a'.repeat(64), classpathDigest: 'b'.repeat(64), proofSha256: 'c'.repeat(64), libraryCount: 107 } };
        const value = { taskId, serviceId, hostId: 'existing', status: 'OBSERVED', expiresAt: expires(), executionReady: false, message: 'Approved runtime observed.', evidence: report };
        runtimeTasks.set(taskId, value);
        return route.fulfill({ status: 202, json: { ...value, status: 'RUNNING', evidence: null } });
      }
      if (path.includes('/runtime-inspections/') && request.method() === 'GET') return route.fulfill({ json: runtimeTasks.get(path.split('/').pop()) });
      if (path.endsWith('/expansions/previews') && request.method() === 'POST') {
        const body = request.postDataJSON(); assert.equal(body.layout, 'SEPARATE'); assert.equal(body.maintenanceAcknowledged, true);
        assert.deepEqual([...body.runtimeTaskIds].sort(), [...runtimeTasks.keys()].sort()); validationRequests++;
        return route.fulfill({ status: 202, json: { schemaVersion: 'add-quorum-review/v1', previewId: '00000000-0000-0000-0000-000000000200', clusterId: 'cluster', hostId: 'candidate', contextHash: 'd'.repeat(64), expiresAt: expires(), status: validationStatus, executionEnabled: validationStatus === 'PASS', message: validationStatus === 'PASS' ? 'Both candidate processes validated.' : 'shared filesystem cannot cover all process reservations', proposedPlan: { layout: 'SEPARATE', quorumMode: 'STATIC', kafkaVersion: '4.3.0', kafkaClusterId: 'abcdefghijklmnopqrstuv', processes: ['controller', 'broker'].map((role, i) => ({ role, nodeId: i ? 2 : 102, dataPaths: ['/opt/' + role + '/data'], metadataPath: '', logPath: '/opt/' + role + '/logs', port: i ? 9092 : 9093, heapXmx: '1g' })) } } });
      }
      if (request.method() !== 'GET') mutations++;
      return route.fulfill({ status: 500, json: { message: 'Unexpected test request ' + path } });
    });
    page = await context.newPage(); page.on('pageerror', e => errors.push(e.message));
    await page.clock.install({ time: new Date() });
    await page.goto(origin + '/clusters/cluster/add-node');
    await page.getByLabel('Target host', { exact: true }).selectOption('candidate');
    await page.getByLabel('Broker maximum heap', { exact: true }).fill('512M');
    await page.getByLabel('Configuration directory', { exact: true }).fill('/srv/tantor-test/config');
    await page.getByRole('button', { name: 'Continue to checks', exact: true }).click();
    await page.getByRole('button', { name: 'Run readiness checks', exact: true }).click();
    await page.getByRole('button', { name: 'Continue to role', exact: true }).click();
    await page.getByRole('radio', { name: /^Broker \+ Controller/ }).check();
    assert.equal(await page.getByRole('button', { name: 'Review selected layout', exact: true }).isEnabled(), false);
    await page.getByRole('rowheader', { name: /Candidate rack configuration/ }).waitFor();
    await page.getByText('Enter the actual candidate rack when the cluster uses racks, then rerun checks.').waitFor();
    assert.equal(await page.getByText('Planning preview · execution unavailable', { exact: true }).count(), 0);
    await page.getByRole('radio', { name: /^Controller Participates/ }).check();
    assert.equal(await page.getByRole('button', { name: 'Review selected layout', exact: true }).isEnabled(), true);
    await page.getByRole('radio', { name: /^Broker \+ Controller/ }).check();
    await page.getByRole('button', { name: 'Edit host or broker rack', exact: true }).click();
    await page.getByLabel('Broker rack', { exact: true }).fill('rack1');
    await page.getByRole('button', { name: 'Continue to checks', exact: true }).click();
    await page.getByRole('button', { name: 'Run readiness checks', exact: true }).click();
    await page.getByRole('button', { name: 'Continue to role', exact: true }).click();
    assert.equal(await page.getByRole('radio', { name: /^Broker \+ Controller/ }).isEnabled(), true);
    assert.equal(await page.getByRole('button', { name: 'Review selected layout', exact: true }).isEnabled(), true);
    await page.getByRole('button', { name: 'Review selected layout', exact: true }).click();
    await page.getByRole('heading', { name: 'Broker + Controller · separate-process planning', exact: true }).waitFor();
    await page.getByRole('button', { name: 'Deploy broker and controller', exact: true }).waitFor();
    assert.equal(await page.getByRole('button', { name: 'Validate selected layout', exact: true }).isEnabled(), false);
    assert.equal(await page.getByRole('button', { name: 'Deploy broker and controller', exact: true }).isEnabled(), false);
    assert.equal(inspections, 0); assert.equal(mutations, 0);
    await page.getByRole('button', { name: 'Inspect process and Kafka version', exact: true }).nth(0).click();
    await page.getByText('Approved participant observations: 1 / 2.', { exact: false }).waitFor();
    await page.getByRole('button', { name: 'Inspect process and Kafka version', exact: true }).nth(1).click();
    await page.getByText('Approved participant observations: 2 / 2.', { exact: false }).waitFor();
    assert.equal(await page.getByRole('button', { name: 'Validate selected layout', exact: true }).isEnabled(), false);
    await page.getByRole('checkbox').check();
    assert.equal(await page.getByRole('button', { name: 'Validate selected layout', exact: true }).isEnabled(), true);
    commonTtl = 2000;
    await page.getByRole('button', { name: 'Refresh common checks', exact: true }).click();
    await page.getByText('Common checks are current.', { exact: false }).waitFor();
    await page.getByText('Run fresh common checks before proceeding.', { exact: false }).waitFor();
    await page.getByText('Approved participant observations: 2 / 2.', { exact: false }).waitFor();
    assert.equal(await page.getByRole('button', { name: 'Validate selected layout', exact: true }).isEnabled(), false);
    commonTtl = 600000;
    await page.getByRole('button', { name: 'Refresh common checks', exact: true }).click();
    await page.getByText('Common checks are current.', { exact: false }).waitFor();
    await page.getByText('Approved participant observations: 2 / 2.', { exact: false }).waitFor();
    await page.getByRole('button', { name: 'Refresh separate-process plan', exact: true }).click();
    await page.getByText('Refresh the planning review before validating this layout.', { exact: false }).waitFor({ state: 'hidden' });
    await page.getByText('Approved participant observations: 2 / 2.', { exact: false }).waitFor();
    assert.equal(inspections, 2, 'Refreshing common/unchanged planning context must not repeat approved participant inspections');
    await page.getByRole('checkbox').check();
    await page.getByRole('button', { name: 'Validate selected layout', exact: true }).click();
    await page.getByText('BLOCKED · shared filesystem cannot cover all process reservations', { exact: false }).waitFor();
    assert.equal(await page.getByText('Execution remains held until release acceptance is complete.', { exact: true }).count(), 0);
    assert.equal(await page.getByRole('button', { name: 'Deploy broker and controller', exact: true }).isEnabled(), false);
    validationStatus = 'PASS';
    await page.getByRole('button', { name: 'Validate selected layout', exact: true }).click();
    await page.getByText('PASS · Both candidate processes validated.', { exact: false }).waitFor();
    assert.equal(await page.getByRole('button', { name: 'Deploy broker and controller', exact: true }).isEnabled(), true);
    assert.equal(validationRequests, 2); assert.equal(mutations, 0); assert.deepEqual(errors, []);
    if (process.env.ADD_NODE_TEST_SCREENSHOT) await page.screenshot({ path: process.env.ADD_NODE_TEST_SCREENSHOT, fullPage: true });
    await page.clock.fastForward(601000);
    await page.getByText('Approved participant observations: 0 / 2.', { exact: false }).waitFor();
    assert.equal(await page.getByRole('button', { name: 'Validate selected layout', exact: true }).isEnabled(), false);
    assert.equal(await page.getByRole('button', { name: 'Deploy broker and controller', exact: true }).isEnabled(), false);
    assert.equal(await page.getByText('This observation no longer counts toward final validation.', { exact: true }).count(), 2);
    console.log('PASS: hidden rack blocker displayed, review blocked until fresh valid checks, controller-only unaffected, separate review reachable, final execution gates retained, 2/2 participant approvals preserved across common expiry/refresh and unchanged-plan refresh, blocked capacity and released PASS gates enforced, expired approvals removed and deployment blocked, no deployment mutations.');
  } catch (error) {
    if (page) console.error((await page.locator('body').innerText()).slice(-4500));
    throw error;
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
