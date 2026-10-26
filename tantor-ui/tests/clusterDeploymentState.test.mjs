import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';
import ts from 'typescript';
import { isClusterDeploying, isDeploymentJob } from '../src/utils/clusterDeploymentState.ts';

const read = file => readFileSync(new URL(`../src/${file}`, import.meta.url), 'utf8');
const hook = ts.transpileModule(read('hooks/useActiveClusterJobs.ts'), { compilerOptions: {
  target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS,
} }).outputText;
const hookContext = vm.createContext({ exports: {}, require: name => name === 'react' ? {} : { isDeploymentJob } });
vm.runInContext(hook, hookContext);
const { locksClusterView, clusterJobLabel } = hookContext.exports;
const job = { id: 'job-1', clusterId: 'cluster-1', type: 'DEPLOYMENT', status: 'IN_PROGRESS', progress: '{}' };
const cluster = { id: 'cluster-1', mode: 'kraft', status: 'SUCCESS', runtimeStatusLabel: 'Kafka Check Failed', runtimeHealth: 'DEGRADED' };

test('deployment jobs lock cluster access without changing service-addition and restart labels', () => {
  assert.equal(locksClusterView(job), true);
  assert.equal(clusterJobLabel(job), 'Deployment');
  assert.equal(locksClusterView({ ...job, operation: 'ADD_SERVICE' }), true);
  assert.equal(clusterJobLabel({ ...job, operation: 'ADD_SERVICE' }), 'Adding service');
  assert.equal(clusterJobLabel({ ...job, type: 'ROLLING_RESTART' }), 'Rolling restart');
  assert.equal(isDeploymentJob({ ...job, operation: 'ADD_SERVICE' }), false);
  assert.equal(locksClusterView({ ...job, type: 'MONITORING_ENABLEMENT' }), false);
});

test('active job and backend deployment metadata override prematurely successful cluster status', () => {
  assert.equal(isClusterDeploying(cluster, [job]), true);
  assert.equal(isClusterDeploying({ ...cluster, deploymentInProgress: true }), true);
  for (const status of ['PENDING', 'RUNNING', 'IN_PROGRESS', 'VALIDATING', 'DEPLOYING']) {
    assert.equal(isClusterDeploying({ ...cluster, status }), true);
  }
  assert.equal(isClusterDeploying(cluster, [{ ...job, clusterId: 'different-cluster' }]), false);
  assert.equal(isClusterDeploying({ ...cluster, mode: 'EXTERNAL' }, [job]), false);
  assert.equal(isClusterDeploying(cluster, [{ ...job, operation: 'ADD_SERVICE' }]), false);
});

function pageHarness() {
  const source = read('pages/Clusters.tsx');
  const ast = ts.createSourceFile('Clusters.tsx', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  const names = ['locked', 'isClickable', 'isDeploymentInProgress', 'statusLabel', 'statusClass'];
  const initializers = new Map();
  function visit(node) {
    if (ts.isVariableDeclaration(node) && names.includes(node.name.getText(ast))) initializers.set(node.name.getText(ast), node.initializer.getText(ast));
    ts.forEachChild(node, visit);
  }
  visit(ast);
  const context = vm.createContext({ isClusterDeploying, locksClusterView, activeJobs: [job], clusters: [cluster] });
  for (const name of names) {
    const compiled = ts.transpileModule(`globalThis.${name} = (${initializers.get(name)});`, { compilerOptions: { target: ts.ScriptTarget.ES2022 } }).outputText;
    vm.runInContext(compiled, context);
  }
  return context;
}

test('cluster list prevents navigation and displays Deploying instead of a runtime-check failure', () => {
  const page = pageHarness();
  assert.equal(page.locked(cluster.id), true);
  assert.equal(page.isClickable(cluster), false);
  assert.equal(page.statusLabel(cluster), 'Deploying');
  assert.equal(page.statusClass(cluster), 'deploying');
  const source = read('pages/Clusters.tsx');
  assert.match(source, /locked\(cluster.id\) && <LockKeyhole/);
  assert.match(source, /if \(isClickable\(cluster\)\) window.location.assign/);
  assert.match(source, /navigate\(`\/jobs\/\$\{job.id\}`\)/);
});

test('deployment failure is visible immediately and successful completion restores navigation', () => {
  const page = pageHarness();
  const failed = { ...cluster, deploymentFailed: true, deploymentInProgress: true };
  assert.equal(page.statusLabel(failed), 'Kafka Check Failed');
  assert.equal(page.statusClass(failed), 'failed');
  assert.equal(page.isClickable(failed), false);
  const completed = { ...cluster, deploymentInProgress: false, runtimeStatusLabel: 'Active', runtimeHealth: 'HEALTHY' };
  page.activeJobs = [];
  page.clusters = [completed];
  assert.equal(page.isClickable(completed), true);
  assert.equal(page.locked(cluster.id), false);
  assert.equal(page.statusLabel(completed), 'Active');
});

test('direct cluster URLs block rendering tabs and redirect to deployment progress', () => {
  const source = read('pages/ClusterDetails.tsx');
  assert.match(source, /if \(blockingJob \|\| isClusterDeploying\(cluster, activeJobs\)\)/);
  assert.match(source, /cluster.deploymentJobId/);
  assert.match(source, /navigate\(`\/jobs\/\$\{jobId\}`, \{ replace: true \}\)/);
  assert.ok(source.indexOf('if (blockingJob || isClusterDeploying') < source.indexOf('<Outlet'));
});
