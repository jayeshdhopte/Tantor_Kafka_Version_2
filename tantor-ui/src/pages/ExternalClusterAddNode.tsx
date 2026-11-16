import { useEffect, useRef, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { ArrowLeft, ArrowRight, CircleAlert, Info, Loader2, RefreshCw, Server } from 'lucide-react';
import { usePermissions } from '../hooks/usePermissions';
import { useActiveClusterJobs } from '../hooks/useActiveClusterJobs';
import { AddNodeDeploymentSettings, type DeploymentSettings } from '../components/AddNodeDeploymentSettings';
import './ClusterAddNode.css';

type ExternalCluster = {
  id: string; name: string; clusterId?: string; kafkaVersion?: string; kafkaMode?: string;
  environment?: string; bootstrapServers?: string; managementLabel?: string;
};
type ExternalHost = {
  id: string; hostname?: string; agentName?: string; ipAddresses?: string;
  version?: string; canExecuteTasks?: boolean; clusterId?: string | null;
  lastHeartbeat?: string; fresh?: boolean; status?: string;
};
type Inventory = { cluster: ExternalCluster | null; hosts: ExternalHost[] };
const steps = ['Select host', 'Common checks', 'Choose role', 'Review'];
type ReadinessCheck = { code: string; name: string; status: string; expected: unknown; observed: unknown; remedy: string };
type Readiness = { id: string; status: string; expiresAt: string; checks: ReadinessCheck[]; deploymentEnabled: false };
const initialSettings: DeploymentSettings = { installPath: '/opt/tantor/kafka', configDirectory: '/opt/tantor/kafka/config', javaHome: '/usr/lib/jvm/java-21-openjdk', runtimeUser: 'kafka', brokerDataPaths: ['/opt/tantor-broker-storage/data'], brokerMetadataPath: '', brokerLogPath: '/opt/tantor-broker-storage/logs', brokerHeapXmx: '1G', controllerDataPaths: ['/opt/tantor-controller-storage/data'], controllerMetadataPath: '/opt/tantor-controller-storage/metadata', controllerLogPath: '/opt/tantor-controller-storage/logs', controllerHeapXmx: '1G' };
const display = (value: unknown) => typeof value === 'string' ? value : JSON.stringify(value);
const pendingChecks = ['Connectivity and agent capabilities', 'Kafka package and approved checksum', 'Java and JVM profile', 'Listener ports', 'Storage and host resources', 'Live cluster identity and health', 'Conflicting operations and reservations'];

async function read<T>(url: string, signal: AbortSignal): Promise<T> {
  const response = await fetch(url, { signal, cache: 'no-store' });
  const body = await response.json().catch(() => null);
  if (!response.ok) throw new Error(body?.message || body?.error || `Could not load inventory (${response.status}).`);
  return body as T;
}
function address(host: ExternalHost) {
  try { const values: unknown = JSON.parse(host.ipAddresses || '[]'); return Array.isArray(values) ? values.filter(value => typeof value === 'string').join(', ') || 'Not reported' : 'Not reported'; }
  catch { return 'Not reported'; }
}
function unavailable(host: ExternalHost) {
  if (host.clusterId) return 'Already linked to an external cluster';
  if (host.fresh !== true || host.status !== 'ONLINE') return 'External agent heartbeat is stale or offline';
  if (!host.lastHeartbeat || !Number.isFinite(Date.parse(host.lastHeartbeat))) return 'Heartbeat not reported';
  return '';
}

export function ExternalClusterAddNode() {
  const { id } = useParams<{ id: string }>();
  return <ExternalWizard key={id} clusterId={id || ''} />;
}

function ExternalWizard({ clusterId }: { clusterId: string }) {
  const { canManage } = usePermissions();
  const jobs = useActiveClusterJobs();
  const [inventory, setInventory] = useState<Inventory | null>(null);
  const [error, setError] = useState('');
  const [revision, setRevision] = useState(0);
  const [hostId, setHostId] = useState('');
  const [step, setStep] = useState(1);
  const heading = useRef<HTMLHeadingElement>(null);
  const [settings, setSettings] = useState<DeploymentSettings>(initialSettings);
  const [hostIp, setHostIp] = useState('');
  const [parcelDir, setParcelDir] = useState('/opt/tantor/parcels');
  const [artifactId, setArtifactId] = useState('');
  const [brokerPort, setBrokerPort] = useState(9092);
  const [brokerRack, setBrokerRack] = useState('');
  const [readiness, setReadiness] = useState<Readiness | null>(null);
  const [readinessError, setReadinessError] = useState('');
  const [checking, setChecking] = useState(false);
  const [now, setNow] = useState(() => Date.now());
  const requestRef = useRef<AbortController | null>(null);
  const readinessUrl = `/api/v1/ui/external-clusters/${encodeURIComponent(clusterId)}/add-node/readiness`;
  const invalidate = () => { requestRef.current?.abort(); setReadiness(null); setReadinessError(''); setChecking(false); };
  useEffect(() => { const timer = window.setInterval(() => setNow(Date.now()), 1000); return () => window.clearInterval(timer); }, []);
  useEffect(() => () => requestRef.current?.abort(), []);
  const previewId = readiness?.id;
  const previewStatus = readiness?.status;
  useEffect(() => {
    if (!previewId || previewStatus && ['EXPIRED', 'STALE'].includes(previewStatus)) return;
    const abort = new AbortController();
    const timer = window.setInterval(() => {
      void read<Readiness>(`${readinessUrl}/${encodeURIComponent(previewId)}`, abort.signal).then(value => {
        if (!value?.id || !Array.isArray(value.checks) || !Number.isFinite(Date.parse(value.expiresAt)) || value.deploymentEnabled !== false) throw new Error('Invalid readiness refresh. Rerun checks.');
        if (!abort.signal.aborted) setReadiness(value);
      }).catch(failure => { if (!abort.signal.aborted) { setReadiness(null); setReadinessError(failure instanceof Error ? failure.message : 'Readiness refresh failed.'); } });
    }, 5000);
    return () => { window.clearInterval(timer); abort.abort(); };
  }, [previewId, previewStatus, readinessUrl]);
  const runChecks = async () => {
    invalidate(); const abort = new AbortController(); requestRef.current = abort; setChecking(true);
    try {
      const response = await fetch(readinessUrl, { method: 'POST', signal: abort.signal, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ agentId: hostId, hostIp, parcelDir, artifactId, brokerPort, brokerRack, deploymentSettings: settings }) });
      const body = await response.json().catch(() => null);
      if (!response.ok) throw new Error(body?.message || `Readiness could not run (${response.status}).`);
      if (!body?.id || !Array.isArray(body.checks) || !Number.isFinite(Date.parse(body.expiresAt)) || body.deploymentEnabled !== false) throw new Error('The server returned invalid external readiness evidence.');
      if (!abort.signal.aborted) { setReadiness(body); setNow(Date.now()); }
    } catch (failure) { if (!abort.signal.aborted) setReadinessError(failure instanceof Error ? failure.message : 'Readiness could not run.'); }
    finally { if (!abort.signal.aborted) setChecking(false); }
  };
  useEffect(() => {
    if (!canManage || !clusterId) return;
    const abort = new AbortController();
    void (async () => {
      try {
        const [clusters, hosts] = await Promise.all([
          read<ExternalCluster[]>('/api/v1/ui/external-clusters', abort.signal),
          read<ExternalHost[]>('/api/v1/ui/external-clusters/agents', abort.signal),
        ]);
        if (!Array.isArray(clusters) || clusters.some(c => !c?.id || !c.name) || !Array.isArray(hosts) || hosts.some(h => !h?.id)) throw new Error('The server returned invalid external cluster or host inventory.');
        if (!abort.signal.aborted) setInventory({ cluster: clusters.find(c => c.id === clusterId) || null, hosts });
      } catch (failure) { if (!abort.signal.aborted) setError(failure instanceof Error ? failure.message : 'Could not load external inventory.'); }
    })();
    return () => abort.abort();
  }, [clusterId, canManage, revision]);
  useEffect(() => { heading.current?.focus(); }, [step]);
  const cluster = inventory?.cluster;
  const host = inventory?.hosts.find(candidate => candidate.id === hostId);
  const activeJob = jobs.find(job => job.clusterId === clusterId);
  const supportedMode = cluster?.kafkaMode?.toLowerCase() === 'kraft';
  const canNavigate = Boolean(canManage && cluster && supportedMode && host && !unavailable(host) && !activeJob);
  const freshPass = Boolean(readiness?.status === 'PASS' && Date.parse(readiness.expiresAt) > now && readiness.checks.length > 0 && readiness.checks.every(check => check.status === 'PASS') && !activeJob);
  const refresh = () => { invalidate(); setHostIp(''); setInventory(null); setError(''); setHostId(''); setStep(1); setRevision(value => value + 1); };
  return <section className="add-node-page animate-fade-in" aria-labelledby="external-add-node-title">
    <nav className="add-node-breadcrumb" aria-label="Breadcrumb"><Link to="/clusters">Cluster</Link><span>/</span><span>{cluster?.name || 'External cluster'}</span><span>/</span><span aria-current="page">Add Node</span></nav>
    <header className="add-node-header"><div><h1 id="external-add-node-title">Add Node</h1><p>Add Node for an external Kafka cluster.</p></div>
      {canManage && <button className="add-node-button" onClick={refresh} disabled={!inventory && !error}><RefreshCw size={16} /> Refresh inventory</button>}
    </header>
    <div className="add-node-notice"><Info size={18} /><span><strong>External Add Node preview.</strong> Phase 2 runs real read-only readiness checks. Checks do not install packages, write Kafka configuration, format storage or restart services. Broker deployment is disabled until Phase 3.</span></div>
    {!canManage ? <p className="add-node-notice error" role="alert">Administrator access is required to prepare a node addition.</p>
      : !clusterId ? <p role="alert">Select an external cluster first.</p>
        : error ? <p className="add-node-notice error" role="alert">{error} Use Refresh inventory to retry.</p>
          : !inventory ? <p className="add-node-loading" role="status"><Loader2 size={18} className="spin" /> Loading external cluster and enrolled hosts…</p>
            : !cluster ? <div className="add-node-notice warning" role="status"><CircleAlert size={18} /><span>This external cluster is not in the current inventory. <Link to="/external-clusters">Connect an external cluster</Link> before preparing Add Node. No cluster was created.</span></div>
              : <>
                <dl className="add-node-summary"><div><dt>Cluster</dt><dd>{cluster.name}</dd></div><div><dt>Kafka</dt><dd>{cluster.kafkaVersion || 'Not reported'}</dd></div><div><dt>Mode</dt><dd>{cluster.kafkaMode || 'Not reported'}</dd></div><div><dt>Environment</dt><dd>{cluster.environment || 'Not reported'}</dd></div></dl>
                {!supportedMode && <p className="add-node-notice warning" role="alert">External Add Node currently targets KRaft clusters. The mode must be reported before this preview can continue.</p>}
                {activeJob && <p className="add-node-notice warning" role="status">A cluster operation is active. <Link to={`/jobs/${encodeURIComponent(activeJob.id)}`}>View job</Link> before preparing another operation.</p>}
                <ol className="add-node-steps" aria-label="Add Node steps">{steps.map((label, i) => <li key={label} className={step === i + 1 ? 'current' : step > i + 1 ? 'visited' : ''} aria-current={step === i + 1 ? 'step' : undefined}><span>{i + 1}</span>{label}</li>)}</ol>
                <section className="add-node-panel" aria-labelledby="external-add-node-step-heading">
                  <h2 id="external-add-node-step-heading" ref={heading} tabIndex={-1}>{['Select a registered host', 'Common readiness checks', 'Choose the Kafka process layout', 'Review selected layout'][step - 1]}</h2>
                  {step === 1 && <>
                    <p>Cluster identity and Kafka version come from the connected external cluster. Selecting a host does not assign it.</p>
                    <div className="add-node-fields"><label>Cluster<input readOnly value={cluster.name} /></label><label>Kafka version<input readOnly value={cluster.kafkaVersion || 'Not reported'} /></label>
                      <label className="full">Target external host<select aria-label="Target external host" value={hostId} disabled={!supportedMode || Boolean(activeJob)} onChange={event => { invalidate(); setHostId(event.target.value); const selected = inventory.hosts.find(h => h.id === event.target.value); try { setHostIp(JSON.parse(selected?.ipAddresses || '[]')[0] || ''); } catch { setHostIp(''); } }}><option value="">Select an enrolled external host</option>
                        {inventory.hosts.map(candidate => <option key={candidate.id} value={candidate.id} disabled={Boolean(unavailable(candidate))}>{candidate.hostname || candidate.agentName || candidate.id} — {address(candidate)}{unavailable(candidate) ? ` · ${unavailable(candidate)}` : ''}</option>)}
                      </select><small>Only online, unlinked external agents can be selected. Run the checks after reviewing the candidate profile.</small></label>
                    </div>
                    {!inventory.hosts.some(candidate => !unavailable(candidate)) && <p className="add-node-notice warning" role="status">No eligible external host is currently enrolled. Enroll a host or restore its external-agent heartbeat, then refresh inventory.</p>}
                  </>}
                  {step === 2 && <>
                    <p>Selected host: {host?.hostname || hostId}. Review the intended profile before running checks. The approved package must already be cached on this host.</p>
                    <AddNodeDeploymentSettings brokerOnly value={settings} disabled={checking || readiness?.status === 'CHECKING'} onChange={value => { invalidate(); setSettings(value); }} />
                    <div className="add-node-fields">
                      <label>Candidate IP<input aria-label="Candidate IP" value={hostIp} disabled={checking} onChange={event => { invalidate(); setHostIp(event.target.value); }} /></label>
                      <label>Cached parcel directory<input aria-label="Cached parcel directory" value={parcelDir} disabled={checking} onChange={event => { invalidate(); setParcelDir(event.target.value); }} /></label>
                      <label>Approved artifact ID (optional)<input aria-label="Approved artifact ID" value={artifactId} disabled={checking} onChange={event => { invalidate(); setArtifactId(event.target.value); }} /></label>
                      <label>Broker listener port<input type="number" aria-label="Broker listener port" value={brokerPort} disabled={checking} onChange={event => { invalidate(); setBrokerPort(Number(event.target.value)); }} /></label>
                      <label>Broker rack<input aria-label="Broker rack" value={brokerRack} disabled={checking} onChange={event => { invalidate(); setBrokerRack(event.target.value); }} /><small>Use the rack confirmed by the live cluster policy. Leave empty only for a cluster without racks.</small></label>
                    </div>
                    <button className="add-node-button" disabled={!canNavigate || checking || (readiness?.status === 'CHECKING' && Date.parse(readiness.expiresAt) > now)} onClick={() => void runChecks()}>{checking || (readiness?.status === 'CHECKING' && Date.parse(readiness.expiresAt) > now) ? <><Loader2 size={16} className="spin" /> Checking readiness…</> : 'Run readiness checks'}</button>
                    {readinessError && <p role="alert" className="add-node-notice error">{readinessError}</p>}
                    <p role="status">{readiness ? `Readiness: ${Date.parse(readiness.expiresAt) <= now ? 'EXPIRED' : readiness.status}. Evidence expires ${new Date(readiness.expiresAt).toLocaleTimeString()}.` : 'Readiness not run. Required checks must pass before continuing.'}</p>
                    <div className="add-node-table-wrap"><table className="add-node-table"><thead><tr><th>Required check</th><th>Status</th><th>Observation and next step</th></tr></thead><tbody>{readiness ? readiness.checks.map(check => <tr key={check.code}><th scope="row">{check.code} · {check.name}</th><td><span className="add-node-badge">{Date.parse(readiness.expiresAt) <= now ? 'EXPIRED' : check.status}</span></td><td><div>{display(check.observed)}</div>{check.status !== 'PASS' && <small>{check.remedy}</small>}</td></tr>) : pendingChecks.map(check => <tr key={check}><th scope="row">{check}</th><td><span className="add-node-badge">Not run</span></td><td>Run readiness checks to collect evidence.</td></tr>)}</tbody></table></div>
                    <p>No job, host assignment, node ID reservation or deployment authorization is created by these checks.</p>
                  </>}
                  {step === 3 && <fieldset className="add-node-roles"><legend className="add-node-sr-only">Kafka process layout</legend>
                    <label className="add-node-role selected"><span><input type="radio" name="external-add-role" checked readOnly /><Server size={18} /><strong>Broker</strong></span><p>Stores partitions and serves producer and consumer traffic.</p><small>Readiness available · deployment in Phase 3</small></label>
                    <label className="add-node-role"><span><input type="radio" name="external-add-role" disabled /><strong>Controller</strong></span><p>Participates in the metadata quorum.</p><small>Unavailable in this phase</small></label>
                    <label className="add-node-role"><span><input type="radio" name="external-add-role" disabled /><strong>Broker + Controller</strong></span><p>Two separate processes with distinct identities and storage.</p><small>Unavailable in this phase</small></label>
                  </fieldset>}
                  {step === 4 && <><dl className="add-node-review"><dt>Cluster</dt><dd>{cluster.name}</dd><dt>Kafka cluster ID</dt><dd><code>{cluster.clusterId || 'Not reported'}</code></dd><dt>Kafka version</dt><dd>{cluster.kafkaVersion || 'Not reported'}</dd><dt>Target host</dt><dd>{host?.hostname || hostId} · {host ? address(host) : 'Not selected'}</dd><dt>External agent</dt><dd>{host?.version || 'Not reported'}</dd><dt>Process role</dt><dd>broker</dd><dt>Broker node ID</dt><dd>Not allocated</dd><dt>Readiness</dt><dd>{freshPass ? 'PASS — current read-only evidence' : 'Fresh checks required'}</dd><dt>Deployment</dt><dd>Disabled — external broker execution is not available yet.</dd></dl><p>Reviewing this selection creates no job, host reservation, service configuration or Kafka membership.</p><button className="add-node-button primary" disabled>Deploy broker</button></>}
                  {!freshPass && step > 2 && <p className="add-node-notice warning" role="alert">Readiness is no longer current. Return to Common checks and rerun before reviewing this layout.</p>}
                  {!canNavigate && step > 1 && <p className="add-node-notice warning" role="alert">This selection is no longer available. Return to host selection or refresh inventory.</p>}
                  <footer className="add-node-footer">{step === 1 ? <Link className="add-node-button" to={`/clusters/${encodeURIComponent(clusterId)}/nodes`}><ArrowLeft size={16} /> Cancel</Link> : <button className="add-node-button" onClick={() => setStep(value => value - 1)}><ArrowLeft size={16} /> Back</button>}
                    {step < 4 && <button className="add-node-button primary" disabled={!canNavigate || (step > 1 && !freshPass)} onClick={() => setStep(value => value + 1)}>{['Continue to checks', 'Continue to role', 'Review selected layout'][step - 1]}<ArrowRight size={16} /></button>}
                  </footer>
                </section>
              </>}
  </section>;
}
