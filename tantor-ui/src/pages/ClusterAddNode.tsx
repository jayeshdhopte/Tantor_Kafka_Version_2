import { useEffect, useRef, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { ArrowLeft, ArrowRight, CircleAlert, Info, Loader2, RefreshCw, Server, ShieldCheck } from 'lucide-react';
import { usePermissions } from '../hooks/usePermissions';
import { useActiveClusterJobs } from '../hooks/useActiveClusterJobs';
import { useAddNodeReadiness, type ReadinessCheck } from '../hooks/useAddNodeReadiness';
import './ClusterAddNode.css';
import { BrokerAdditionPlanReview } from '../components/BrokerAdditionPlanReview';
import { ControllerAdditionPlanReview } from '../components/ControllerAdditionPlanReview';
import { SeparateProcessAdditionPlanReview } from '../components/SeparateProcessAdditionPlanReview';

type Host = {
  id: string;
  hostname?: string;
  ipAddresses?: string;
  ipAddress?: string;
  status?: string;
  available?: boolean;
  availabilityReason?: string;
  agentType?: string;
  deployable?: boolean;
  clusterId?: string;
  clusterName?: string;
};

type Cluster = {
  id: string;
  name: string;
  mode: string;
  status: string;
  kafkaVersion?: string;
  kafkaClusterId?: string;
  environment?: string;
  bootstrapServers?: string;
  hosts?: { hostId?: string }[];
  config?: Record<string, unknown>;
};

type Inventory = { cluster: Cluster; hosts: Host[] };
type Step = 1 | 2 | 3 | 4;

const steps = ['Select host', 'Common checks', 'Choose role', 'Review'];
const readinessChecks = [
  ['C09–C10', 'Agent and host identity', 'Intended machine, live reachability and supported task capabilities'],
  ['C14–C15', 'Kafka package', 'Matching version, host availability and approved checksum'],
  ['C16–C17', 'Java and runtime', 'Service-user Java compatibility, OS and service manager'],
  ['C18–C19', 'Host resources', 'Available capacity, persistent mounts and permissions'],
  ['C11–C13', 'Existing service and storage', 'No conflicting deployment or unexplained Kafka data'],
  ['C20', 'Connectivity', 'DNS, broker routes and controller endpoints'],
  ['C01–C08', 'Cluster readiness', 'Live identity, topology, feature state and partition/quorum health'],
  ['C21', 'Other operations', 'Cluster and host reservations'],
];

function hostAddress(host: Host) {
  if (host.ipAddress) return host.ipAddress;
  if (!host.ipAddresses) return 'Address not reported';
  try {
    const addresses: unknown = JSON.parse(host.ipAddresses);
    if (Array.isArray(addresses)) return addresses.filter(value => typeof value === 'string').join(', ') || 'Address not reported';
  } catch { /* Older agents may report an address as plain text. */ }
  return host.ipAddresses;
}

function unavailableReason(host: Host, cluster: Cluster) {
  if (host.clusterId || cluster.hosts?.some(member => member.hostId === host.id)) {
    return host.clusterName ? `Assigned to ${host.clusterName}` : 'Already assigned to a cluster';
  }
  if (host.agentType === 'KAFKA_DISCOVERY' || host.deployable === false) return 'A deployment agent is required';
  if (host.deployable !== true) return 'Deployment eligibility not reported';
  if (host.available !== true) return host.availabilityReason || `Unavailable (${host.status || 'status unknown'})`;
  if (!['AVAILABLE', 'ONLINE'].includes(host.status?.toUpperCase() || '')) return 'Agent availability not confirmed';
  return '';
}

async function readJson<T>(path: string, signal: AbortSignal): Promise<T> {
  const response = await fetch(path, { signal });
  const body = await response.json().catch(() => null);
  if (!response.ok) throw new Error(body?.message || body?.error || `Could not load inventory (${response.status}).`);
  if (body === null) throw new Error('The server returned an empty inventory response.');
  return body as T;
}

export function ClusterAddNode() {
  const { id } = useParams<{ id: string }>();
  return <AddNodeWizard key={id} clusterId={id || ''} />;
}

function AddNodeWizard({ clusterId }: { clusterId: string }) {
  const { canManage } = usePermissions();
  const activeJobs = useActiveClusterJobs();
  const [inventory, setInventory] = useState<Inventory | null>(null);
  const [error, setError] = useState('');
  const [revision, setRevision] = useState(0);
  const [hostId, setHostId] = useState('');
  const [brokerRack, setBrokerRack] = useState('');
  const [step, setStep] = useState<Step>(1);
  const [role, setRole] = useState<'broker' | 'controller' | 'separate'>('broker');
  const readiness = useAddNodeReadiness(clusterId, hostId, revision, brokerRack);
  const heading = useRef<HTMLHeadingElement>(null);

  useEffect(() => {
    if (!canManage || !clusterId) return;
    const abort = new AbortController();
    const load = async () => {
      try {
        const [cluster, hosts] = await Promise.all([
          readJson<Cluster>(`/api/v1/ui/clusters/${encodeURIComponent(clusterId)}`, abort.signal),
          readJson<Host[]>('/api/v1/ui/hosts', abort.signal),
        ]);
        if (cluster.id !== clusterId || !cluster.name || !cluster.mode || !cluster.status
          || !Array.isArray(hosts) || hosts.some(host => !host || typeof host.id !== 'string')) {
          throw new Error('The server returned an invalid cluster or host inventory.');
        }
        if (!abort.signal.aborted) setInventory({ cluster, hosts });
      } catch (failure) {
        if (!abort.signal.aborted) setError(failure instanceof Error ? failure.message : 'Could not load inventory.');
      }
    };
    void load();
    return () => abort.abort();
  }, [clusterId, canManage, revision]);

  useEffect(() => { heading.current?.focus(); }, [step]);

  const refreshInventory = () => {
    setInventory(null);
    setError('');
    setHostId('');
    setRole('broker');
    setStep(1);
    setRevision(value => value + 1);
  };

  const cluster = inventory?.cluster;
  const selectedHost = inventory?.hosts.find(host => host.id === hostId);
  const blockingJob = activeJobs.find(job => job.clusterId === clusterId);
  const clusterBlocker = cluster && cluster.mode.toLowerCase() !== 'kraft'
    ? 'This Add Node flow supports managed KRaft clusters only. External and ZooKeeper clusters are unavailable.'
    : cluster && cluster.status.toUpperCase() !== 'SUCCESS'
      ? `The cluster is not available for node addition (status: ${cluster.status}).`
      : '';
  const canPreview = Boolean(canManage && cluster && selectedHost && !unavailableReason(selectedHost, cluster) && !clusterBlocker && !blockingJob);
  const reported = (value: unknown) => (typeof value === 'string' && value.trim()) || (typeof value === 'number' && Number.isFinite(value)) ? String(value) : 'Not reported';
  const next = () => {
    if (!canPreview || (step === 2 && !readiness.commonPassed) || (step === 3 && role !== 'controller' && !readiness.brokerPassed)) return;
    if (step < 4) setStep((step + 1) as Step);
  };
  const evidence = (value: unknown) => value === '' || value === null || value === undefined ? 'Not reported' : typeof value === 'string' ? value : JSON.stringify(value) ?? 'Not reported';
  const checkTable = (checks: ReadinessCheck[]) => <div className="add-node-table-wrap"><table className="add-node-table">
    <thead><tr><th scope="col">Check</th><th scope="col">Expected / observed</th><th scope="col">Status</th></tr></thead>
    <tbody>{checks.map(check => <tr key={check.code}><th scope="row">{check.name}<small>{check.code} · {check.source}</small></th><td>
      <small>Expected</small><div>{evidence(check.expected)}</div><small>Observed</small><div>{evidence(check.observed)}</div>
      {check.remedy && check.status !== 'PASS' && <p className="add-node-remedy">{check.remedy}</p>}
      <small>{new Date(check.observedAt).toLocaleString()}</small>
    </td><td><span className={`add-node-badge ${check.status.toLowerCase()}`}>{check.status === 'PASS' ? 'Passed' : check.status === 'FAIL' ? 'Failed' : check.status === 'UNKNOWN' ? 'Unknown' : 'Blocked'}</span></td></tr>)}</tbody>
  </table></div>;

  return <section className="add-node-page animate-fade-in" aria-labelledby="add-node-title">
    <nav className="add-node-breadcrumb" aria-label="Breadcrumb">
      <Link to="/clusters">Cluster</Link><span>/</span>
      <Link to={`/clusters/${encodeURIComponent(clusterId)}/nodes`}>{cluster?.name || 'Cluster nodes'}</Link><span>/</span>
      <span aria-current="page">Add Node</span>
    </nav>
    <header className="add-node-header">
      <div><h1 id="add-node-title">Add Node</h1><p>Add a broker, controller, or separate broker and controller processes to an existing managed Kafka cluster.</p></div>
      {canManage && <button className="add-node-button" onClick={refreshInventory} disabled={!inventory && !error}><RefreshCw size={16} /> Refresh inventory</button>}
    </header>
    <div className="add-node-notice"><Info size={18} /><span><strong>Add Node readiness.</strong> Checks observe the host and live Kafka cluster. Choose the process layout after common checks pass. Final validation runs before deployment changes the candidate.</span></div>

    {!canManage ? <div className="add-node-notice error" role="alert"><CircleAlert size={18} /><span>Administrator access is required to prepare a node addition.</span></div>
      : !clusterId ? <div className="add-node-notice error" role="alert">Select a cluster from the cluster inventory first.</div>
        : error ? <div className="add-node-notice error" role="alert"><CircleAlert size={18} /><span>{error} Use Refresh inventory to try again.</span></div>
          : !inventory || !cluster ? <div className="add-node-loading" role="status"><Loader2 size={20} className="spin" /> Loading cluster and registered hosts…</div>
            : <>
              <dl className="add-node-summary">
                <div><dt>Cluster</dt><dd>{cluster.name}</dd></div>
                <div><dt>Kafka</dt><dd>{reported(cluster.kafkaVersion)}</dd></div>
                <div><dt>Mode</dt><dd>{cluster.mode.toLowerCase() === 'kraft' ? 'KRaft' : cluster.mode}</dd></div>
                <div><dt>Environment</dt><dd>{reported(cluster.environment)}</dd></div>
              </dl>
              {clusterBlocker && <div className="add-node-notice warning" role="alert"><CircleAlert size={18} /><span>{clusterBlocker}</span></div>}
              {blockingJob && <div className="add-node-notice warning" role="status"><CircleAlert size={18} /><span>A cluster operation is active. <Link to={`/jobs/${blockingJob.id}`}>View job</Link> before preparing another operation.</span></div>}

              <ol className="add-node-steps" aria-label="Add Node steps">
                {steps.map((label, index) => <li key={label} className={index + 1 === step ? 'current' : index + 1 < step ? 'visited' : ''} aria-current={index + 1 === step ? 'step' : undefined}>
                  <span>{index + 1}</span>{label}
                </li>)}
              </ol>

              <section className="add-node-panel" aria-labelledby="add-node-step-heading">
                <h2 id="add-node-step-heading" ref={heading} tabIndex={-1}>{['Select a registered host', 'Common readiness checks', 'Choose the Kafka process layout', 'Review selected layout'][step - 1]}</h2>
                {step === 1 && <>
                  <p>Cluster settings are inherited. Selecting a host does not assign it to this cluster.</p>
                  <div className="add-node-fields">
                    <label>Cluster<input readOnly value={cluster.name} /></label>
                    <label>Kafka version<input readOnly value={reported(cluster.kafkaVersion)} /></label>
                    <label className="full">Target host<select aria-label="Target host" aria-describedby="add-node-host-help" value={hostId} disabled={Boolean(clusterBlocker || blockingJob)} onChange={event => { setHostId(event.target.value); setBrokerRack(''); setRole('broker'); }}>
                      <option value="">Select a registered host</option>
                      {inventory.hosts.map(host => {
                        const reason = unavailableReason(host, cluster);
                        return <option key={host.id} value={host.id} disabled={Boolean(reason)}>{host.hostname || host.id} — {hostAddress(host)}{reason ? ` · ${reason}` : ''}</option>;
                      })}
                    </select><small id="add-node-host-help">Only available hosts with a deployment agent can be selected. Run live readiness checks after selection.</small></label>
                    <label className="full">Broker rack<input aria-label="Broker rack" value={brokerRack} maxLength={128} placeholder="For example, rack1 or us-east-1d" disabled={Boolean(clusterBlocker || blockingJob)} onChange={event => setBrokerRack(event.target.value.trim())} /><small>Enter this host's actual rack or availability zone. Required for a rack-aware cluster; leave blank when racks are not configured. Changing it requires fresh checks.</small></label>
                  </div>
                  {!inventory.hosts.some(host => !unavailableReason(host, cluster)) && <div className="add-node-notice warning">No eligible hosts are currently reported. Enroll or release a host in <Link to="/hosts">Hosts</Link>, then refresh inventory.</div>}
                  <details className="add-node-identity"><summary>Inherited Kafka identity</summary><code>{reported(cluster.kafkaClusterId)}</code><p>This is the Kafka cluster ID. Node IDs have not been allocated.</p></details>
                </>}

                {step === 2 && <>
                  <p>{selectedHost?.hostname || hostId} · Read-only backend and agent observations. Failed, unknown or missing checks block continuation.</p>
                  {readiness.error && <div className="add-node-notice error" role="alert">{readiness.error} Rerun checks after resolving the issue.</div>}
                  {readiness.preview && <p role="status">Preview: {readiness.expired ? 'Expired' : readiness.preview.status}. Valid until {new Date(readiness.preview.expiresAt).toLocaleTimeString()}. Results are invalidated by changes to the host or cluster context.</p>}
                  {readiness.preview && <details className="add-node-identity"><summary>Inherited broker inspection inputs</summary><dl className="add-node-review">
                    <dt>Runtime user</dt><dd>{reported(readiness.preview.brokerPlan.runtimeUser)}</dd>
                    <dt>Data paths</dt><dd>{evidence(readiness.preview.brokerPlan.dataPaths)}</dd>
                    <dt>Metadata path</dt><dd>{readiness.preview.brokerPlan.metadataPath ? reported(readiness.preview.brokerPlan.metadataPath) : 'Kafka default: first data path'}</dd>
                    <dt>Application logs</dt><dd>{reported(readiness.preview.brokerPlan.logPath)}</dd>
                    <dt>Quorum mode</dt><dd>{reported(readiness.preview.brokerPlan.quorumMode)}</dd>
                  </dl></details>}
                  {readiness.preview ? checkTable(readiness.preview.checks.filter(check => check.phase === 'COMMON')) : <>
                  <div className="add-node-table-wrap"><table className="add-node-table">
                    <thead><tr><th scope="col">Check</th><th scope="col">Required observation</th><th scope="col">Status</th></tr></thead>
                    <tbody>{readinessChecks.map(([code, name, description]) => <tr key={code}><th scope="row">{name}<small>{code}</small></th><td>{description}</td><td><span className="add-node-badge">Not run</span></td></tr>)}</tbody>
                  </table></div>
                  </>}
                  {readiness.running && <div className="add-node-loading" role="status"><Loader2 size={18} className="spin" /> Waiting for the enrolled agent and live checks…</div>}
                  {(readiness.expired || readiness.preview?.status === 'STALE') && <div className="add-node-notice warning">Readiness evidence is no longer current. Run fresh checks.</div>}
                  <button className="add-node-button" disabled={!canPreview || readiness.running} onClick={() => void readiness.run()}><ShieldCheck size={16} /> {readiness.preview || readiness.error ? 'Run checks again' : 'Run readiness checks'}</button>
                </>}

                {step === 3 && <>
                  <p>Select the process layout for {selectedHost?.hostname || hostId}. Final review checks the selected layout and shows release availability before deployment.</p>
                  <fieldset className="add-node-roles"><legend className="add-node-sr-only">Kafka process layout</legend>
                    <label className={`add-node-role ${role === 'broker' ? 'selected' : ''}`}><span><input type="radio" name="add-node-role" value="broker" checked={role === 'broker'} onChange={() => setRole('broker')} /><Server size={18} /><strong>Broker</strong></span><p>Stores partitions and serves producer and consumer traffic.</p><small>Broker-only execution</small></label>
                    <label className={`add-node-role ${role === 'controller' ? 'selected' : ''}`}><span><input type="radio" name="add-node-role" value="controller" checked={role === 'controller'} onChange={() => setRole('controller')} /><strong>Controller</strong></span><p>Participates in the metadata quorum.</p><small>Runtime and quorum validation required</small></label>
                    <label className={`add-node-role ${role === 'separate' ? 'selected' : ''}`}><span><input type="radio" name="add-node-role" value="separate" checked={role === 'separate'} onChange={() => setRole('separate')} /><strong>Broker + Controller</strong></span><p>Two separate processes with distinct identities and storage.</p><small>Controller-first execution after both process checks pass</small></label>
                  </fieldset>
                  {role !== 'broker' && <p>The final review verifies existing participant runtimes, the live quorum and the selected process layout. Static quorum expansion requires planned broker and controller downtime.</p>}
                  {role !== 'controller' && <>
                  {role === 'broker' ? <div className="add-node-notice"><Info size={18} /><span>Adding a broker leaves controller voting membership unchanged. Existing partitions are not automatically moved; reassignment is a separate operation.</span></div>
                    : <div className="add-node-notice"><Info size={18} /><span>The broker process must pass the checks below before this layout can be reviewed. Final validation also checks the controller and combined resource requirements.</span></div>}
                  {readiness.preview && checkTable(readiness.preview.checks.filter(check => check.phase === 'BROKER'))}
                  <p role={readiness.brokerPassed ? undefined : 'status'}>{readiness.brokerPassed ? 'Broker checks passed. Final deployment validation will still be required.' : 'Resolve all failed, missing or expired broker checks, including the candidate rack, before review.'}</p>
                  {!readiness.brokerPassed && <button className="add-node-button" onClick={() => setStep(1)}>Edit host or broker rack</button>}
                  <button className="add-node-button" disabled={readiness.running} onClick={() => { setStep(2); void readiness.run(); }}>Run fresh checks</button>
                  </>}
                </>}

                {step === 4 && <>
                  {role === 'separate' ? <SeparateProcessAdditionPlanReview clusterId={clusterId} hostId={hostId} brokerRack={brokerRack} commonPreview={readiness.preview} /> : role === 'controller' ? <ControllerAdditionPlanReview clusterId={clusterId} hostId={hostId} commonPreview={readiness.preview} /> : <>
                  <p>Review the selected host, inherited settings and proposed broker plan.</p>
                  <dl className="add-node-review">
                    <dt>Cluster</dt><dd>{cluster.name}</dd>
                    <dt>Target host</dt><dd>{selectedHost?.hostname || hostId}<small>{selectedHost ? hostAddress(selectedHost) : 'No host selected'}</small></dd>
                    <dt>Process layout</dt><dd>Broker · one process</dd>
                    <dt>Broker rack</dt><dd>{brokerRack || 'Not configured'}</dd>
                    <dt>Kafka version</dt><dd>{reported(cluster.kafkaVersion)} · inherited</dd>
                    <dt>Kafka cluster ID</dt><dd><code>{reported(cluster.kafkaClusterId)}</code></dd>
                    <dt>Kafka node ID</dt><dd>Not allocated · will be reserved by the backend during deployment</dd>
                    <dt>Broker port</dt><dd>{reported(cluster.config?.listener_port)} · inherited configuration</dd>
                    <dt>Preview data paths</dt><dd>{evidence(readiness.preview?.brokerPlan.dataPaths)}</dd>
                    <dt>Final configuration</dt><dd>The proposed plan remains subject to final validation and atomic reservation before execution.</dd>
                    <dt>Readiness</dt><dd><span className="add-node-badge">{readiness.commonPassed && readiness.brokerPassed ? 'Preview checks passed' : 'Checks expired or blocked'}</span></dd>
                  </dl>
                  {canPreview && readiness.preview && readiness.commonPassed && readiness.brokerPassed && <BrokerAdditionPlanReview key={`${readiness.preview.previewId}:${readiness.preview.contextHash}`} clusterId={clusterId} preview={readiness.preview} />}
                  <div className="add-node-notice warning"><CircleAlert size={18} /><span>Review does not reserve a host or node ID. An accepted deployment job reserves them and repeats final checks before changing the candidate.</span></div>
                  </>}
                </>}

                {!canPreview && step > 1 && <div className="add-node-notice warning" role="alert">This selection is no longer available. Return to host selection or refresh inventory.</div>}
                <footer className="add-node-footer">
                  {step === 1 ? <Link className="add-node-button" to={`/clusters/${encodeURIComponent(clusterId)}/nodes`}><ArrowLeft size={16} /> Cancel</Link>
                    : <button className="add-node-button" onClick={() => setStep((step - 1) as Step)}><ArrowLeft size={16} /> Back</button>}
                  {step < 4 ? <button className="add-node-button primary" disabled={!canPreview || (step === 2 && !readiness.commonPassed) || (step === 3 && role !== 'controller' && !readiness.brokerPassed)} onClick={next}>{['Continue to checks', 'Continue to role', 'Review selected layout'][step - 1]}<ArrowRight size={16} /></button>
                    : null}
                </footer>
              </section>
            </>}
  </section>;
}
