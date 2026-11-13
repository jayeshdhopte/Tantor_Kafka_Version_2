import { useEffect, useRef, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { ArrowLeft, ArrowRight, CircleAlert, Info, Loader2, RefreshCw, Server } from 'lucide-react';
import { usePermissions } from '../hooks/usePermissions';
import { useActiveClusterJobs } from '../hooks/useActiveClusterJobs';
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
  const refresh = () => { setInventory(null); setError(''); setHostId(''); setStep(1); setRevision(value => value + 1); };
  return <section className="add-node-page animate-fade-in" aria-labelledby="external-add-node-title">
    <nav className="add-node-breadcrumb" aria-label="Breadcrumb"><Link to="/clusters">Cluster</Link><span>/</span><span>{cluster?.name || 'External cluster'}</span><span>/</span><span aria-current="page">Add Node</span></nav>
    <header className="add-node-header"><div><h1 id="external-add-node-title">Add Node</h1><p>Add Node for an external Kafka cluster.</p></div>
      {canManage && <button className="add-node-button" onClick={refresh} disabled={!inventory && !error}><RefreshCw size={16} /> Refresh inventory</button>}
    </header>
    <div className="add-node-notice"><Info size={18} /><span><strong>External Add Node preview.</strong> You can review the broker flow. Readiness checks and deployment are not available yet. No host, Kafka service or cluster membership will be changed.</span></div>
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
                      <label className="full">Target external host<select aria-label="Target external host" value={hostId} disabled={!supportedMode || Boolean(activeJob)} onChange={event => setHostId(event.target.value)}><option value="">Select an enrolled external host</option>
                        {inventory.hosts.map(candidate => <option key={candidate.id} value={candidate.id} disabled={Boolean(unavailable(candidate))}>{candidate.hostname || candidate.agentName || candidate.id} — {address(candidate)}{unavailable(candidate) ? ` · ${unavailable(candidate)}` : ''}</option>)}
                      </select><small>Only online, unlinked external agents can be selected. Full host readiness is checked in a later phase.</small></label>
                    </div>
                    {!inventory.hosts.some(candidate => !unavailable(candidate)) && <p className="add-node-notice warning" role="status">No eligible external host is currently enrolled. Enroll a host or restore its external-agent heartbeat, then refresh inventory.</p>}
                  </>}
                  {step === 2 && <>
                    <p>Selected host: {host?.hostname || hostId}. Readiness is not implemented in this preview. Continuing reviews the screens only.</p>
                    <div className="add-node-table-wrap"><table className="add-node-table"><thead><tr><th>Required check</th><th>Status</th></tr></thead><tbody>{pendingChecks.map(check => <tr key={check}><th scope="row">{check}</th><td><span className="add-node-badge">Not run</span></td></tr>)}</tbody></table></div>
                    <p>No PASS result, reservation or deployment authorization is created by this screen.</p>
                  </>}
                  {step === 3 && <fieldset className="add-node-roles"><legend className="add-node-sr-only">Kafka process layout</legend>
                    <label className="add-node-role selected"><span><input type="radio" name="external-add-role" checked readOnly /><Server size={18} /><strong>Broker</strong></span><p>Stores partitions and serves producer and consumer traffic.</p><small>Broker flow preview</small></label>
                    <label className="add-node-role"><span><input type="radio" name="external-add-role" disabled /><strong>Controller</strong></span><p>Participates in the metadata quorum.</p><small>Unavailable in this phase</small></label>
                    <label className="add-node-role"><span><input type="radio" name="external-add-role" disabled /><strong>Broker + Controller</strong></span><p>Two separate processes with distinct identities and storage.</p><small>Unavailable in this phase</small></label>
                  </fieldset>}
                  {step === 4 && <><dl className="add-node-review"><dt>Cluster</dt><dd>{cluster.name}</dd><dt>Kafka cluster ID</dt><dd><code>{cluster.clusterId || 'Not reported'}</code></dd><dt>Kafka version</dt><dd>{cluster.kafkaVersion || 'Not reported'}</dd><dt>Target host</dt><dd>{host?.hostname || hostId} · {host ? address(host) : 'Not selected'}</dd><dt>External agent</dt><dd>{host?.version || 'Not reported'}</dd><dt>Process role</dt><dd>broker</dd><dt>Broker node ID</dt><dd>Not allocated</dd><dt>Readiness</dt><dd>Not run</dd><dt>Deployment</dt><dd>Disabled — external broker execution is not available yet.</dd></dl><p>Reviewing this selection creates no job, host reservation, service configuration or Kafka membership.</p><button className="add-node-button primary" disabled>Deploy broker</button></>}
                  {!canNavigate && step > 1 && <p className="add-node-notice warning" role="alert">This selection is no longer available. Return to host selection or refresh inventory.</p>}
                  <footer className="add-node-footer">{step === 1 ? <Link className="add-node-button" to={`/clusters/${encodeURIComponent(clusterId)}/nodes`}><ArrowLeft size={16} /> Cancel</Link> : <button className="add-node-button" onClick={() => setStep(value => value - 1)}><ArrowLeft size={16} /> Back</button>}
                    {step < 4 && <button className="add-node-button primary" disabled={!canNavigate} onClick={() => setStep(value => value + 1)}>{['Continue to checks', 'Continue to role', 'Review selected layout'][step - 1]}<ArrowRight size={16} /></button>}
                  </footer>
                </section>
              </>}
  </section>;
}
