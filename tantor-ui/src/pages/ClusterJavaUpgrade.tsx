import { useState, useEffect } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { ArrowUpCircle, RefreshCw, Coffee, Info, Server } from 'lucide-react';
import { usePermissions } from '../hooks/usePermissions';
import { confirmAction } from '../components/confirmUtils';
import { ClusterLoadingState } from '../components/ClusterLoadingState';
import { useActiveClusterJobs } from '../hooks/useActiveClusterJobs';
import { reportedJavaRuntime, type ReportedJavaHost } from '../utils/reportedJavaRuntime';
import { uniqueJavaUpgradeHosts } from '../utils/javaUpgradeHosts';
import './ClusterActions.css';

interface ClusterInfo {
  id: string;
  name?: string;
  kafkaVersion: string;
  status: string;
  mode: string;
  managementLevel?: string;
  nodeCount?: number;
  hosts?: Array<{ hostId: string; hostname: string; ipAddress: string }>;
}

interface HostParcel {
  artifactId: string;
  hostId: string;
  serviceType: string;
  version: string;
  status: string;
  active: boolean;
}

export function ClusterJavaUpgrade() {
  const navigate = useNavigate();
  const { id } = useParams<{ id: string }>();
  const { canManage } = usePermissions();
  const activeJobs = useActiveClusterJobs();
  const locked = activeJobs.some(job => job.clusterId === id && ['ROLLING_RESTART', 'JAVA_UPGRADE'].includes(job.type));
  const [cluster, setCluster] = useState<ClusterInfo | null>(null);
  const [parcels, setParcels] = useState<HostParcel[]>([]);
  const [javaClusterHosts, setJavaClusterHosts] = useState<Array<{hostId: string; hostname: string; ipAddress: string}>>([]);
  const [javaInventory, setJavaInventory] = useState<ReportedJavaHost[]>([]);
  const [javaArtifactId, setJavaArtifactId] = useState('');
  const [javaHostIds, setJavaHostIds] = useState<string[]>([]);
  const [javaPending, setJavaPending] = useState(false);
  const [javaError, setJavaError] = useState('');
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    const controller = new AbortController();
    const signal = controller.signal;
    void (async () => {
      try {
        const [clusterRes, parcelsRes, hostsRes] = await Promise.all([
          fetch(`/api/v1/ui/clusters/${id}`, { signal }),
          fetch('/api/v1/ui/parcels', { signal }),
          fetch('/api/v1/ui/hosts', { signal }).catch(() => null),
        ]);
        if (!clusterRes.ok) throw new Error('Unable to load Java upgrade information.');
        const loaded: ClusterInfo = await clusterRes.json();
        const inventory: unknown = hostsRes?.ok ? await hostsRes.json().catch(() => []) : [];
        const available = parcelsRes.ok ? await parcelsRes.json() : [];
        let hosts = loaded.hosts || [];
        if (loaded.mode === 'EXTERNAL') {
          const response = await fetch(`/api/v1/ui/external-java/hosts?clusterId=${id}`, { signal });
          hosts = response.ok ? await response.json() : [];
        }
        if (signal.aborted) return;
        setCluster(loaded);
        setParcels(available);
        setJavaInventory(Array.isArray(inventory) ? inventory : []);
        setJavaClusterHosts(uniqueJavaUpgradeHosts(hosts));
      } catch (error) {
        if (!signal.aborted) setJavaError(error instanceof Error ? error.message : 'Unable to load Java upgrade information.');
      } finally {
        if (!signal.aborted) setLoading(false);
      }
    })();
    return () => controller.abort();
  }, [id]);

  const stagedJava = parcels.filter(parcel => parcel.serviceType === 'JAVA' && parcel.status === 'DISTRIBUTED'
    && javaClusterHosts.some(host => host.hostId === parcel.hostId));
  const javaArtifacts = Array.from(new Map(stagedJava.map(parcel => [parcel.artifactId, parcel])).values());
  const javaArtifact = javaArtifacts.find(artifact => artifact.artifactId === javaArtifactId);
  const javaHosts = javaClusterHosts.filter(host =>
    stagedJava.some(parcel => parcel.artifactId === javaArtifactId && parcel.hostId === host.hostId)) || [];

  const triggerJavaUpgrade = async () => {
    if (!canManage || !id || !javaArtifact || javaHostIds.length === 0 || javaPending || locked) return;
    if (!(await confirmAction(`Switch the VM's default Java to ${javaArtifact.version} on ${javaHostIds.length} selected host(s)? Running Kafka services will restart one host at a time.`))) return;
    setJavaPending(true);
    setJavaError('');
    try {
      const reply = await fetch(`/api/v1/ui/clusters/${id}/java-upgrade`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ artifactId: javaArtifactId, version: javaArtifact.version, hostIds: javaHostIds }),
      });
      const result = await reply.json().catch(() => ({}));
      if (!reply.ok || !result.jobId) throw new Error(result.error || result.message || 'Could not start Java upgrade.');
      navigate(`/jobs/${result.jobId}`);
    } catch (error) {
      setJavaError(error instanceof Error ? error.message : 'Could not start Java upgrade.');
    } finally {
      setJavaPending(false);
    }
  };

  if (loading) return <ClusterLoadingState />;
  return <div className="cluster-actions-page">
    {locked && <p className="cluster-actions-lock" role="status">Operation in progress</p>}
    {!cluster && javaError && <p className="cluster-action-error" role="alert">{javaError}</p>}
    <div className="cluster-actions-cards">
        {canManage && (cluster?.mode?.toLowerCase() === 'kraft' || cluster?.mode === 'EXTERNAL') && (
          <section className="cluster-action-card" aria-labelledby="java-action-title">
            <div className="cluster-action-heading"><span className="cluster-action-icon"><Coffee size={21} aria-hidden="true" /></span>
              <div><h3 id="java-action-title">Upgrade Java</h3><p>Upgrade the Java runtime on your selected Kafka hosts.</p></div>
            </div>
            <p className="cluster-action-note"><Info size={16} aria-hidden="true" /><span>Confirm the JDK is compatible with Kafka {cluster.kafkaVersion} and the selected hosts are dedicated Kafka VMs.</span></p>
            <div className="cluster-java-runtime-table">
              <table aria-label="Currently reported Java runtime"><thead><tr><th>Host</th><th>Installed Java (reported)</th><th>Distribution / vendor</th></tr></thead>
                <tbody>{javaClusterHosts.length ? javaClusterHosts.map(host => {
                  const runtime = reportedJavaRuntime(host, javaInventory);
                  return <tr key={host.hostId}><th scope="row">{host.hostname || host.hostId}{host.ipAddress && <small>{host.ipAddress}</small>}</th><td>{runtime.version}</td><td>{runtime.distribution}</td></tr>;
                }) : <tr><td colSpan={3}>No eligible Kafka hosts are reported for this cluster.</td></tr>}</tbody>
              </table>
            </div>
            <p className="cluster-action-hint">Agent-reported inventory may be older than the running JVM. Missing version or vendor information is shown as Not reported; verify compatibility before upgrading.</p>
            <div className="cluster-action-form">
              <label className="cluster-action-field" htmlFor="java-upgrade-artifact">Distributed JDK
                <select id="java-upgrade-artifact" value={javaArtifactId} disabled={javaPending}
                  onChange={event => { setJavaArtifactId(event.target.value); setJavaHostIds([]); setJavaError(''); }}>
                  <option value="">Select a JDK</option>
                  {javaArtifacts.map(artifact => <option key={artifact.artifactId} value={artifact.artifactId}>Java {artifact.version}</option>)}
                </select>
              </label>
              {javaArtifacts.length === 0 && <p className="cluster-action-hint">Distribute a JDK to these hosts from Artifacts first.</p>}
              {javaHosts.length > 0 && (
                <fieldset className="cluster-action-hosts" disabled={javaPending}>
                  <legend>Kafka hosts <span>{javaHostIds.length} selected</span></legend>
                  <div className="cluster-action-host-grid">
                    {javaHosts.map(host => (
                      <label className="cluster-action-host" key={host.hostId} data-selected={javaHostIds.includes(host.hostId)}>
                        <input type="checkbox" checked={javaHostIds.includes(host.hostId)}
                          onChange={event => setJavaHostIds(current => event.target.checked
                            ? Array.from(new Set([...current, host.hostId])) : current.filter(id => id !== host.hostId))} />
                        <Server size={18} aria-hidden="true" /><span><strong>{host.hostname || host.hostId}</strong>{host.ipAddress && <small>{host.ipAddress}</small>}</span>
                      </label>
                    ))}
                  </div>
                </fieldset>
              )}
            </div>
            {javaError && <p className="cluster-action-error" role="alert">{javaError}</p>}
            <div className="cluster-action-footer"><button className="cluster-action-primary" type="button" disabled={!javaArtifact || javaHostIds.length === 0 || javaPending || locked}
              onClick={triggerJavaUpgrade}>{javaPending ? <RefreshCw size={16} className="spin" aria-hidden="true" /> : <ArrowUpCircle size={16} aria-hidden="true" />}{javaPending ? 'Queueing…' : 'Upgrade Java'}</button></div>
          </section>
        )}
    </div>
  </div>;
}
