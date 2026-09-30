import { useState, useEffect, useCallback } from 'react';
import { useNavigate, useParams, useSearchParams } from 'react-router-dom';
import { Activity, ArrowUpCircle, RefreshCw, CheckCircle2, XCircle } from 'lucide-react';
import { usePermissions } from '../hooks/usePermissions';
import { confirmAction, notifyAction } from '../components/confirmUtils';

interface ClusterInfo {
  id: string;
  kafkaVersion: string;
  status: string;
  mode: string;
  managementLevel?: string;
  nodeCount?: number;
  hosts?: Array<{ hostId: string; hostname: string; ipAddress: string }>;
}

interface HostParcel {
  hostId: string;
  serviceType: string;
  version: string;
  status: string;
  active: boolean;
}

export function ClusterActions() {
  const navigate = useNavigate();
  const { id } = useParams<{ id: string }>();
  const { canManage } = usePermissions();
  const [searchParams] = useSearchParams();
  const restartTaskFromUrl = searchParams.get('restartTask');
  const [taskId] = useState<string | null>(restartTaskFromUrl);
  const [status, setStatus] = useState<string>(restartTaskFromUrl ? 'Loading restart progress...' : '');
  const [loading, setLoading] = useState(false);
  const [cluster, setCluster] = useState<ClusterInfo | null>(null);
  const [parcels, setParcels] = useState<HostParcel[]>([]);
  const [targetVersion, setTargetVersion] = useState('');
  const [upgradeMsg, setUpgradeMsg] = useState('');
  const [upgradeLoading, setUpgradeLoading] = useState(false);
  const [monitoringHostId, setMonitoringHostId] = useState('');
  const [prometheusUrl, setPrometheusUrl] = useState('');
  const [grafanaUrl, setGrafanaUrl] = useState('');
  const [monitoringLoading, setMonitoringLoading] = useState(false);

  const fetchUpgradeContext = useCallback(async () => {
    try {
      const [clusterRes, parcelsRes] = await Promise.all([
        fetch(`/api/v1/ui/clusters/${id}`),
        fetch('/api/v1/ui/parcels'),
      ]);
      if (clusterRes.ok) {
        const loaded = await clusterRes.json();
        setCluster(loaded);
        setMonitoringHostId(current => current || loaded.hosts?.[0]?.hostId || '');
      }
      if (parcelsRes.ok) setParcels(await parcelsRes.json());
    } catch (e) {
      console.error(e);
    }
  }, [id]);

  const triggerRollingRestart = async () => {
    if (!canManage) return;
    const nodeCount = cluster?.nodeCount || cluster?.hosts?.length || 0;
    const warning = nodeCount === 1
      ? 'WARNING: Only one node is present. Three nodes are recommended for availability, and Kafka will be interrupted during this restart. Do you want to continue?'
      : 'WARNING: This will begin a rolling restart of the cluster. Continue?';
    if (!(await confirmAction(warning))) return;
    
    setLoading(true);
    try {
      const res = await fetch(`/api/v1/clusters/${id}/actions/rolling-restart`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ confirmSingleNode: nodeCount === 1 }),
      });
      if (res.ok) {
        const data = await res.json();
        if (data.jobId) navigate(`/jobs/${data.jobId}`);
      } else {
        const data = await res.json().catch(() => ({}));
        notifyAction(data.error || "Failed to trigger rolling restart.");
      }
    } catch {
      notifyAction("Error triggering rolling restart.");
    } finally {
      setLoading(false);
    }
  };

  const activeUpgradeVersions = Array.from(new Set(
    parcels
      .filter(p => p.serviceType === 'KAFKA' && p.active && p.status === 'ACTIVE' && p.version !== cluster?.kafkaVersion)
      .map(p => p.version)
  )).sort((a, b) => a.localeCompare(b, undefined, { numeric: true })).reverse();

  const enableMonitoring = async () => {
    if (!canManage) return;
    if (!monitoringHostId || !prometheusUrl.trim() || !grafanaUrl.trim()) {
      notifyAction('Select a host and provide both Prometheus and Grafana artifact URLs.');
      return;
    }
    setMonitoringLoading(true);
    try {
      const res = await fetch(`/api/v1/clusters/${id}/actions/enable-monitoring`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ hostId: monitoringHostId, prometheusUrl, grafanaUrl }),
      });
      const body = await res.json().catch(() => ({}));
      if (res.ok && body.jobId) navigate(`/jobs/${body.jobId}`);
      else notifyAction(body.error || 'Failed to create monitoring enablement job.');
    } catch {
      notifyAction('Network error while creating monitoring job.');
    } finally {
      setMonitoringLoading(false);
    }
  };

  useEffect(() => {
    void (async () => { await fetchUpgradeContext(); })();
  }, [fetchUpgradeContext]);

  useEffect(() => {
    Promise.resolve().then(() => {
      if (!targetVersion && activeUpgradeVersions.length > 0) {
        setTargetVersion(activeUpgradeVersions[0]);
      }
    });
  // activeUpgradeVersions reference changes on every render ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â only react to length change
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeUpgradeVersions.length, targetVersion]);

  const triggerUpgrade = async () => {
    if (!canManage) return;
    if (!targetVersion) return;
    if (!(await confirmAction(`Upgrade this cluster from Kafka ${cluster?.kafkaVersion || 'current'} to ${targetVersion}?`))) return;

    setUpgradeLoading(true);
    setUpgradeMsg('');
    try {
      const res = await fetch(`/api/v1/ui/clusters/${id}/upgrade`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ targetVersion }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(data.error || 'Failed to schedule upgrade.');
      }
      setUpgradeMsg(`Upgrade to Kafka ${targetVersion} scheduled. Watch Deployment Logs for symlink switch, validation, and automatic rollback details.`);
      await fetchUpgradeContext();
    } catch (e: unknown) {
      setUpgradeMsg(e instanceof Error ? e.message : 'Failed to schedule upgrade.');
    } finally {
      setUpgradeLoading(false);
    }
  };

  useEffect(() => {
    if (!taskId) return;

    const interval = setInterval(async () => {
      try {
        const res = await fetch(`/api/v1/clusters/${id}/actions/tasks/${taskId}`);
        if (res.ok) {
          const data = await res.json();
          setStatus(data.status);
          if (data.status.startsWith('COMPLETED') || data.status.startsWith('FAILED') || data.status.startsWith('PAUSED')) {
            clearInterval(interval);
          }
        }
      } catch (e) {
        console.error(e);
      }
    }, 2000);

    return () => clearInterval(interval);
  }, [taskId, id]);

  return (
    <div className="topics-tab" style={{ width: '100%' }}>
      <div className="topics-header" style={{ marginBottom: '1.5rem' }}>
        <div>
          <h2 className="cluster-section-heading">Cluster Actions</h2>
          <p style={{ fontSize: '0.875rem', color: 'var(--text-secondary)', marginTop: '0.25rem' }}>Perform disruptive day-two operations on your cluster.</p>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: '1.5rem' }}>
        {canManage && (
        <div style={{ border: '1px solid var(--border-subtle)', borderRadius: 'var(--radius-lg)', padding: '24px 32px', background: "var(--bg-surface)" }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: '1.5rem' }}>
            <div style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              width: '32px',
              height: '32px',
              borderRadius: '50%',
              border: '1px solid #94a3b8',
              color: 'var(--text-muted)',
              flexShrink: 0
            }}>
              <ArrowUpCircle size={18} aria-hidden="true" />
            </div>
            <div style={{ flex: 1 }}>
              <h3 style={{ fontWeight: 'var(--font-semibold)', color: 'var(--text-primary)', margin: '0 0 4px 0', fontSize: 'var(--text-md)' }}>Upgrade Kafka Version</h3>
              <p style={{ fontSize: 'var(--text-sm)', color: 'var(--text-secondary)', margin: 0 }}>
                Apply an active parcel version to this running cluster.
              </p>
            </div>
          </div>

          <p style={{ fontSize: 'var(--text-sm)', color: '#475569', marginTop: '1.25rem', marginBottom: '1.25rem', lineHeight: 1.6 }}>
            Choose a new active parcel, then Tantor stages the target binaries into the versioned install directory, switches the stable Kafka symlink, validates the service, and automatically rolls back to the previous symlink target if validation fails.
          </p>

          <div style={{
            background: '#f8fafc',
            border: '1px solid #f1f5f9',
            borderRadius: 'var(--radius-lg)',
            padding: 'var(--space-4)',
            display: 'grid',
            gridTemplateColumns: '1fr auto',
            gap: '12px',
            alignItems: 'center'
          }}>
            <select
              value={targetVersion}
              onChange={e => setTargetVersion(e.target.value)}
              disabled={upgradeLoading}
              style={{
                width: '100%',
                padding: '10px 12px',
                borderRadius: 'var(--radius-md)',
                border: '1px solid var(--border-subtle)',
                fontSize: 'var(--text-base)',
                background: "var(--bg-surface)",
                outline: 'none',
                color: 'var(--button-primary-active)'
              }}
            >
              {activeUpgradeVersions.length === 0 ? (
                <option value="">No active upgrade parcel</option>
              ) : activeUpgradeVersions.map(version => (
                <option key={version} value={version}>Kafka {version}</option>
              ))}
            </select>
            <button
              onClick={triggerUpgrade}
              disabled={upgradeLoading}
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                justifyContent: 'center',
                gap: 'var(--space-2)',
                height: '42px',
                padding: '0 20px',
                borderRadius: 'var(--radius-md)',
                background: 'var(--button-primary)',
                color: "var(--text-light)",
                fontWeight: 'var(--font-medium)',
                fontSize: 'var(--text-base)',
                border: 'none',
                cursor: 'pointer',
                transition: 'all 0.2s'
              }}
            >
              {upgradeLoading ? <RefreshCw size={16} className="spin" /> : null}
              Upgrade Kafka
            </button>
          </div>

          {upgradeMsg && (
            <p style={{ margin: '1rem 0 0', fontSize: '0.875rem', color: upgradeMsg.startsWith('Failed') ? '#b91c1c' : '#166534' }}>
              {upgradeMsg}
            </p>
          )}
        </div>
        )}
        
        {canManage && (
          <div style={{ border: '1px solid var(--border-subtle)', borderRadius: 'var(--radius-lg)', padding: '24px 32px', background: "var(--bg-surface)" }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: '1.5rem' }}>
              <div style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                width: '32px',
                height: '32px',
                borderRadius: '50%',
                border: '1px solid #94a3b8',
                color: 'var(--text-muted)',
                flexShrink: 0
              }}>
                <Activity size={18} aria-hidden="true" />
              </div>
              <div style={{ flex: 1 }}>
                <h3 style={{ fontWeight: 'var(--font-semibold)', color: 'var(--text-primary)', margin: '0 0 4px 0', fontSize: 'var(--text-md)' }}>Monitoring Enablement</h3>
                <p style={{ fontSize: 'var(--text-sm)', color: 'var(--text-secondary)', margin: 0 }}>Deploy Prometheus and Grafana through a tracked job.</p>
              </div>
            </div>

            <p style={{ fontSize: 'var(--text-sm)', color: '#475569', marginTop: '1.25rem', marginBottom: '1.25rem', lineHeight: 1.6 }}>
              Choose a new active parcel, then Tantor stages the target binaries into the versioned install directory, switches the stable Kafka symlink, validates the service, and automatically rolls back to the previous symlink target if validation fails.
            </p>

            <div style={{
              background: '#f8fafc',
              border: '1px solid #f1f5f9',
              borderRadius: 'var(--radius-lg)',
              padding: 'var(--space-4)',
              display: 'flex',
              flexDirection: 'column',
              gap: '12px'
            }}>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px' }}>
                <select 
                  value={monitoringHostId} 
                  onChange={event => setMonitoringHostId(event.target.value)}
                  style={{
                    width: '100%',
                    padding: '10px 12px',
                    borderRadius: 'var(--radius-md)',
                    border: '1px solid var(--border-subtle)',
                    fontSize: 'var(--text-base)',
                    background: "var(--bg-surface)",
                    outline: 'none',
                    color: 'var(--button-primary-active)'
                  }}
                >
                  {(cluster?.hosts || []).map(host => <option key={host.hostId} value={host.hostId}>{host.hostname || host.ipAddress || 'Unnamed host'}{host.hostname && host.ipAddress ? ` | ${host.ipAddress}` : ''}</option>)}
                </select>
                <input 
                  value={prometheusUrl} 
                  onChange={event => setPrometheusUrl(event.target.value)} 
                  placeholder="Prometheus Artifact URL" 
                  style={{
                    width: '100%',
                    padding: '10px 12px',
                    borderRadius: 'var(--radius-md)',
                    border: '1px solid var(--border-subtle)',
                    fontSize: 'var(--text-base)',
                    background: "var(--bg-surface)",
                    outline: 'none',
                    color: 'var(--button-primary-active)'
                  }}
                />
              </div>
              <div style={{ display: 'flex', gap: '12px' }}>
                <input 
                  value={grafanaUrl} 
                  onChange={event => setGrafanaUrl(event.target.value)} 
                  placeholder="Grafana Artifact URL" 
                  style={{
                    flex: 1,
                    padding: '10px 12px',
                    borderRadius: 'var(--radius-md)',
                    border: '1px solid var(--border-subtle)',
                    fontSize: 'var(--text-base)',
                    background: "var(--bg-surface)",
                    outline: 'none',
                    color: 'var(--button-primary-active)'
                  }}
                />
                <button 
                  onClick={enableMonitoring} 
                  disabled={monitoringLoading}
                  style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    gap: 'var(--space-2)',
                    height: '42px',
                    padding: '0 20px',
                    borderRadius: 'var(--radius-md)',
                    background: 'var(--button-primary)',
                    color: "var(--text-light)",
                    fontWeight: 'var(--font-medium)',
                    fontSize: 'var(--text-base)',
                    border: 'none',
                    cursor: 'pointer',
                    transition: 'all 0.2s'
                  }}
                >
                  {monitoringLoading ? <RefreshCw size={16} className="spin" /> : null} 
                  Enable Monitoring
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Rolling Restart Card */}
        {canManage && (
        <div style={{ border: '1px solid var(--border-subtle)', borderRadius: 'var(--radius-lg)', padding: '24px 32px', background: "var(--bg-surface)" }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: '1.5rem' }}>
            <div style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              width: '32px',
              height: '32px',
              borderRadius: '50%',
              border: '1px solid #94a3b8',
              color: 'var(--text-muted)',
              flexShrink: 0
            }}>
              <RefreshCw size={18} aria-hidden="true" />
            </div>
            <div style={{ flex: 1 }}>
              <h3 style={{ fontWeight: 'var(--font-semibold)', color: 'var(--text-primary)', margin: '0 0 4px 0', fontSize: 'var(--text-md)' }}>Rolling Restart</h3>
              <p style={{ fontSize: 'var(--text-sm)', color: 'var(--text-secondary)', margin: 0 }}>
                Restart quorum services and brokers one at a time.
              </p>
            </div>
          </div>
          <p style={{ fontSize: 'var(--text-sm)', color: '#475569', marginTop: '1.25rem', marginBottom: '1.5rem', lineHeight: 1.6 }}>
            The orchestrator restarts non-leader controllers or ZooKeeper members first, then the active controller and brokers. It waits for each agent task, all brokers, and all replicas to become healthy before continuing. Zero-downtime restart requires at least two brokers and three metadata quorum nodes.
          </p>
          <div style={{
            background: '#f8fafc',
            border: '1px solid #f1f5f9',
            borderRadius: 'var(--radius-lg)',
            padding: 'var(--space-4)',
            display: 'flex',
            justifyContent: 'flex-end'
          }}>
            <button 
              onClick={triggerRollingRestart}
              disabled={loading}
              style={{
                display: 'inline-flex',
                alignItems: 'center',
                justifyContent: 'center',
                gap: 'var(--space-2)',
                height: '42px',
                padding: '0 20px',
                borderRadius: 'var(--radius-md)',
                background: 'var(--button-primary)',
                color: "var(--text-light)",
                fontWeight: 'var(--font-medium)',
                fontSize: 'var(--text-base)',
                border: 'none',
                cursor: 'pointer',
                transition: 'all 0.2s'
              }}
            >
              Start Rolling Restart
            </button>
          </div>

          {/* Progress Tracker */}
          {taskId && (
            <div style={{ backgroundColor: '#f9fafb', padding: '1.5rem', marginTop: '1.5rem', borderRadius: 'var(--radius-md)' }}>
              <h4 style={{ fontSize: '0.875rem', fontWeight: 'var(--font-medium)', color: 'var(--text-primary)', marginBottom: '0.75rem', display: 'flex', alignItems: 'center', gap: '0.5rem', margin: '0 0 0.75rem 0' }}>
                <Activity size={16} color="#3b82f6" /> Live Task Status
              </h4>
              <div style={{ backgroundColor: '#111827', borderRadius: '0.5rem', padding: '1rem', fontFamily: 'monospace', fontSize: '0.875rem', boxShadow: 'inset 0 2px 4px 0 rgba(0, 0, 0, 0.06)', position: 'relative', overflow: 'hidden' }}>
                {!status.startsWith('COMPLETED') && !status.startsWith('FAILED') && !status.startsWith('PAUSED') && (
                  <div style={{ position: 'absolute', top: 0, left: 0, width: '100%', height: '4px' }}>
                    <div style={{ height: '100%', backgroundColor: '#3b82f6', width: '33%', animation: 'pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite' }}></div>
                  </div>
                )}
                <div style={{ display: 'flex', alignItems: 'flex-start', gap: '0.75rem' }}>
                  {status.startsWith('COMPLETED') ? (
                    <CheckCircle2 size={18} color="#34d399" style={{ flexShrink: 0, marginTop: '2px' }} />
                  ) : status.startsWith('FAILED') || status.startsWith('PAUSED') ? (
                    <XCircle size={18} color="#f87171" style={{ flexShrink: 0, marginTop: '2px' }} />
                  ) : (
                    <RefreshCw size={18} color="#60a5fa" className="spin" style={{ flexShrink: 0, marginTop: '2px' }} />
                  )}
                  <span style={{ wordBreak: 'break-all', color: status.startsWith('FAILED') || status.startsWith('PAUSED') ? '#fca5a5' : '#d1d5db' }}>
                    {status}
                  </span>
                </div>
              </div>
            </div>
          )}
        </div>
        )}

      </div>
    </div>
  );
}
