import { useState, useEffect, useCallback } from 'react';
import { useNavigate, useParams, useSearchParams } from 'react-router-dom';
import { Activity, ArrowUpCircle, RefreshCw, CheckCircle2, XCircle, SlidersHorizontal } from 'lucide-react';
import './ClusterActions.css';
import { usePermissions } from '../hooks/usePermissions';
import { confirmAction, notifyAction } from '../components/confirmUtils';

import { RollingRestartModal } from '../components/RollingRestartModal';
import { useActiveClusterJobs } from '../hooks/useActiveClusterJobs';

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

export function ClusterActions() {
  const navigate = useNavigate();
  const { id } = useParams<{ id: string }>();
  const { canManage } = usePermissions();
  const [searchParams] = useSearchParams();
  const restartTaskFromUrl = searchParams.get('restartTask');
  const [taskId] = useState<string | null>(restartTaskFromUrl);
  const [status, setStatus] = useState<string>(restartTaskFromUrl ? 'Loading restart progress...' : '');
  const [restartOpen, setRestartOpen] = useState(false);
  const activeJobs = useActiveClusterJobs();
  const locked = activeJobs.some(job => job.clusterId === id && ['ROLLING_RESTART', 'JAVA_UPGRADE'].includes(job.type));
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
    <div className="cluster-actions-page">
      <header className="cluster-actions-header">
        <SlidersHorizontal size={25} aria-hidden="true" />
        <div><h2>Cluster Actions</h2><p>Perform disruptive day-two operations on your cluster.</p></div>
        {locked && <span className="cluster-actions-lock">Operation in progress</span>}
      </header>
      <div className="cluster-actions-cards">
        {canManage && cluster?.mode !== 'EXTERNAL' && (
          <section className="cluster-action-card" aria-labelledby="kafka-action-title">
            <div className="cluster-action-heading"><span className="cluster-action-icon"><ArrowUpCircle size={21} aria-hidden="true" /></span>
              <div><h3 id="kafka-action-title">Upgrade Kafka Version</h3><p>Apply an active parcel version to this running cluster.</p></div>
            </div>
            <p className="cluster-action-description">Choose a new active parcel, then Tantor stages the target binaries into the versioned install directory, switches the stable Kafka symlink, validates the service, and automatically rolls back to the previous symlink target if validation fails.</p>
            <div className="cluster-action-form cluster-action-inline">
              <label className="cluster-action-field">Target Kafka version
                <select value={targetVersion} onChange={e => setTargetVersion(e.target.value)} disabled={upgradeLoading || locked}>
                  {activeUpgradeVersions.length === 0 ? <option value="">No active upgrade parcel</option>
                    : activeUpgradeVersions.map(version => <option key={version} value={version}>Kafka {version}</option>)}
                </select>
              </label>
              <button className="cluster-action-primary" onClick={triggerUpgrade} disabled={upgradeLoading || locked}>
                {upgradeLoading ? <RefreshCw size={16} className="spin" aria-hidden="true" /> : <ArrowUpCircle size={16} aria-hidden="true" />}Upgrade Kafka
              </button>
            </div>
            {upgradeMsg && <p className={upgradeMsg.startsWith('Failed') ? 'cluster-action-error' : 'cluster-action-result'} role="status">{upgradeMsg}</p>}
          </section>
        )}
        {canManage && cluster?.mode !== 'EXTERNAL' && (
          <section className="cluster-action-card" aria-labelledby="monitoring-action-title">
            <div className="cluster-action-heading"><span className="cluster-action-icon"><Activity size={21} aria-hidden="true" /></span>
              <div><h3 id="monitoring-action-title">Monitoring Enablement</h3><p>Deploy Prometheus and Grafana through a tracked job.</p></div>
            </div>
            <p className="cluster-action-description">Select the target host and provide the Prometheus and Grafana artifact URLs.</p>
            <div className="cluster-action-form cluster-action-monitoring">
              <label className="cluster-action-field">Target host
                <select value={monitoringHostId} onChange={event => setMonitoringHostId(event.target.value)}>
                  {(cluster?.hosts || []).map(host => <option key={host.hostId} value={host.hostId}>{host.hostname || host.ipAddress || 'Unnamed host'}{host.hostname && host.ipAddress ? ` | ${host.ipAddress}` : ''}</option>)}
                </select>
              </label>
              <label className="cluster-action-field">Prometheus artifact URL<input value={prometheusUrl} onChange={event => setPrometheusUrl(event.target.value)} placeholder="Prometheus Artifact URL" /></label>
              <label className="cluster-action-field">Grafana artifact URL<input value={grafanaUrl} onChange={event => setGrafanaUrl(event.target.value)} placeholder="Grafana Artifact URL" /></label>
            </div>
            <div className="cluster-action-footer"><button className="cluster-action-primary" onClick={enableMonitoring} disabled={monitoringLoading || locked}>
              {monitoringLoading ? <RefreshCw size={16} className="spin" aria-hidden="true" /> : <Activity size={16} aria-hidden="true" />}Enable Monitoring
            </button></div>
          </section>
        )}
        {canManage && cluster?.mode !== 'EXTERNAL' && (
          <section className="cluster-action-card" aria-labelledby="restart-action-title">
            <div className="cluster-action-heading"><span className="cluster-action-icon"><RefreshCw size={21} aria-hidden="true" /></span>
              <div><h3 id="restart-action-title">Rolling Restart</h3><p>Restart quorum services and brokers one at a time.</p></div>
            </div>
            <p className="cluster-action-description">Choose brokers, controllers, all Kafka processes or selected processes. Combined roles restart together. Health gates stop the sequence on failure; Retry / Resume preserves completed nodes.</p>
            <div className="cluster-action-footer"><button className="cluster-action-primary" onClick={() => setRestartOpen(true)} disabled={activeJobs.some(job => job.clusterId === id)}>
              <RefreshCw size={16} aria-hidden="true" />Start Rolling Restart
            </button></div>
            {taskId && <div className="cluster-action-progress" role="status"><h4><Activity size={16} aria-hidden="true" />Live Task Status</h4>
              <div className="cluster-action-status" data-failed={status.startsWith('FAILED') || status.startsWith('PAUSED')}>
                {status.startsWith('COMPLETED') ? <CheckCircle2 size={18} aria-hidden="true" />
                  : status.startsWith('FAILED') || status.startsWith('PAUSED') ? <XCircle size={18} aria-hidden="true" />
                    : <RefreshCw size={18} className="spin" aria-hidden="true" />}<span>{status}</span>
              </div>
            </div>}
          </section>
        )}
      </div>
      {restartOpen && cluster && <RollingRestartModal cluster={cluster} onClose={() => setRestartOpen(false)} onStarted={jobId => { setRestartOpen(false); navigate(`/jobs/${jobId}`); }} />}
    </div>
  );
}
