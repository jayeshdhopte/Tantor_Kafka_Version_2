import { useEffect, useCallback, useMemo, useState } from 'react';
import { useParams } from 'react-router-dom';
import { AlertTriangle, CheckCircle2, Download } from 'lucide-react';
import './ClusterOverview.css';

interface OverviewSummary {
  brokerCount: number;
  activeController: number | null;
  activeControllerId: number | null;
  configuredControllerCount: number;
  version: string;
  controllerType: string;
}

interface PartitionSummary {
  online: number;
  total: number;
  underReplicated: number;
  inSyncReplicas: number;
  totalReplicas: number;
  outOfSyncReplicas: number;
}

interface BrokerRow {
  brokerId: number;
  host: string;
  port: number;
  controller: boolean;
  diskUsageBytes: number;
  diskTotalBytes: number;
  hostDiskUsedBytes: number | null;
  hostDiskTotalBytes: number | null;
  hostDiskMetricStatus: 'LIVE' | 'STALE' | 'UNAVAILABLE';
  hostDiskLastSeen: string | null;
  logReplicaCount: number;
  inSyncReplicas: number;
  replicas: number;
  replicaSkewPct: number | null;
  leaders: number;
  leaderSkewPct: number | null;
}

interface ControllerRow {
  nodeId: number;
  host: string;
  port: number | null;
  activeLeader: boolean;
}

interface NodePathRow {
  nodeId: number;
  host: string;
  role: string;
  installDir: string;
  config: string;
  dataDir: string;
  logDir: string;
  hasTelemetry: boolean;
}

interface ClusterOverviewResponse {
  name: string;
  kafkaClusterId: string;
  originType: string;
  installDirectory: string;
  configDirectory: string;
  dataDirectory: string;
  logDirectory: string;
  generatedAt: string;
  warnings: string[];
  uptime: OverviewSummary;
  partitions: PartitionSummary;
  brokers: BrokerRow[];
  controllers: ControllerRow[];
  nodePaths: NodePathRow[];
}

export function ClusterOverview() {
  const { id } = useParams<{ id: string }>();
  const [overview, setOverview] = useState<ClusterOverviewResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchOverview = useCallback(async () => {
    try {
      const res = await fetch(`/api/v1/clusters/${id}/overview`);
      if (!res.ok) {
        throw new Error('Failed to fetch cluster overview');
      }
      const data = await res.json();
      setOverview(data);
      setError(null);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to fetch cluster overview');
    } finally {
      setLoading(false);
    }
  }, [id]);

  useEffect(() => {
    void (async () => { await fetchOverview(); })();
    const interval = setInterval(() => { void (async () => { await fetchOverview(); })(); }, 10000);
    return () => clearInterval(interval);
  }, [fetchOverview]);

  const csv = useMemo(() => {
    if (!overview) return '';
    const rows = [
      ['Broker ID', 'Kafka Data Usage Bytes', 'Host Disk Used Bytes', 'Host Disk Total Bytes', 'Host Disk Status', 'Host Disk Last Seen', 'Log Replicas', 'In Sync Replicas', 'Replicas', 'Replica Skew', 'Leaders', 'Leader Skew', 'Port', 'Host'],
      ...overview.brokers.map(broker => [
        broker.brokerId,
        broker.diskUsageBytes,
        broker.hostDiskUsedBytes ?? '',
        broker.hostDiskTotalBytes ?? '',
        broker.hostDiskMetricStatus ?? 'UNAVAILABLE',
        broker.hostDiskLastSeen ?? '',
        broker.logReplicaCount,
        broker.inSyncReplicas,
        broker.replicas,
        formatSkew(broker.replicaSkewPct),
        broker.leaders,
        formatSkew(broker.leaderSkewPct),
        broker.port,
        broker.host,
      ]),
    ];
    return rows.map(row => row.map(value => `"${String(value).replaceAll('"', '""')}"`).join(',')).join('\n');
  }, [overview]);

  const exportCsv = () => {
    if (!overview) return;
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `${overview.name || 'cluster'}-brokers-overview.csv`;
    link.click();
    URL.revokeObjectURL(url);
  };

  if (loading && !overview) {
    return <div className="state-center">Loading cluster overview...</div>;
  }

  if (!overview) {
    return (
      <div className="overview-dashboard">
        <div className="overview-alert error">
          <AlertTriangle size={17} />
          <span>{error || 'Cluster overview is unavailable'}</span>
        </div>
      </div>
    );
  }

  const { uptime, partitions } = overview;

  return (
    <div className="overview-dashboard animate-fade-in">
      {error && <Notice kind="error" text={error} />}
      {overview.warnings?.map(warning => <Notice key={warning} kind="warning" text={warning} />)}

      <section className="overview-section">
        <div className="overview-card">
          <div className="section-header-row">
            <h2>Cluster Identity</h2>
            <button className="overview-export" type="button" onClick={exportCsv}>
              <Download size={16} />
              Export CSV
            </button>
          </div>
          <div className="overview-grid identity-grid">
            <div className="overview-item">
              <div className="overview-label">Kafka cluster ID</div>
              <div className="overview-value cluster-id-value">{overview.kafkaClusterId || '-'}</div>
            </div>
            <div className="overview-item">
              <div className="overview-label">Cluster type</div>
              <div className="overview-value">{overview.originType || '-'}</div>
            </div>
            {overview.originType !== 'EXTERNAL' && (
              <>
                <div className="overview-item">
                  <div className="overview-label">Install directory</div>
                  <div className="overview-value">{overview.installDirectory || '-'}</div>
                </div>
                <div className="overview-item">
                  <div className="overview-label">Config directory</div>
                  <div className="overview-value">{overview.configDirectory || '-'}</div>
                </div>
                <div className="overview-item">
                  <div className="overview-label">Data directory</div>
                  <div className="overview-value">{overview.dataDirectory || '-'}</div>
                </div>
                <div className="overview-item">
                  <div className="overview-label">Log directory</div>
                  <div className="overview-value">{overview.logDirectory || '-'}</div>
                </div>
              </>
            )}
          </div>
        </div>
      </section>

      <section className="overview-section">
        <div className="overview-card">
          <div className="section-header-row">
            <h2>Uptime</h2>
          </div>
          <div className="overview-grid uptime-grid">
            <div className="overview-item">
              <div className="overview-label">Broker Count</div>
              <div className="overview-value">{(uptime.brokerCount || 0).toString().padStart(2, '0')}</div>
            </div>
            <div className="overview-item">
              <div className="overview-label">
                {uptime.controllerType === 'ZooKeeper' ? 'Active Broker Controller ID' : 'Active Controller ID'}
              </div>
              <div className="overview-value">{uptime.activeControllerId ?? uptime.activeController ?? '-'}</div>
            </div>
            <div className="overview-item">
              <div className="overview-label">
                {uptime.controllerType === 'ZooKeeper' ? 'ZooKeeper Ensemble' : 'Configured Controllers'}
              </div>
              <div className="overview-value">
                {uptime.controllerType === 'ZooKeeper' ? 'Not reported' : (uptime.configuredControllerCount ?? '-')}
              </div>
            </div>
            <div className="overview-item">
              <div className="overview-label">Version</div>
              <div className="overview-value">{uptime.version || '-'}</div>
            </div>
          </div>
        </div>
      </section>

      <section className="overview-section">
        <div className="overview-card">
          <div className="section-header-row">
            <h2>Partitions</h2>
          </div>
          <div className="overview-grid partitions-grid">
            <div className="overview-item">
              <div className="overview-label">Online</div>
              <div className="overview-value">{partitions.online} of {partitions.total}</div>
            </div>
            <div className="overview-item">
              <div className="overview-label">URP</div>
              <div className="overview-value">{partitions.underReplicated}</div>
            </div>
            <div className="overview-item">
              <div className="overview-label">In Sync Replicas</div>
              <div className="overview-value">{partitions.inSyncReplicas} of {partitions.totalReplicas}</div>
            </div>
            <div className="overview-item">
              <div className="overview-label">Out Of Sync Replicas</div>
              <div className="overview-value">{partitions.outOfSyncReplicas}</div>
            </div>
            <div className="overview-item">
              <div className="overview-label">Controller Type</div>
              <div className="overview-value">{uptime.controllerType || '-'}</div>
            </div>
          </div>

          <div className="overview-table-wrap">
            <table className="data-table overview-table">
              <thead>
                <tr>
                  <th>Broker ID</th>
                  <th title="Kafka replica-log bytes reported by the Kafka Admin API">
                    Kafka Data Usage
                    <span className="overview-metric-level">Broker level</span>
                  </th>
                  <th title="OS filesystem containing the Kafka data directory; reported by the node agent">
                    Host Disk Usage
                    <span className="overview-metric-level">OS level</span>
                  </th>
                  <th>In sync replicas</th>
                  <th>Replicas</th>
                  <th>Replicas skew</th>
                  <th>Leaders</th>
                  <th>Leaders skew</th>
                  <th>Port</th>
                  <th>Host</th>
                </tr>
              </thead>
              <tbody>
                 {overview.brokers.map(broker => (
                  <tr key={broker.brokerId}>
                    <td>
                      <div className="overview-broker-id">
                        <CheckCircle2 size={24} color="#FFFFFF" fill="var(--color-success)" />
                        <span>{broker.brokerId}</span>
                      </div>
                    </td>
                    <td>{formatBytes(broker.diskUsageBytes)}</td>
                    <td>{formatHostDiskUsage(broker)}</td>
                    <td>{broker.inSyncReplicas}</td>
                    <td>{broker.replicas}</td>
                    <td>{formatSkew(broker.replicaSkewPct)}</td>
                    <td>{broker.leaders}</td>
                    <td>{formatSkew(broker.leaderSkewPct)}</td>
                    <td>{broker.port ? broker.port : '-'}</td>
                    <td>{broker.host}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      {uptime.controllerType === 'KRaft'
        && overview.controllers
        && overview.controllers.length > 0 && (
        <section className="overview-section">
          <div className="overview-band">
            <div className="section-header-row">
              <h2>Controller Voters</h2>
            </div>
            <div className="overview-table-wrap">
              <table className="data-table overview-table">
                <thead>
                  <tr>
                    <th>Node ID</th>
                    <th>Host</th>
                    <th>Port</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {overview.controllers.map(c => (
                    <tr key={c.nodeId}>
                      <td>
                        <div className="overview-broker-id">
                          <span>{c.nodeId}</span>
                        </div>
                      </td>
                      <td className="font-mono">{c.host}</td>
                      <td>{c.port ? c.port : '-'}</td>
                      <td>{c.activeLeader ? <span className="metric-status live">Active leader</span> : 'Voter'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </section>
      )}

      {overview.originType === 'EXTERNAL' && overview.nodePaths && (
        <section className="overview-section">
          <div className="overview-band">
            <div className="section-header-row">
              <h2>Paths & Directories</h2>
            </div>
            <div className="overview-table-wrap">
              <table className="data-table overview-table">
                <thead>
                  <tr>
                    <th>Node ID</th>
                    <th>Host</th>
                    <th>Role</th>
                    <th>Install Dir</th>
                    <th>Config</th>
                    <th>Data Dir</th>
                    <th>Log Dir</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {overview.nodePaths.map(p => (
                    <tr key={p.nodeId}>
                      <td>{p.nodeId}</td>
                      <td className="font-mono">{p.host}</td>
                      <td><span className="role-badge">{p.role}</span></td>
                      <td>{p.installDir || <span className="text-muted">Not reported</span>}</td>
                      <td>{p.config || <span className="text-muted">Not reported</span>}</td>
                      <td>{p.dataDir || <span className="text-muted">Not reported</span>}</td>
                      <td>{p.logDir || <span className="text-muted">Not reported</span>}</td>
                      <td>
                        {p.hasTelemetry ? (
                          <span className="text-green text-sm flex items-center gap-1"><CheckCircle2 size={14} /> Managed</span>
                        ) : (
                          <span className="text-muted text-sm">Bootstrap metadata</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </section>
      )}
    </div>
  );
}

function Notice({ kind, text }: { kind: 'error' | 'warning'; text: string }) {
  return (
    <div className={`overview-alert ${kind}`}>
      <AlertTriangle size={17} />
      <span>{text}</span>
    </div>
  );
}


function formatSkew(value: number | null) {
  if (value === null || value === undefined) return '-';
  if (value === 0) return '0%';
  return `${value > 0 ? '+' : ''}${value}%`;
}

function formatBytes(bytes: number) {
  if (!bytes) return '0 B';
  const sizes = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), sizes.length - 1);
  return `${(bytes / Math.pow(1024, i)).toFixed(i === 0 ? 0 : 2)} ${sizes[i]}`;
}

function formatDiskUsage(usedBytes: number | null | undefined, totalBytes: number | null | undefined) {
  if (usedBytes == null || !Number.isFinite(usedBytes) || usedBytes < 0) return '-';
  const used = formatBytes(usedBytes);
  return totalBytes != null && totalBytes > 0 ? `${used} / ${formatBytes(totalBytes)}` : used;
}

function formatHostDiskUsage(broker: BrokerRow) {
  if (broker.hostDiskMetricStatus !== 'LIVE' || broker.hostDiskUsedBytes == null || broker.hostDiskTotalBytes == null) {
    const label = broker.hostDiskMetricStatus === 'STALE' ? 'Stale' : 'Agent unavailable';
    const title = broker.hostDiskLastSeen ? `Last reported ${new Date(broker.hostDiskLastSeen).toLocaleString()}` : undefined;
    return <span className="metric-status stale" title={title}>{label}</span>;
  }
  return (
    <span>
      {formatDiskUsage(broker.hostDiskUsedBytes, broker.hostDiskTotalBytes)}{' '}
      <span className="metric-status live">Live</span>
    </span>
  );
}
