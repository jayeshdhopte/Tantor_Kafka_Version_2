import { useState, useEffect, useCallback } from 'react';
import { useParams } from 'react-router-dom';
import { Search } from 'lucide-react';
import './Brokers.css';

type RoleFilter = 'all' | 'broker' | 'controller' | 'broker_controller';

interface Broker {
  brokerId: number;
  hostname: string;
  role: string;
  brokerHealth: string; // HEALTHY | DEGRADED | OFFLINE
  controller: boolean;
  jmxReachable: boolean;
  cpuUsagePct: number | null;
  memoryUsedMb: number | null;
  memoryTotalMb: number | null;
  diskUsedGb: number | null;
  diskTotalGb: number | null;
  diskUsedBytes: number | null;
  diskTotalBytes: number | null;
  hostMetricStatus: 'LIVE' | 'STALE' | 'UNAVAILABLE';
  messagesInPerSec: number;
  bytesInPerSec: number;
  lastHeartbeat: string;
}

export function Brokers() {
  const { id } = useParams<{ id: string }>();
  const [brokers, setBrokers] = useState<Broker[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sortField, setSortField] = useState<keyof Broker>('brokerId');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('asc');
  const [roleFilter, setRoleFilter] = useState<RoleFilter>('all');
  const [search, setSearch] = useState('');

  const fetchBrokers = useCallback(async () => {
    try {
      const res = await fetch(`/api/v1/clusters/${id}/brokers`);
      if (!res.ok) throw new Error('Failed to fetch brokers');
      setBrokers(await res.json());
      setError(null);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to fetch brokers');
    } finally {
      setLoading(false);
    }
  }, [id]);

  useEffect(() => {
    void (async () => { await fetchBrokers(); })();
    const interval = setInterval(() => { void (async () => { await fetchBrokers(); })(); }, 10000);
    return () => clearInterval(interval);
  }, [fetchBrokers]);

  const handleSort = (field: keyof Broker) => {
    if (sortField === field) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortOrder('asc');
    }
  };

  const normalizeRole = (broker: Broker): Exclude<RoleFilter, 'all'> | 'unknown' => {
    const roleParts = String(broker.role || '')
      .toLowerCase()
      .split(/[_\s,+/-]+/)
      .filter(Boolean);
    const isBroker = roleParts.includes('broker');
    const isController = roleParts.includes('controller') || broker.controller;

    if (isBroker && isController) return 'broker_controller';
    if (isController) return 'controller';
    if (isBroker) return 'broker';
    return 'unknown';
  };

  const matchesRoleFilter = (broker: Broker, filter: RoleFilter) =>
    filter === 'all' || normalizeRole(broker) === filter;

  const formatRole = (role: string) => role
    .split(/[_-]+/)
    .filter(Boolean)
    .map(part => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' + ');

  const filteredBrokers = brokers
    .filter(b => matchesRoleFilter(b, roleFilter))
    .filter(b =>
      b.hostname.toLowerCase().includes(search.toLowerCase()) ||
      b.brokerId.toString().includes(search)
    )
    .sort((a, b) => {
      const aVal = a[sortField];
      const bVal = b[sortField];
      if (typeof aVal === 'string' && typeof bVal === 'string')
        return sortOrder === 'asc' ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
      return sortOrder === 'asc'
        ? (aVal as number) - (bVal as number)
        : (bVal as number) - (aVal as number);
    });

  const brokerNodes = brokers.filter(broker => {
    const normalizedRole = normalizeRole(broker);
    return normalizedRole === 'broker' || normalizedRole === 'broker_controller';
  });
  const agg = {
    totalMsgIn: brokerNodes.reduce((s, b) => s + (b.messagesInPerSec || 0), 0),
    totalBytesIn: brokerNodes.reduce((s, b) => s + (b.bytesInPerSec || 0), 0),
    avgCpu: (() => {
      const liveCpu = brokers.filter(b => b.hostMetricStatus === 'LIVE' && b.cpuUsagePct != null);
      return liveCpu.reduce((s, b) => s + (b.cpuUsagePct || 0), 0) / (liveCpu.length || 1);
    })(),
    offline: brokerNodes.filter(b => b.brokerHealth === 'OFFLINE').length,
  };

  const formatBytes = (bytes: number) => {
    if (!Number.isFinite(bytes) || bytes <= 0) return '0 B';
    if (bytes < 1) return `${parseFloat(bytes.toFixed(2))} B`;
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.min(Math.floor(Math.log(bytes) / Math.log(k)), sizes.length - 1);
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const formatMessageRate = (messages: number) => {
    if (!Number.isFinite(messages) || messages <= 0) return '0';
    if (messages < 1) return parseFloat(messages.toFixed(2)).toString();
    if (messages < 10) return parseFloat(messages.toFixed(1)).toString();
    return messages.toFixed(0);
  };

  const ProgressBar = ({ value, max }: { value: number; max: number }) => {
    const pct = max > 0 ? Math.min(100, Math.max(0, (value / max) * 100)) : 0;
    return (
      <div
        className="progress-bar-container"
        title={`${value.toFixed(1)} / ${max.toFixed(1)}`}
      >
        <div className="progress-bar-fill" style={{ width: `${pct}%` }} />
      </div>
    );
  };

  if (loading && brokers.length === 0) {
    return <div className="state-center">Loading role metrics…</div>;
  }

  return (
    <div className="brokers-dashboard animate-fade-in">

      {/* ── Overview KPI cards ── */}
      <div className="brokers-overview-gradient">
        <div className="metric-card figma-card">
          <div className="card-header">
            <span className="label">Total Ingestion</span>
          </div>
          <div className="card-body">
            <span className="value">{formatBytes(agg.totalBytesIn)}/s</span>
            <span className="subtext">{formatMessageRate(agg.totalMsgIn)} msg/s</span>
          </div>
        </div>

        <div className="metric-card figma-card">
          <div className="card-header">
            <span className="label">Active Brokers</span>
          </div>
          <div className="card-body">
            <span className="value">{brokerNodes.length - agg.offline}/{brokerNodes.length}</span>
            <span className="subtext">{agg.offline} Offline Nodes</span>
          </div>
        </div>

        <div className="metric-card figma-card">
          <div className="card-header">
            <span className="label">Avg. Cluster CPU</span>
          </div>
          <div className="card-body">
            <span className="value">{agg.avgCpu.toFixed(1)}%</span>
          </div>
        </div>
      </div>

      {/* ── Controls ── */}
      <div className="brokers-list-header">
        <h2 className="brokers-list-title cluster-section-heading">Roles List</h2>
        <div className="brokers-controls figma-controls">
          <div className="search-wrapper">
            <Search size={16} className="search-icon" />
            <input
              type="text"
              placeholder="Search hostname or ID..."
              value={search}
              onChange={e => setSearch(e.target.value)}
              className="search-input"
            />
          </div>
          <select
            value={roleFilter}
            onChange={event => setRoleFilter(event.target.value as RoleFilter)}
            className="role-select"
          >
            <option value="all">All Roles</option>
            <option value="broker">Broker Only</option>
            <option value="controller">Controller Only</option>
            <option value="broker_controller">Broker + Controller</option>
          </select>
        </div>
      </div>

      {/* ── Error ── */}
      {error && <div className="error-alert">{error}</div>}

      {/* ── Table ── */}
      <div className="brokers-table-container">
        <table className="data-table">
          <thead>
            <tr>
              <th onClick={() => handleSort('brokerId')} className="sortable">
                ID
              </th>
              <th onClick={() => handleSort('hostname')} className="sortable">
                Host IP
              </th>
              <th>Role</th>
              <th onClick={() => handleSort('cpuUsagePct')} className="sortable">
                CPU
              </th>
              <th onClick={() => handleSort('memoryUsedMb')} className="sortable">
                RAM
              </th>
              <th onClick={() => handleSort('diskUsedBytes')} className="sortable" title="OS filesystem containing the Kafka data directory; reported by the node agent">
                Host Disk <span className="broker-metric-level">OS level</span>
              </th>

              <th>Last Update</th>
            </tr>
          </thead>
          <tbody>
            {filteredBrokers.map(broker => (
              <tr key={broker.brokerId}>

                {/* ID */}
                <td>
                  <div className="broker-id-cell">
                    <span>{broker.brokerId}</span>
                  </div>
                </td>

                {/* Hostname */}
                <td className="font-mono">{broker.hostname}</td>

                {/* Role */}
                <td>
                  <span className="role-badge">
                    {normalizeRole(broker) === 'broker_controller'
                      ? 'Broker + Controller'
                      : formatRole(broker.role)}
                  </span>
                </td>

                {/* CPU */}
                <td>
                  {broker.hostMetricStatus === 'LIVE' && broker.cpuUsagePct != null ? (
                    <div className="metric-cell figma-metric">
                      <span className="metric-val">{broker.cpuUsagePct.toFixed(1)}%</span>
                      <ProgressBar value={broker.cpuUsagePct} max={100} />
                    </div>
                  ) : <MetricUnavailable status={broker.hostMetricStatus} lastHeartbeat={broker.lastHeartbeat} />}
                </td>

                {/* RAM */}
                <td>
                  {broker.hostMetricStatus === 'LIVE' && broker.memoryUsedMb != null && broker.memoryTotalMb != null ? (
                    <div className="metric-cell figma-metric">
                      <span className="metric-val">{formatBytes(broker.memoryUsedMb * 1024 * 1024)}</span>
                      <ProgressBar value={broker.memoryUsedMb} max={broker.memoryTotalMb} />
                    </div>
                  ) : <MetricUnavailable status={broker.hostMetricStatus} lastHeartbeat={broker.lastHeartbeat} />}
                </td>

                {/* Disk */}
                <td>
                  {broker.hostMetricStatus === 'LIVE' && broker.diskUsedBytes != null && broker.diskTotalBytes != null ? (
                    <div className="metric-cell figma-metric">
                      <span className="metric-val">{formatBytes(broker.diskUsedBytes)} / {formatBytes(broker.diskTotalBytes)}</span>
                      <ProgressBar value={broker.diskUsedBytes} max={broker.diskTotalBytes} />
                    </div>
                  ) : <MetricUnavailable status={broker.hostMetricStatus} lastHeartbeat={broker.lastHeartbeat} />}
                </td>



                {/* Heartbeat */}
                <td className="text-muted text-sm">
                  {broker.lastHeartbeat ? new Date(broker.lastHeartbeat).toLocaleTimeString() : '-'}
                </td>

              </tr>
            ))}

            {filteredBrokers.length === 0 && (
              <tr>
                <td colSpan={7} className="text-center py-4 text-muted">
                  No roles found matching criteria
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

    </div>
  );
}

function MetricUnavailable({ status, lastHeartbeat }: { status: Broker['hostMetricStatus']; lastHeartbeat: string | null }) {
  const label = status === 'STALE' ? 'Stale' : 'Agent unavailable';
  const title = lastHeartbeat ? `Last reported ${new Date(lastHeartbeat).toLocaleString()}` : undefined;
  return <span className="broker-metric-status stale" title={title}>{label}</span>;
}
