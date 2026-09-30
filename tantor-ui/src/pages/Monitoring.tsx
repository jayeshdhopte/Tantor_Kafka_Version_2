import { useCallback, useEffect, useMemo, useState } from 'react';
import { AlertTriangle, RefreshCw, Check, Server } from 'lucide-react';
import { Area, AreaChart, CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { CustomSelect } from '../components/CustomSelect';
import { AnchoredMenu } from '../components/AnchoredMenu';
import './Monitoring.css';

interface MonitoringNode {
  nodeId?: string | null;
  hostId?: string | null;
  hostname?: string | null;
  hostIp?: string | null;
  role?: string | null;
}

interface MonitoringCluster {
  id: string;
  name: string;
  originType: 'INTERNAL' | 'EXTERNAL' | string;
  monitoringEnabled: boolean;
  kafkaExporterTarget?: string;
  jmxAvailable?: boolean;
  warning?: string;
  nodes?: MonitoringNode[];
}

interface MonitoringOverview {
  clusterId: string;
  name: string;
  originType: string;
  prometheusUrl?: string;
  kafkaExporterTarget?: string;
  jmxAvailable?: boolean;
  kafkaExporterUp?: number | null;
  jmxUp?: number | null;
  kafkaExporterUpTargets?: number | null;
  kafkaExporterTotalTargets?: number | null;
  jmxUpTargets?: number | null;
  jmxTotalTargets?: number | null;
  brokerCount?: number | null;
  topicCount?: number | null;
  partitionCount?: number | null;
  underReplicatedPartitions?: number | null;
  consumerLag?: number | null;
  messagesInPerSecond?: number | null;
  bytesInPerSecond?: number | null;
  bytesOutPerSecond?: number | null;
  jvmHeapUsedPercent?: number | null;
  jvmHeapAvailableBytes?: number | null;
  jvmHeapTotalBytes?: number | null;
  brokerCpuPercent?: number | null;
  jvmProcessCpuPercent?: number | null;
  systemCpuPercent?: number | null;
  warnings?: string[];
  hostMemoryUsedPercent?: number | null;
  hostMemoryAvailableMb?: number | null;
  hostMemoryTotalMb?: number | null;
  selectedNodeId?: string | null;
  nodes?: MonitoringNode[];
}

interface MonitoringSample {
  time: string;
  brokers: number | null;
  topics: number | null;
  partitions: number | null;
  underReplicated: number | null;
  lag: number | null;
  messagesIn: number | null;
  bytesIn: number | null;
  bytesOut: number | null;
  heap: number | null;
  hostMemory: number | null;
  brokerCpu: number | null;
  systemCpu: number | null;
}

const formatNumber = (value?: number | null, digits = 0) => {
  if (value === undefined || value === null || Number.isNaN(value)) return 'N/A';
  return value.toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: digits });
};

const formatBytes = (value?: number | null) => {
  if (value === undefined || value === null || Number.isNaN(value)) return 'N/A';
  if (value <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let next = value;
  let unit = 0;
  while (next >= 1024 && unit < units.length - 1) {
    next /= 1024;
    unit += 1;
  }
  return `${next.toFixed(unit === 0 ? 0 : 2)} ${units[unit]}`;
};

const hasValue = (value?: number | null) => value !== undefined && value !== null && !Number.isNaN(value);

const chartNumber = (value?: number | null) => hasValue(value) ? Number(value) : null;

const nodeValue = (node: MonitoringNode) => String(node.nodeId || '');

const nodeLabel = (node: MonitoringNode) => {
  const nodeName = node.nodeId ? `Node ${node.nodeId}` : 'Node';
  const host = node.hostIp || node.hostname;
  const role = node.role;
  return [nodeName, role, host].filter(Boolean).join(' | ');
};

export function Monitoring() {
  const [selectedType, setSelectedType] = useState<'INTERNAL' | 'EXTERNAL'>('INTERNAL');
  const [clusters, setClusters] = useState<MonitoringCluster[]>([]);
  const [selectedClusterId, setSelectedClusterId] = useState('');
  const [selectedNodeId, setSelectedNodeId] = useState('');
  const [nodes, setNodes] = useState<{ value: string, label: string, role?: string | null }[]>([]);

  const [overview, setOverview] = useState<MonitoringOverview | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [refreshInterval, setRefreshInterval] = useState(10); // Default 10 seconds
  const [history, setHistory] = useState<MonitoringSample[]>([]);
  const [showIntervalDropdown, setShowIntervalDropdown] = useState(false);
  const [liveDropdownAnchor, setLiveDropdownAnchor] = useState<HTMLDivElement | null>(null);
  const selectedCluster = useMemo(() => clusters.find(c => c.id === selectedClusterId), [clusters, selectedClusterId]);

  // Load nodes when selectedClusterId changes
  useEffect(() => {
    if (!selectedClusterId || !selectedCluster) {
      Promise.resolve().then(() => {
        setNodes([]);
        setSelectedNodeId('');
      });
      return;
    }
    const formatted = (selectedCluster.nodes || [])
      .filter(node => Boolean(nodeValue(node)))
      .map(node => ({ value: nodeValue(node), label: nodeLabel(node), role: node.role }));
    Promise.resolve().then(() => {
      setNodes(formatted);
      setSelectedNodeId(current => formatted.some(node => node.value === current) ? current : (formatted[0]?.value || ''));
    });
  }, [selectedCluster, selectedClusterId]);


  // 1. Load clusters and hosts on mount
  const loadInitialData = useCallback(async () => {
    if (!selectedType) {
      setClusters([]);
      setSelectedClusterId('');
      return;
    }

    setLoading(true);
    setError(null);
    try {
      const clustersRes = await fetch(`/api/v1/monitoring/clusters?type=${selectedType}`);
      let clusterList: MonitoringCluster[] = [];
      if (clustersRes.ok) {
        clusterList = await clustersRes.json();
        setClusters(clusterList);
      }

      setSelectedClusterId(current =>
        clusterList.some(cluster => cluster.id === current)
          ? current
          : (clusterList[0]?.id || '')
      );
    } catch (err: unknown) {
      console.error(err);
      setError('Failed to load initial monitoring data.');
    } finally {
      setLoading(false);
    }
  }, [selectedType]);

  useEffect(() => {
    Promise.resolve().then(() => {
      setOverview(null);
      setHistory([]);
      setSelectedNodeId('');
    });
    void (async () => { await loadInitialData(); })();
  }, [selectedType, loadInitialData]);



  // Fetch overview metrics for the selected cluster
  const loadOverview = useCallback(async (silent = false) => {
    if (!selectedClusterId && !selectedCluster) return;

    if (!silent) setLoading(true);
    try {
      // 1. Fetch Prometheus Metrics
      const params = new URLSearchParams();
      if (selectedNodeId) {
        params.set('nodeId', selectedNodeId);
      }
      const query = params.toString();
      const res = await fetch(`/api/v1/monitoring/clusters/${selectedClusterId}/overview${query ? `?${query}` : ''}`);
      if (res.ok) {
        const data = await res.json();
        setOverview(data);
      }

    } catch (err) {
      console.error(err);
    } finally {
      if (!silent) setLoading(false);
    }
  }, [selectedClusterId, selectedNodeId, selectedCluster]);

  useEffect(() => {
    if (selectedClusterId) {
      Promise.resolve().then(() => setHistory([]));
      void (async () => { await loadOverview(); })();
    }
  }, [selectedClusterId, loadOverview]);

  // Append sample to history when overview updates
  useEffect(() => {
    if (!overview) return;
    Promise.resolve().then(() => {
      setHistory(current => {
      const next: MonitoringSample = {
        time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
        brokers: chartNumber(overview.brokerCount),
        topics: chartNumber(overview.topicCount),
        partitions: chartNumber(overview.partitionCount),
        underReplicated: chartNumber(overview.underReplicatedPartitions),
        lag: chartNumber(overview.consumerLag),
        messagesIn: chartNumber(overview.messagesInPerSecond),
        bytesIn: chartNumber(overview.bytesInPerSecond),
        bytesOut: chartNumber(overview.bytesOutPerSecond),
        heap: chartNumber(overview.jvmHeapUsedPercent),
        hostMemory: chartNumber(overview.hostMemoryUsedPercent),
        brokerCpu: chartNumber(overview.jvmProcessCpuPercent ?? overview.brokerCpuPercent),
        systemCpu: chartNumber(overview.systemCpuPercent),
      };
      return [...current, next].slice(-15); // keep 15 samples
      });
    });
  }, [overview]);

  // Auto refresh loop
  useEffect(() => {
    if (!autoRefresh || !selectedClusterId) return;
    const timer = window.setInterval(() => {
      if (!document.hidden) {
        loadOverview(true);
      }
    }, refreshInterval * 1000);
    return () => window.clearInterval(timer);
  }, [autoRefresh, refreshInterval, loadOverview, selectedClusterId]);

  // Mock initial history if empty to generate pretty graphs immediately
  const graphHistory = history;
  const clusterTitle = overview?.name || selectedCluster?.name || 'Select a cluster';
  const exporterTarget = overview?.kafkaExporterTarget || selectedCluster?.kafkaExporterTarget;
  const selectedNode = nodes.find(node => node.value === selectedNodeId);
  const kafkaExporterHealthy = hasValue(overview?.kafkaExporterTotalTargets)
    && Number(overview?.kafkaExporterTotalTargets) > 0
    && Number(overview?.kafkaExporterUpTargets) === Number(overview?.kafkaExporterTotalTargets);
  const kafkaExporterStatus = targetHealthStatus(
    overview?.kafkaExporterUpTargets,
    overview?.kafkaExporterTotalTargets
  );
  const jmxStatus = targetHealthStatus(overview?.jmxUpTargets, overview?.jmxTotalTargets);
  // Kafka Exporter reports broker/partition metrics, but it is not the Kafka
  // process itself. A live JMX target is also authoritative evidence that the
  // selected Kafka JVM is running, even while exporter deployment is degraded.
  const kafkaRunning = Boolean(overview) && (
    kafkaExporterHealthy
    || jmxStatus.state === 'up'
    || (overview?.brokerCount || 0) > 0
  );
  const warningMessages = [
    selectedCluster?.warning,
    ...(overview?.warnings || []),
  ].filter((message): message is string => Boolean(message && message.trim()));
  const selectedRole = selectedNode?.role || '';
  const controllerOnlySelected = selectedRole.toLowerCase().includes('controller')
    && !selectedRole.toLowerCase().includes('broker');
  const combinedRoleSelected = selectedRole.toLowerCase().includes('controller')
    && selectedRole.toLowerCase().includes('broker');
  const brokerMetricsApplicable = !controllerOnlySelected;
  const jvmRoleLabel = controllerOnlySelected
    ? 'Controller JVM'
    : (combinedRoleSelected ? 'Broker + Controller JVM' : 'Broker JVM');
  const jmxTargetLabel = controllerOnlySelected
    ? 'Controller JMX'
    : (combinedRoleSelected ? 'Broker + Controller JMX' : 'Broker JMX');
  const visibleWarnings = warningMessages.filter(message => !(
    controllerOnlySelected && message.toLowerCase().includes('kafka exporter')
  ));
  const displayCpuUsage = overview?.jvmProcessCpuPercent ?? overview?.brokerCpuPercent ?? overview?.systemCpuPercent;
  const displayMemoryUsage = overview?.jvmHeapUsedPercent ?? overview?.hostMemoryUsedPercent;
  const cpuUsageLabel = hasValue(overview?.jvmProcessCpuPercent ?? overview?.brokerCpuPercent)
    ? `${jvmRoleLabel} CPU`
    : 'Host CPU Usage';
  const memoryUsageLabel = hasValue(overview?.jvmHeapUsedPercent)
    ? `${jvmRoleLabel} Heap`
    : 'Host Memory Usage';

  return (
    <div className="monitoring-container animate-fade-in">
      <div className="monitoring-white-box">
        {/* Header Section */}
        <div className="header-section">
          <div className="title-area">
            <div>
              <h1>Monitoring</h1>
              <p className="subtitle">Real-time Kafka & system metrics</p>
            </div>
          </div>

          {/* Controls */}
          <div className="controls-area">
            <div className="monitoring-control-field monitoring-type-field">
              <span>Cluster type</span>
              <CustomSelect
                value={selectedType}
                onChange={val => {
                  setSelectedType(val as 'INTERNAL' | 'EXTERNAL');
                  setSelectedClusterId('');
                }}
                options={[
                  { value: 'INTERNAL', label: 'Internal' },
                  { value: 'EXTERNAL', label: 'External' },
                ]}
                width="154px"
                placeholder="Select Type"
              />
            </div>


            {/* CLUSTER NAME Selector */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
              <span style={{ fontSize: '11px', fontWeight: 'var(--font-bold)', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                Cluster Name
              </span>
              <CustomSelect
                value={selectedClusterId}
                onChange={val => {
                  setSelectedClusterId(val);
                  setSelectedNodeId('');
                  setOverview(null);
                  setHistory([]);
                }}
                options={clusters.length > 0 ? clusters.map(c => ({ value: c.id, label: c.name })) : [{ value: '', label: 'No clusters found' }]}
                width="160px"
                placeholder="Select Cluster"
              />
            </div>

            {/* NODE NAME Selector */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
              <span style={{ fontSize: '11px', fontWeight: 'var(--font-bold)', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                Node Name
              </span>
              <CustomSelect
                value={selectedNodeId}
                onChange={val => setSelectedNodeId(val)}
                options={nodes}
                width="min(360px, 100%)"
                placeholder="Select Node"
              />
            </div>

            {/* Live indicator Pill Box */}
            <div ref={setLiveDropdownAnchor} className="live-pill-dropdown-wrapper" style={{ height: '40px', display: 'flex', alignItems: 'center' }}>
              <div
                className={`live-pill-container ${autoRefresh ? 'active' : ''}`}
                onClick={() => setShowIntervalDropdown(!showIntervalDropdown)}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 'var(--space-2)',
                  background: '#F8FAFC',
                  border: '1px solid var(--border-subtle)',
                  borderRadius: 'var(--radius-md)',
                  padding: '8px 12px',
                  cursor: 'pointer',
                  userSelect: 'none',
                  height: '40px',
                  boxSizing: 'border-box'
                }}
              >
                <span style={{
                  width: '8px',
                  height: '8px',
                  borderRadius: '50%',
                  background: autoRefresh ? '#10B981' : '#94A3B8',
                  display: 'inline-block'
                }}></span>
                <span style={{ fontSize: 'var(--text-base)', fontWeight: 'var(--font-semibold)', color: '#334155' }}>Live</span>
                <div
                  style={{
                    width: '16px',
                    height: '16px',
                    borderRadius: '4px',
                    border: '1px solid #CBD5E1',
                    background: autoRefresh ? '#3B82F6' : '#fff',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    marginLeft: '4px'
                  }}
                >
                  {autoRefresh && <Check size={12} strokeWidth={3} color="#fff" />}
                </div>
              </div>

              {showIntervalDropdown && liveDropdownAnchor && (
                <AnchoredMenu
                  anchor={liveDropdownAnchor}
                  className="live-dropdown-menu"
                  onClose={() => setShowIntervalDropdown(false)}
                  align="start"
                  minWidth={180}
                >
                  {[5, 10, 15, 30, 60].map((sec) => (
                    <div
                      key={sec}
                      className={`live-dropdown-item ${refreshInterval === sec && autoRefresh ? 'selected' : ''}`}
                      onClick={() => {
                        setRefreshInterval(sec);
                        setAutoRefresh(true);
                        setShowIntervalDropdown(false);
                      }}
                    >
                      <span className="live-pill-dot active"></span>
                      Live | {sec} Sec
                    </div>
                  ))}
                  <div className="dropdown-divider" />
                  <div
                    className={`live-dropdown-item ${!autoRefresh ? 'paused' : ''}`}
                    onClick={() => {
                      setAutoRefresh(!autoRefresh);
                      setShowIntervalDropdown(false);
                    }}
                  >
                    <span className="live-pill-dot"></span>
                    {autoRefresh ? 'Pause Live Feed' : 'Resume Live Feed'}
                  </div>
                </AnchoredMenu>
              )}
            </div>

            {/* Refresh interval status display */}
            <div style={{
              display: 'flex',
              alignItems: 'center',
              background: '#F8FAFC',
              border: '1px solid var(--border-subtle)',
              borderRadius: 'var(--radius-md)',
              padding: '8px 16px',
              height: '40px',
              fontSize: 'var(--text-base)',
              fontWeight: 'var(--font-semibold)',
              color: '#334155',
              boxSizing: 'border-box'
            }}>
              {refreshInterval} Sec
            </div>

            {/* Manual Refresh Button */}
            <button
              className="manual-refresh-button"
              onClick={() => {
                if (selectedClusterId) {
                  loadOverview();
                } else {
                  loadInitialData();
                }
              }}
              disabled={loading}
              style={{
                height: '40px',
                width: '40px',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                border: '1px solid var(--border-subtle)',
                borderRadius: 'var(--radius-md)',
                background: "var(--bg-surface)",
                cursor: 'pointer'
              }}
            >
              <RefreshCw size={18} className={loading ? 'spin' : ''} />
            </button>
          </div>
        </div>

        {selectedClusterId === '' ? (
          <div className="monitoring-empty-state-card">
            <div className="monitoring-empty-illustration">
              <svg width="120" height="96" viewBox="0 0 120 96" fill="none" xmlns="http://www.w3.org/2000/svg">
                {/* Card 1 */}
                <g filter="url(#shadow-1)">
                  <rect x="10" y="2" width="100" height="24" rx="6" fill="white" stroke="var(--border-subtle)" strokeWidth="1.5" />
                  <rect x="22" y="12" width="16" height="4" rx="2" fill="var(--border-focus)" fillOpacity="0.4" />
                  <rect x="46" y="12" width="40" height="4" rx="2" fill="var(--border-focus)" fillOpacity="0.4" />
                </g>

                {/* Card 2 */}
                <g filter="url(#shadow-2)">
                  <rect x="10" y="34" width="100" height="24" rx="6" fill="white" stroke="var(--border-subtle)" strokeWidth="1.5" />
                  <rect x="22" y="44" width="36" height="4" rx="2" fill="var(--border-focus)" fillOpacity="0.4" />
                  <rect x="66" y="44" width="20" height="4" rx="2" fill="var(--border-focus)" fillOpacity="0.4" />
                </g>

                {/* Card 3 */}
                <g filter="url(#shadow-3)">
                  <rect x="10" y="66" width="100" height="24" rx="6" fill="white" stroke="var(--border-subtle)" strokeWidth="1.5" />
                  <rect x="22" y="76" width="12" height="4" rx="2" fill="var(--border-focus)" fillOpacity="0.4" />
                  <rect x="42" y="76" width="30" height="4" rx="2" fill="var(--border-focus)" fillOpacity="0.4" />
                </g>

                <defs>
                  <filter id="shadow-1" x="6" y="0" width="108" height="32" filterUnits="userSpaceOnUse" colorInterpolationFilters="sRGB">
                    <feDropShadow dx="0" dy="2" stdDeviation="2" floodColor="#0F172A" floodOpacity="0.04" />
                  </filter>
                  <filter id="shadow-2" x="6" y="32" width="108" height="32" filterUnits="userSpaceOnUse" colorInterpolationFilters="sRGB">
                    <feDropShadow dx="0" dy="2" stdDeviation="2" floodColor="#0F172A" floodOpacity="0.04" />
                  </filter>
                  <filter id="shadow-3" x="6" y="64" width="108" height="32" filterUnits="userSpaceOnUse" colorInterpolationFilters="sRGB">
                    <feDropShadow dx="0" dy="2" stdDeviation="2" floodColor="#0F172A" floodOpacity="0.04" />
                  </filter>
                </defs>
              </svg>
            </div>
            <h2>Select a cluster to monitor</h2>
            <p>Choose a cluster from the dropdown above to display its real-time metrics and nodes.</p>
          </div>
        ) : (
          <>
            {error && (
              <div className="error-banner">
                <p>{error}</p>
              </div>
            )}

            {visibleWarnings.length > 0 && (
              <div className="monitoring-warning-list">
                {visibleWarnings.map(message => (
                  <div className="monitoring-warning" key={message}>
                    <AlertTriangle size={16} />
                    <span>{message}</span>
                  </div>
                ))}
              </div>
            )}

            {/* Broker Details Header Card */}
            <div className="broker-details-card">
              <div style={{ display: 'flex', alignItems: 'center', gap: '18px' }}>
                <Server size={28} color="var(--accent-primary)" style={{ flexShrink: 0 }} />
                <div className="broker-info">
                  <h2 style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                    {clusterTitle}
                    <span className={`cluster-source-tag ${kafkaRunning ? 'state-positive' : 'state-negative'}`}>
                      {selectedType === 'INTERNAL' ? 'Internal' : 'External'}
                    </span>
                  </h2>
                  <p className="broker-meta">
                    {[exporterTarget?.split(':')[0], selectedNode?.label].filter(Boolean).join(' | ') || 'Monitoring endpoint unavailable'}
                  </p>
                </div>
              </div>
              <div className="monitoring-status-right">
                <span className={`kafka-running-badge ${kafkaRunning ? 'state-positive' : 'state-negative'}`}>
                  <span className="status-dot"></span>
                  {kafkaRunning ? 'Kafka running' : 'Kafka offline'}
                </span>
              </div>
            </div>

            <div className="monitoring-data-panel">
              {/* Real-time Performance Section */}
              <div className="section-header-row">
                <div className="performance-header-left">
                  <h3>Real-time Performance</h3>
                  <span className="live-performance-badge">
                    Live
                  </span>
                </div>
                <div className="monitoring-status-pills-row">
                  {brokerMetricsApplicable && (
                    <span className={`monitoring-connection-pill ${kafkaExporterStatus.state}`}>
                      {targetHealthLabel('Kafka Exporter', kafkaExporterStatus)}
                    </span>
                  )}
                  <span className={`monitoring-connection-pill ${jmxStatus.state}`}>
                    {targetHealthLabel(jmxTargetLabel, jmxStatus)}
                  </span>
                </div>
              </div>

              <div className="performance-section-card">
                <div className="charts-grid-row">
                  {/* CPU Usage Chart */}
                  <div className="chart-box-wrapper">
                    <div className="chart-box-header">
                      <span>{cpuUsageLabel}</span>
                      <span className="chart-stat-value green">
                        {hasValue(displayCpuUsage) ? `${formatNumber(displayCpuUsage, 1)}%` : 'N/A'}
                      </span>
                    </div>
                    <div className="chart-body-container">
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={graphHistory} margin={{ top: 5, right: 5, left: -25, bottom: 5 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                          <XAxis dataKey="time" tick={{ fontSize: 9, fill: '#94a3b8' }} />
                          <YAxis tick={{ fontSize: 9, fill: '#94a3b8' }} domain={[0, 100]} />
                          <Tooltip contentStyle={{ fontSize: '11px', borderRadius: '6px' }} />
                          <Line type="monotone" dataKey="brokerCpu" stroke="#3b82f6" strokeWidth={1.5} dot={false} activeDot={{ r: 4 }} />
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                  </div>

                  {/* Memory Usage Chart */}
                  <div className="chart-box-wrapper">
                    <div className="chart-box-header">
                      <span>{memoryUsageLabel}</span>
                      <span className="chart-stat-value green">
                        {hasValue(displayMemoryUsage) ? `${formatNumber(displayMemoryUsage, 1)}%` : 'N/A'}
                      </span>
                    </div>
                    <div className="chart-body-container">
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={graphHistory} margin={{ top: 5, right: 5, left: -25, bottom: 5 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                          <XAxis dataKey="time" tick={{ fontSize: 9, fill: '#94a3b8' }} />
                          <YAxis tick={{ fontSize: 9, fill: '#94a3b8' }} domain={[0, 100]} />
                          <Tooltip contentStyle={{ fontSize: '11px', borderRadius: '6px' }} />
                          <Line type="monotone" dataKey="heap" stroke="#10b981" strokeWidth={1.5} dot={false} activeDot={{ r: 4 }} />
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                  </div>

                  {/* Messages In is a broker metric and does not apply to controller-only nodes. */}
                  {brokerMetricsApplicable && (
                    <div className="chart-box-wrapper">
                      <div className="chart-box-header">
                        <span>Messages In</span>
                        <span className="chart-stat-value red">
                          {hasValue(overview?.messagesInPerSecond) ? `${formatNumber(overview?.messagesInPerSecond, 1)}/s` : 'N/A'}
                        </span>
                      </div>
                      <div className="chart-body-container">
                        <ResponsiveContainer width="100%" height="100%">
                          <AreaChart data={graphHistory} margin={{ top: 5, right: 5, left: -25, bottom: 5 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                            <XAxis dataKey="time" tick={{ fontSize: 9, fill: '#94a3b8' }} />
                            <YAxis tick={{ fontSize: 9, fill: '#94a3b8' }} />
                            <Tooltip contentStyle={{ fontSize: '11px', borderRadius: '6px' }} />
                            <Area type="monotone" dataKey="messagesIn" stroke="#c084fc" fill="#f3e8ff" strokeWidth={1.5} />
                          </AreaChart>
                        </ResponsiveContainer>
                      </div>
                    </div>
                  )}
                </div>
              </div>

              {/* Kafka broker traffic is not exposed by controller-only nodes. */}
              {brokerMetricsApplicable && (
                <div className="monitoring-bottom-section">
                  <h3 className="monitoring-section-title-custom">Kafka Broker</h3>
                  <div className="monitoring-kpi-row">
                  <div className="kpi-card-box">
                    <span className="kpi-card-label">MSG IN/Sec</span>
                    <strong className="kpi-card-val">{formatNumber(overview?.messagesInPerSecond, 2)}</strong>
                  </div>
                  <div className="kpi-card-box">
                    <span className="kpi-card-label">Bytes IN/Sec</span>
                    <strong className="kpi-card-val">{formatBytes(overview?.bytesInPerSecond)}</strong>
                  </div>
                  <div className="kpi-card-box">
                    <span className="kpi-card-label">Bytes Out/Sec</span>
                    <strong className="kpi-card-val">{formatBytes(overview?.bytesOutPerSecond)}</strong>
                  </div>
                  <div className="kpi-card-box">
                    <span className="kpi-card-label">Partition</span>
                    <strong className="kpi-card-val">{formatNumber(overview?.partitionCount)}</strong>
                  </div>
                  <div className="kpi-card-box">
                    <span className="kpi-card-label">Under-replication</span>
                    <strong className="kpi-card-val">{formatNumber(overview?.underReplicatedPartitions)}</strong>
                  </div>
                  <div className="kpi-card-box">
                    <span className="kpi-card-label">Consumer Lag</span>
                    <strong className="kpi-card-val">{formatNumber(overview?.consumerLag)}</strong>
                  </div>
                  <div className="kpi-card-box">
                    <span className="kpi-card-label">Broker</span>
                    <strong className="kpi-card-val">{formatNumber(overview?.brokerCount)}</strong>
                  </div>
                  <div className="kpi-card-box">
                    <span className="kpi-card-label">Topics</span>
                    <strong className="kpi-card-val">{formatNumber(overview?.topicCount)}</strong>
                  </div>
                  </div>
                </div>
              )}

              {/* System Resources Section */}
              <div className="monitoring-bottom-section">
                <h3 className="monitoring-section-title-custom">System Resources</h3>
                <div className="resources-cards-grid">
                  <ResourceCard
                    label={`${jvmRoleLabel} CPU`}
                    value={overview?.jvmProcessCpuPercent ?? overview?.brokerCpuPercent}
                    tone="purple"
                    subtext={availablePercentText(overview?.jvmProcessCpuPercent ?? overview?.brokerCpuPercent)}
                  />
                  <ResourceCard
                    label="System CPU"
                    value={overview?.systemCpuPercent}
                    tone="green"
                    subtext={availablePercentText(overview?.systemCpuPercent)}
                  />
                  <ResourceCard
                    label="JVM Heap"
                    value={overview?.jvmHeapUsedPercent}
                    tone="purple"
                    subtext={hasValue(overview?.jvmHeapAvailableBytes)
                      ? availableCapacityText(
                          Number(overview?.jvmHeapAvailableBytes),
                          overview?.jvmHeapTotalBytes
                        )
                      : undefined}
                  />
                  <ResourceCard
                    label="Host Memory (Agent Heartbeat)"
                    value={overview?.hostMemoryUsedPercent}
                    tone="blue"
                    subtext={hasValue(overview?.hostMemoryAvailableMb)
                      ? availableCapacityText(
                          Number(overview?.hostMemoryAvailableMb) * 1024 * 1024,
                          hasValue(overview?.hostMemoryTotalMb)
                            ? Number(overview?.hostMemoryTotalMb) * 1024 * 1024
                            : undefined
                        )
                      : undefined}
                  />
                </div>
              </div>
            </div>
          </>
        )}

      </div>
    </div>
  );
}

const boundedPercent = (value?: number | null) => {
  if (!hasValue(value)) return 0;
  return Math.max(0, Math.min(100, Number(value)));
};

const availablePercentText = (usedPercent?: number | null) => {
  if (!hasValue(usedPercent)) return undefined;
  return `${formatNumber(100 - boundedPercent(usedPercent), 1)}% available`;
};

const availableCapacityText = (availableBytes: number, totalBytes?: number | null) => {
  const available = `${formatBytes(availableBytes)} available`;
  return hasValue(totalBytes) && Number(totalBytes) > 0
    ? `${available} / ${formatBytes(Number(totalBytes))} total`
    : available;
};

type TargetHealthState = 'up' | 'degraded' | 'down' | 'unavailable';

const targetHealthStatus = (up?: number | null, total?: number | null): {
  up: number;
  total: number;
  state: TargetHealthState;
} => {
  if (!hasValue(total) || Number(total) <= 0) {
    return { up: 0, total: 0, state: 'unavailable' };
  }
  const totalCount = Math.max(0, Number(total));
  const upCount = hasValue(up) ? Math.max(0, Number(up)) : 0;
  if (upCount >= totalCount) return { up: upCount, total: totalCount, state: 'up' };
  if (upCount > 0) return { up: upCount, total: totalCount, state: 'degraded' };
  return { up: 0, total: totalCount, state: 'down' };
};

const targetHealthLabel = (name: string, health: ReturnType<typeof targetHealthStatus>) => {
  if (health.state === 'unavailable') return `${name} N/A`;
  if (health.state === 'down') return `${name} DOWN`;
  return `${name} ${health.up}/${health.total} UP`;
};

function ResourceCard({ label, value, subtext, tone = 'blue' }: {
  label: string;
  value?: number | null;
  subtext?: string;
  tone?: 'blue' | 'green' | 'purple';
}) {
  const percent = boundedPercent(value);
  return (
    <div className="resource-card">
      <div className="resource-card-header">
        <span className="resource-card-label">{label}</span>
        <strong className="resource-card-value">
          {hasValue(value) ? `${formatNumber(value, 1)}%` : 'N/A'}
        </strong>
      </div>
      <div className="progress-track-bg">
        <div className={`progress-fill-bar ${tone}`} style={{ width: `${percent}%` }} />
      </div>
      {subtext && <span className="resource-subtitle-info">{subtext}</span>}
    </div>
  );
}

