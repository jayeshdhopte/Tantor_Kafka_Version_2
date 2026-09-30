import { useCallback, useEffect, useMemo, useState } from 'react';
import { createPortal } from 'react-dom';
import { useNavigate } from 'react-router-dom';
import {
  Activity, AlertTriangle, ExternalLink, Info, Network, Plus, RefreshCw, Server, X, FileCheck, FileText, FileCode,
  type LucideIcon
} from 'lucide-react';
import {
  Bar, BarChart, CartesianGrid, Cell, Legend, Pie, PieChart, Line, LineChart,
  ResponsiveContainer, Tooltip, XAxis, YAxis, type LegendProps
} from 'recharts';
import { usePermissions } from '../hooks/usePermissions';
import { clusterStatusTone } from '../utils/clusterStatusTone';
import './Dashboard.css';

interface DashboardSummary {
  totalHosts: number;
  activeHosts: number;
  offlineHosts: number;
  pendingHosts: number;
  totalClusters: number;
  activeClusters: number;
  failedClusters: number;
  externalClusters: number;
  internalClusters: number;
  activeAlerts: number;
  runningTasks: number;
  failedTasks: number;
  activeParcels: number;
  failedParcels: number;
  runningServices: number;
  failedServices: number;
  firstClusterCreatedAt?: string;
  latestClusterCreatedAt?: string;
  lastActivityAt?: string;
}

interface ChartRow {
  name: string;
  hostId?: string;
  status?: string;
  value?: number;
  usedGb?: number;
  freeGb?: number;
  totalGb?: number;
  usedPct?: number;
  success?: number;
  failed?: number;
  running?: number;
  label?: string;
}

interface ServiceRow {
  name: string;
  description: string;
  status: string;
  type: string;
}

interface ClusterHealthRow {
  id: string;
  name: string;
  mode?: string;
  kafkaVersion?: string;
  source?: string;
  status: string;
  reason: string;
  hostCount?: number;
  bootstrapServers?: string;
}

interface ActivityRow {
  id: string;
  level: string;
  message: string;
  clusterId?: string;
  createdAt?: string;
}

interface TaskRow {
  id: string;
  command: string;
  status: string;
  hostId: string;
  clusterName?: string;
  createdAt?: string;
  updatedAt?: string;
  errorMsg?: string;
}

interface DashboardPayload {
  generatedAt: string;
  summary: DashboardSummary;
  hostStatus: ChartRow[];
  clusterStatus: ChartRow[];
  clusterHealth: ClusterHealthRow[];
  hostDiskUsage: ChartRow[];
  taskStatus: ChartRow[];
  taskTimeline: ChartRow[];
  runningServices: ServiceRow[];
  failedServices: ServiceRow[];
  recentActivities: ActivityRow[];
  recentTasks: TaskRow[];
}

const emptyDashboard: DashboardPayload = {
  generatedAt: '',
  summary: {
    totalHosts: 0,
    activeHosts: 0,
    offlineHosts: 0,
    pendingHosts: 0,
    totalClusters: 0,
    activeClusters: 0,
    failedClusters: 0,
    externalClusters: 0,
    internalClusters: 0,
    activeAlerts: 0,
    runningTasks: 0,
    failedTasks: 0,
    activeParcels: 0,
    failedParcels: 0,
    runningServices: 0,
    failedServices: 0,
  },
  hostStatus: [],
  clusterStatus: [],
  clusterHealth: [],
  hostDiskUsage: [],
  taskStatus: [],
  taskTimeline: [],
  runningServices: [],
  failedServices: [],
  recentActivities: [],
  recentTasks: [],
};

const summarizeError = (message: string, limit = 170) => {
  const clean = message
    .replace(/\s*Logs:\s*==>.*/is, '')
    .replace(/\s+/g, ' ')
    .trim();

  return clean.length <= limit ? clean : `${clean.slice(0, limit).trimEnd()}...`;
};

const STATUS_COLORS: Record<string, string> = {
  SUCCESS: '#2AC792',
  ONLINE: '#2AC792',
  RUNNING: '#378ADD',
  IN_PROGRESS: '#378ADD',
  PENDING: '#BA7517',
  DELETING: '#BA7517',
  OFFLINE: '#E02424',
  FAILED: '#E02424',
  UNKNOWN: '#8b8982',
};

interface TaskLegendPayload {
  color?: string;
  value?: React.ReactNode;
}

const renderTaskLegend = (props: LegendProps & { payload?: readonly TaskLegendPayload[] }) => {
  const { payload = [] } = props;
  return (
    <div className="task-legend">
      {payload.map((entry, index) => (
        <span key={`item-${index}`} className="task-legend-item">
          <i style={{ background: entry.color }} />
          {entry.value}
        </span>
      ))}
    </div>
  );
};

import { useAuth } from '../contexts/useAuth';
import { ClusterDeployment } from './ClusterDeployment';

export function Dashboard() {
  const navigate = useNavigate();
  const { decodedToken } = useAuth();
  const { canManage } = usePermissions();
  const [dashboard, setDashboard] = useState<DashboardPayload>(emptyDashboard);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [showDeploymentModal, setShowDeploymentModal] = useState(false);
  const [deploymentStep, setDeploymentStep] = useState<'choice' | 'deploy'>('choice');
  const [serviceTab, setServiceTab] = useState<'running' | 'failed'>('running');
  const [taskTab, setTaskTab] = useState<'success' | 'failed'>('success');
  const [openInfo, setOpenInfo] = useState<'overview' | 'cluster-health' | null>(null);

  // Capitalize the first letter of username
  const username = useMemo(() => {
    const rawName = decodedToken?.preferred_username || decodedToken?.name || 'User';
    return rawName.charAt(0).toUpperCase() + rawName.slice(1);
  }, [decodedToken]);

  const fetchDashboard = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const res = await fetch('/api/v1/ui/dashboard');
      if (!res.ok) throw new Error(`Dashboard request failed (${res.status})`);
      setDashboard(await res.json());
    } catch (error: unknown) {
      setError(error instanceof Error ? error.message : 'Failed to load dashboard');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    const initial = window.setTimeout(() => void fetchDashboard(), 0);
    const interval = window.setInterval(() => void fetchDashboard(), 10000);
    return () => {
      window.clearTimeout(initial);
      window.clearInterval(interval);
    };
  }, [fetchDashboard]);

  const summary = dashboard.summary;


  const kpis = useMemo(() => [
    {
      label: 'Active Hosts',
      value: `${summary.activeHosts.toString().padStart(2, '0')}`,
      detail: `${summary.offlineHosts} Offline | ${summary.pendingHosts} Pending`,
      icon: Server,
      tone: 'cyan',
    },
    {
      label: 'Clusters',
      value: `${summary.totalClusters.toString().padStart(2, '0')}`,
      detail: `${summary.internalClusters} Internal | ${summary.externalClusters} External`,
      icon: Network,
      tone: 'purple',
    },
    {
      label: 'External Clusters',
      value: `${summary.externalClusters.toString().padStart(2, '0')}`,
      detail: `${summary.externalClusters} External`,
      icon: ExternalLink,
      tone: 'indigo',
    },
    {
      label: 'Failed Service',
      value: `${summary.failedServices.toString().padStart(2, '0')}`,
      detail: `${summary.failedServices} Failed | ${summary.failedParcels} Issues`,
      icon: AlertTriangle,
      tone: 'pink',
    },
    {
      label: 'Running Service',
      value: `${summary.runningServices.toString().padStart(2, '0')}`,
      detail: `${summary.activeParcels} Active parcel`,
      icon: Activity,
      tone: 'green',
    },
  ], [summary]);

  const serviceIcon = () => {
    return FileCheck;
  };

  return (
    <div className="db animate-fade-in">
      <header className="db-hero">
        <div>
          <div>
            <h1><span aria-hidden="true">{'\u{1F44B}'}</span> Welcome {username}!</h1>
            <p className="db-subtitle-wrap">
              Dashboard overview
              <span className="db-info-wrap">
                <button
                  type="button"
                  className="db-info-button hero"
                  aria-label="About the dashboard overview"
                  aria-expanded={openInfo === 'overview'}
                  onClick={() => setOpenInfo(current => current === 'overview' ? null : 'overview')}
                >
                  <Info size={14} />
                </button>
                {openInfo === 'overview' && (
                  <span className="db-info-popover overview" role="status">
                    A summary of your Kafka environment, hosts, clusters, and service status.
                  </span>
                )}
              </span>
            </p>
          </div>
          <div className="db-hero-actions">
            <span className="db-generated">Last update: {relativeTime(dashboard.generatedAt)}</span>
            <button className="db-btn ghost" onClick={fetchDashboard} aria-label="Refresh dashboard" title="Refresh">
              <RefreshCw size={14} className={loading ? 'spin' : ''} />
            </button>
            {canManage && (
              <button className="db-btn primary" onClick={() => { setDeploymentStep('choice'); setShowDeploymentModal(true); }}>
                <Plus size={14} />
                New Cluster
              </button>
            )}
          </div>
        </div>

        <div className="db-kpi-grid">
          {kpis.map(kpi => (
            <article key={kpi.label} className={`db-kpi-card ${kpi.tone}`}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)' }}>
                <div className="db-kpi-icon"><kpi.icon size={18} /></div>
                <span>{kpi.label}</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', marginTop: '12px' }}>
                <strong>{kpi.value}</strong>
                <small>{kpi.detail}</small>
              </div>
            </article>
          ))}
        </div>
      </header>

      {error && <div className="db-banner error">{error}</div>}

      <section className="db-cluster-health">
        <PanelTitle title="Cluster Health" detail={(
          <span className="db-info-wrap">
            <button
              type="button"
              className="db-info-button neutral"
              aria-label="About cluster health"
              aria-expanded={openInfo === 'cluster-health'}
              onClick={() => setOpenInfo(current => current === 'cluster-health' ? null : 'cluster-health')}
            >
              <Info size={16} />
            </button>
            {openInfo === 'cluster-health' && (
              <span className="db-info-popover cluster-health" role="status">
                Shows the current health and status of each configured Kafka cluster.
              </span>
            )}
          </span>
        )} />
        {dashboard.clusterHealth.length ? (
          <div className="db-cluster-list">
            {dashboard.clusterHealth.map(cluster => (
              <button
                key={cluster.id}
                className={`db-cluster-card ${healthTone(cluster.status)}`}
                onClick={() => navigate(`/clusters/${cluster.id}`)}
              >
                <span className="db-cluster-dot" />
                <div>
                  <strong>{cluster.name || 'Unnamed cluster'}</strong>
                  <small>
                    {cluster.source || 'Cluster'} - Kafka {cluster.kafkaVersion || '-'}
                  </small>
                  <em>{cluster.reason}</em>
                </div>
                <b>{statusLabel(cluster.status)}</b>
              </button>
            ))}
          </div>
        ) : (
          <EmptyPanel text="No cluster records yet." compact />
        )}
      </section>

      <section className="db-main-grid">
        <article className="db-panel large">
          <PanelTitle title="Host Disk Usage" detail="From latest host heartbeat" />
          {dashboard.hostDiskUsage.length ? (
            <>
              <div className="db-host-disk-legend" aria-label="Host status color legend">
                <span><i className="live" />Live</span>
                <span><i className="offline" />Offline</span>
              </div>
              <ResponsiveContainer width="100%" height={244}>
                <BarChart data={dashboard.hostDiskUsage} layout="vertical" margin={{ top: 8, right: 22, bottom: 8, left: 18 }}>
                  <CartesianGrid stroke="#eeeae3" horizontal={false} />
                  <XAxis type="number" domain={[0, 100]} tickFormatter={v => `${v}%`} stroke="#8b8982" fontSize={11} />
                  <YAxis dataKey="name" type="category" width={132} stroke="var(--text-secondary)" fontSize={11} tickLine={false} />
                  <Tooltip content={<DiskTooltip />} />
                  <Bar dataKey="usedPct" radius={[0, 6, 6, 0]} barSize={16}>
                    {dashboard.hostDiskUsage.map(host => (
                      <Cell
                        key={host.hostId || host.name}
                        fill={host.status?.toUpperCase() === 'OFFLINE' ? '#D92D20' : 'var(--color-info)'}
                      />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </>
          ) : (
            <EmptyPanel text="No disk data yet. Wait for host heartbeat metrics." />
          )}
        </article>

        <article className="db-panel">
          <PanelTitle title="Cluster Status" detail={`${summary.totalClusters} cluster records`} />
          <StatusDonut data={dashboard.clusterStatus} />
        </article>

        <article className="db-panel">
          <PanelTitle title="Host Fleet" detail={`${summary.activeHosts} active of ${summary.totalHosts}`} />
          <StatusDonut data={dashboard.hostStatus} />
        </article>
      </section>

      <section className="db-main-grid lower">
        <article className="db-panel large">
          <PanelTitle
            title="Task Activity"
            detail={
              <span className="db-panel-select">
                Last 7 days
              </span>
            }
          />
          <ResponsiveContainer width="100%" height={235}>
            <LineChart data={dashboard.taskTimeline} margin={{ top: 8, right: 18, bottom: 8, left: 0 }}>
              <CartesianGrid stroke="#eeeae3" vertical={false} />
              <XAxis dataKey="label" stroke="#8b8982" fontSize={11} tickLine={false} />
              <YAxis allowDecimals={false} stroke="#8b8982" fontSize={11} tickLine={false} />
              <Tooltip />
              <Legend content={renderTaskLegend} verticalAlign="bottom" align="left" wrapperStyle={{ bottom: -5 }} />
              <Line type="monotone" dataKey="failed" stroke="var(--accent-primary)" strokeWidth={2} dot={false} name="Failed" />
              <Line type="monotone" dataKey="running" stroke="#FFCF57" strokeWidth={2} dot={false} name="Running" />
              <Line type="monotone" dataKey="success" stroke="#098C60" strokeWidth={2} dot={false} name="Success" />
            </LineChart>
          </ResponsiveContainer>
        </article>

        <article className="db-panel services-panel">
          <PanelTitle title="Services" detail="" />
          <div className="tab-headers">
            <button type="button" className={serviceTab === 'running' ? 'active-tab' : ''} onClick={() => setServiceTab('running')}>Running ({summary.runningServices})</button>
            <button type="button" className={serviceTab === 'failed' ? 'active-tab' : ''} onClick={() => setServiceTab('failed')}>Failed ({summary.failedServices})</button>
          </div>
          <div className="tab-content">
            <ServiceList rows={serviceTab === 'running' ? dashboard.runningServices : dashboard.failedServices} iconFor={serviceIcon} />
          </div>
        </article>
      </section>

      <section className="db-bottom-grid">
        <article className="db-panel">
          <PanelTitle title="Activity Feed" detail="View all" onDetailClick={() => navigate('/audit')} />
          <div className="db-feed">
            {dashboard.recentActivities.length ? dashboard.recentActivities.map(item => (
              <div key={item.id} className="db-feed-row">
                <span className={`db-feed-level ${item.level?.toLowerCase() || 'info'}`}><FileText size={16} /></span>
                <div>
                  <strong>{item.message}</strong>
                  <small>{formatDateTime(item.createdAt)}</small>
                </div>
              </div>
            )) : <EmptyPanel text="No activity has been logged yet." compact />}
          </div>
        </article>

        <article className="db-panel">
          <PanelTitle title="Recent Tasks" detail="View all" onDetailClick={() => navigate('/jobs')} />
          <div className="tab-headers" role="tablist" aria-label="Recent task outcome">
            <button
              type="button"
              role="tab"
              aria-selected={taskTab === 'success'}
              className={taskTab === 'success' ? 'active-tab' : ''}
              onClick={() => setTaskTab('success')}
            >
              {statusLabel('SUCCESS')} ({dashboard.recentTasks.filter(isSuccessfulTask).length})
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={taskTab === 'failed'}
              className={taskTab === 'failed' ? 'active-tab' : ''}
              onClick={() => setTaskTab('failed')}
            >
              {statusLabel('FAILED')} ({dashboard.recentTasks.filter(t => !isSuccessfulTask(t)).length})
            </button>
          </div>
          <div className="db-task-list">
            {dashboard.recentTasks.filter(task => taskTab === 'success' ? isSuccessfulTask(task) : !isSuccessfulTask(task)).length
              ? dashboard.recentTasks.filter(task => taskTab === 'success' ? isSuccessfulTask(task) : !isSuccessfulTask(task)).map(task => (
                <div key={task.id} className="db-task-row">
                  <span className={`db-task-status ${task.status?.toLowerCase()}`}><FileCode size={16} /></span>
                  <div>
                    <strong>{prettyCommand(task.command)}</strong>
                    <small>{task.clusterName || task.hostId} - {formatDateTime(task.createdAt)}</small>
                    {task.errorMsg && (
                      <details className="db-task-error">
                        <summary>
                          <AlertTriangle size={14} aria-hidden="true" />
                          <span>{summarizeError(task.errorMsg)}</span>
                          <b>Details</b>
                        </summary>
                        <pre>{task.errorMsg}</pre>
                      </details>
                    )}
                  </div>
                </div>
              ))
              : <EmptyPanel text={taskTab === 'success' ? 'No successful tasks found.' : 'No failed tasks found.'} compact />}
          </div>
        </article>
      </section>

      {showDeploymentModal && createPortal(
        <div className="cd-modal-overlay" onClick={() => setShowDeploymentModal(false)}>
          {deploymentStep === 'choice' ? (
            <div className="cd-deployment-modal" onClick={e => e.stopPropagation()}>
              <div className="cd-deployment-modal-header">
                <div className="cd-deployment-modal-header-content">
                  <h2>Cluster Development</h2>
                  <p>Create a managed Kafka cluster or connect an exiting external cluster.</p>
                </div>
                <button className="cd-icon-btn close-btn" onClick={() => setShowDeploymentModal(false)} title="Close">
                  <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M18 6L6 18M6 6l12 12"></path></svg>
                </button>
              </div>

              <div className="cd-deployment-cards-wrapper">
                <div className="cd-deployment-choice-grid">
                  <div className="cd-deployment-card">
                    <div className="cd-deployment-card-content">
                      <svg className="cluster-choice-icon managed" width="24" height="24" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
                        <circle cx="12" cy="4.5" r="3.25" />
                        <path d="M12 7.75v6M5 13.75h14M5 13.75V17M19 13.75V17" fill="none" stroke="currentColor" strokeWidth="2.5" />
                        <rect x="2" y="17" width="6" height="5" rx="0.5" />
                        <rect x="16" y="17" width="6" height="5" rx="0.5" />
                      </svg>
                      <h3>Create your Cluster</h3>
                      <p>Build a new KRaft or ZooKeeper cluster on selected Tantor host</p>
                    </div>
                    <button className="cd-deployment-btn outline" onClick={(e) => { e.stopPropagation(); setShowDeploymentModal(false); navigate('/cluster-deployment'); }}>Create</button>
                  </div>

                  <div className="cd-deployment-card">
                    <div className="cd-deployment-card-content">
                      <svg className="cluster-choice-icon existing" width="24" height="24" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
                        <path d="M22 11V3h-7v3H9V3H2v8h7V8h2v10h4v3h7v-8h-7v3h-2V8h2v3h7v-8zM7 9H4V5h3v4zm13-4h-3V5h3v4zm0 14h-3v-4h3v4z" />
                      </svg>
                      <h3>Existing Cluster</h3>
                      <p>Connect or discover an external Kafka cluster</p>
                    </div>
                    <button className="cd-deployment-btn outline" onClick={(e) => { e.stopPropagation(); setShowDeploymentModal(false); navigate('/external-clusters'); }}>Explorer</button>
                  </div>
                </div>
              </div>
            </div>
          ) : (
            <div className="cd-modal-container" onClick={e => e.stopPropagation()}>
              <button className="cd-modal-close" onClick={() => setShowDeploymentModal(false)}>
                <X size={20} />
              </button>
              <div className="cd-modal-content">
                <ClusterDeployment onClose={() => setShowDeploymentModal(false)} />
              </div>
            </div>
          )}
        </div>,
        document.body
      )}
    </div>
  );
}

function PanelTitle({ title, detail, onDetailClick }: { title: React.ReactNode; detail?: string | React.ReactNode; onDetailClick?: () => void }) {
  return (
    <div className="db-panel-title">
      <div>
        <h2>{title}</h2>
      </div>
      {onDetailClick ? (
        <button type="button" className="db-panel-detail" onClick={onDetailClick}>{detail}</button>
      ) : <span>{detail}</span>}
    </div>
  );
}

function EmptyPanel({ text, compact = false }: { text: string; compact?: boolean }) {
  return <div className={`db-empty-panel ${compact ? 'compact' : ''}`}>{text}</div>;
}

function StatusDonut({ data }: { data: ChartRow[] }) {
  const clean = data.filter(row => (row.value || 0) > 0);
  if (!clean.length) return <EmptyPanel text="No status data yet." compact />;

  return (
    <div className="db-donut-wrap">
      <div className="db-donut-legend">
        {clean.map(row => {
          const isGreen = row.status?.toUpperCase() === 'SUCCESS' || row.status?.toUpperCase() === 'ONLINE';
          const label = row.status?.toUpperCase() === 'SUCCESS' ? 'Success' : `${row.name} ${row.value}`;
          return (
            <span key={row.status || row.name} className={isGreen ? 'green-pill' : 'default-pill'}>
              <i style={{ background: STATUS_COLORS[row.status || 'UNKNOWN'] || STATUS_COLORS.UNKNOWN }} />
              {label}
            </span>
          );
        })}
      </div>
      <div style={{ display: 'flex', justifyContent: 'center', width: '100%', height: '180px' }}>
        <ResponsiveContainer width={180} height={180}>
          <PieChart width={180} height={180}>
            <Pie
              data={clean}
              dataKey="value"
              nameKey="name"
              innerRadius={80}
              outerRadius={90}
              paddingAngle={3}
            >
              {clean.map(row => <Cell key={row.status || row.name} fill={STATUS_COLORS[row.status || 'UNKNOWN'] || STATUS_COLORS.UNKNOWN} />)}
            </Pie>
            <Tooltip />
          </PieChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function ServiceList({ rows, iconFor }: { rows: ServiceRow[]; iconFor: (type: string) => LucideIcon }) {
  if (!rows.length) return <EmptyPanel text="No services found for this status." compact />;
  return (
    <div className="db-service-list">
      {rows.map(row => {
        const Icon = iconFor(row.type);
        return (
          <div key={`${row.name}-${row.status}`} className="db-service-row">
            <div className={`db-service-icon ${row.status.toLowerCase()}`}><Icon size={18} strokeWidth={2} /></div>
            <div>
              <strong>{row.name}</strong>
              <small>{row.description}</small>
            </div>
          </div>
        );
      })}
    </div>
  );
}

function healthTone(status: string) {
  return clusterStatusTone(status) === 'state-positive' ? 'good' : 'bad';
}

function statusLabel(status: string) {
  if (!status) return 'Unknown';
  if (status.toUpperCase() === 'HEALTHY') return 'Healthy';
  return status.toLowerCase().replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

interface DiskTooltipRow { name: string; status?: string; usedGb: number; totalGb: number; freeGb: number }

function DiskTooltip({ active, payload }: { active?: boolean; payload?: Array<{ payload: DiskTooltipRow }> }) {
  if (!active || !payload?.length) return null;
  const row = payload[0].payload;
  return (
    <div className="db-tooltip">
      <strong>{row.name}</strong>
      <span>Status: {row.status?.toUpperCase() === 'OFFLINE' ? 'Offline' : 'Live'}</span>
      <span>{row.usedGb} GB used of {row.totalGb} GB</span>
      <span>{row.freeGb} GB free</span>
    </div>
  );
}

function relativeTime(value?: string) {
  if (!value) return '-';
  const time = new Date(value).getTime();
  if (Number.isNaN(time)) return '-';
  const diff = Date.now() - time;
  if (diff < 60_000) return 'just now';
  const minutes = Math.round(diff / 60_000);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.round(hours / 24)}d ago`;
}

function formatDateTime(value?: string) {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  return date.toLocaleString([], {
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function prettyCommand(command?: string) {
  if (!command) return 'Task';
  return command.toLowerCase().split('_').map(part => part.charAt(0).toUpperCase() + part.slice(1)).join(' ');
}

function isSuccessfulTask(task: TaskRow) {
  return ['SUCCESS', 'SUCCEEDED', 'COMPLETED', 'COMPLETED_SUCCESSFULLY'].includes(task.status?.toUpperCase() || '');
}
