import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  AlertTriangle, CheckCircle, RefreshCw,
  Shield, Activity
} from 'lucide-react';
import './Alerts.css';

interface AlertRow {
  id: string;
  severity: string;
  title: string;
  description?: string;
  clusterId?: string;
  kafkaClusterId?: string;
  clusterName?: string;
  hostId?: string;
  hostIp?: string;
  status?: string;
  createdAt?: string;
  resolvedAt?: string;
  errorLog?: string;
  source?: string;
}

const ALERT_REFRESH_INTERVAL_MS = 2_000;

export function Alerts() {
  const [alerts, setAlerts] = useState<AlertRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [viewMode, setViewMode] = useState<'CURRENT' | 'RESOLVED'>('CURRENT');

  const fetchAlerts = useCallback(async (quiet = false) => {
    if (!quiet) setLoading(true);
    if (!quiet) setError('');
    try {
      const res = await fetch('/api/v1/ui/alerts');
      if (!res.ok) throw new Error(`Alerts request failed (${res.status})`);
      setAlerts(await res.json());
    } catch (e: unknown) {
      if (!quiet) setError(e instanceof Error ? e.message : 'Failed to load alerts');
    } finally {
      if (!quiet) setLoading(false);
    }
  }, []);

  useEffect(() => {
    void (async () => { await fetchAlerts(); })();
    const timer = window.setInterval(() => {
      if (document.visibilityState === 'visible') {
        void (async () => { await fetchAlerts(true); })();
      }
    }, ALERT_REFRESH_INTERVAL_MS);
    const refreshWhenVisible = () => {
      if (document.visibilityState === 'visible') void (async () => { await fetchAlerts(true); })();
    };
    document.addEventListener('visibilitychange', refreshWhenVisible);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener('visibilitychange', refreshWhenVisible);
    };
  }, [fetchAlerts]);

  const activeAlerts = useMemo(
    () => alerts.filter(alert => alert.status?.toUpperCase() !== 'RESOLVED'),
    [alerts],
  );

  const resolvedAlerts = useMemo(
    () => alerts.filter(alert => alert.status?.toUpperCase() === 'RESOLVED'),
    [alerts],
  );

  const visibleAlerts = viewMode === 'CURRENT' ? activeAlerts : resolvedAlerts;

  const summary = useMemo(() => {
    const critical = activeAlerts.filter(alert => alert.severity?.toUpperCase() === 'CRITICAL').length;
    const warning = activeAlerts.filter(alert => alert.severity?.toUpperCase() === 'WARNING').length;
    const clusters = new Set(activeAlerts.map(alert => alert.clusterId).filter(Boolean)).size;
    return { critical, warning, clusters };
  }, [activeAlerts]);

  return (
    <div className="alerts-container">
      {/* Frame 1000004628 */}
      <div className="alerts-wrapper">
        
        <header className="alerts-header">
          {/* Left side info */}
          <div className="alerts-header-info">
            <h1>Alerts</h1>
            <p className="alerts-subtitle">Runtime health, failed tasks, storage pressure, and cluster availability signals.</p>
          </div>
          
          {/* Right side actions */}
          <div className="alerts-header-actions">
            <span className={`alerts-status-badge ${activeAlerts.length ? 'needs-attention' : 'healthy'}`}>
              {activeAlerts.length ? 'Live system needs attention' : 'Live system is healthy'}
            </span>
            <button className="alerts-refresh-btn" onClick={() => fetchAlerts()} aria-label="Refresh alerts">
              <RefreshCw size={14} className={`alerts-refresh-icon ${loading ? 'spin' : ''}`} />
            </button>
          </div>
        </header>

        {error && <div className="alerts-banner-error">{error}</div>}

        {/* Frame 1000005223 */}
        <div className="alerts-content-body">
          
          {/* Frame 1000005211 (KPI Gradient Container) */}
          <section className="alerts-kpi-banner">
            <div className="alerts-kpi-row">
              
              {/* Critical KPI Card */}
              <div className="alerts-kpi-card">
                <div className="alerts-kpi-header">
                  <div className="alerts-kpi-icon-container critical">
                    <Shield size={24} className="alerts-kpi-icon" />
                  </div>
                  <span className="alerts-kpi-title">Critical</span>
                </div>
                <div className="alerts-kpi-value-row">
                  <span className="alerts-kpi-value">{String(summary.critical).padStart(2, '0')}</span>
                </div>
              </div>

              {/* Warnings KPI Card */}
              <div className="alerts-kpi-card">
                <div className="alerts-kpi-header">
                  <div className="alerts-kpi-icon-container warning">
                    <AlertTriangle size={24} className="alerts-kpi-icon" />
                  </div>
                  <span className="alerts-kpi-title">Warnings</span>
                </div>
                <div className="alerts-kpi-value-row">
                  <span className="alerts-kpi-value">{String(summary.warning).padStart(2, '0')}</span>
                </div>
              </div>

              {/* Impacted Clusters KPI Card */}
              <div className="alerts-kpi-card">
                <div className="alerts-kpi-header">
                  <div className="alerts-kpi-icon-container report">
                    <Activity size={24} className="alerts-kpi-icon" />
                  </div>
                  <span className="alerts-kpi-title">Impacted Clusters</span>
                </div>
                <div className="alerts-kpi-value-row">
                  <span className="alerts-kpi-value">{String(summary.clusters).padStart(2, '0')}</span>
                </div>
              </div>

            </div>
          </section>

          {/* Frame 1000005212 (Details Panel) */}
          <section className="alerts-details-panel">
            <div className="alerts-details-toolbar">
              <div>
                <h2>{viewMode === 'CURRENT' ? 'Current Alerts' : 'Resolved Alerts'}</h2>
                <span className="alerts-details-count">
                  {visibleAlerts.length} {visibleAlerts.length === 1 ? 'alert' : 'alerts'}
                </span>
              </div>
              <label className="alerts-view-filter">
                <span>View</span>
                <select
                  value={viewMode}
                  onChange={event => setViewMode(event.target.value as 'CURRENT' | 'RESOLVED')}
                  aria-label="Filter alerts by status"
                >
                  <option value="CURRENT">Current</option>
                  <option value="RESOLVED">Resolved</option>
                </select>
              </label>
            </div>

            {/* Frame 1000005221 */}
            <div className="alerts-details-list">
              {loading ? (
                <div className="alerts-empty-state">
                  <RefreshCw className="spin" size={24} />
                  <strong>Loading alerts...</strong>
                </div>
              ) : visibleAlerts.length === 0 ? (
                <div className="alerts-empty-state healthy">
                  <CheckCircle size={44} />
                  <strong>{viewMode === 'CURRENT' ? 'All systems are healthy' : 'No resolved alerts'}</strong>
                  <span>
                    {viewMode === 'CURRENT'
                      ? 'There are no current alerts requiring attention.'
                      : 'Resolved alert history will appear here.'}
                  </span>
                </div>
              ) : (
                visibleAlerts.map(alert => (
                  /* Frame 1000005219 / 1000005225 */
                  <article key={alert.id} className="alerts-detail-card">
                    {/* Frame 1000005354 */}
                    <div className="alerts-detail-card-content">
                      {/* Frame 1000005353 */}
                      <div className="alerts-detail-title-row">
                        <span className={`alerts-detail-severity-icon ${severityTone(alert.severity)}`}>
                          <AlertTriangle size={24} />
                        </span>
                        
                        {/* Frame 1000005218 */}
                        <div className="alerts-detail-info-col">
                          {/* Frame 1000005217 */}
                          <div className="alerts-detail-header-text">
                            {/* Frame 1000005352 */}
                            <div className="alerts-detail-title-line">
                              <h3>{alert.title}</h3>
                              <span className="alerts-detail-category-pill">{sourceLabel(alert.source)}</span>
                            </div>
                            
                            {/* Frame 1000005355 */}
                            {alert.description && (
                              <p className="alerts-detail-desc">{alert.description}</p>
                            )}
                            
                            {/* Frame 1000005356 (Metadata Grid) */}
                            <div className="alerts-detail-metadata">
                              {/* Cluster */}
                              <div className="alerts-meta-block">
                                <span className="meta-block-label">Cluster</span>
                                <span className="meta-block-val">{clusterLabel(alert)}</span>
                              </div>
                              <div className="alerts-meta-separator" />
                              
                              {/* Cluster ID */}
                              <div className="alerts-meta-block">
                                <span className="meta-block-label">Cluster ID</span>
                                <span className="meta-block-val mono">{alert.kafkaClusterId || '-'}</span>
                              </div>
                              <div className="alerts-meta-separator" />
                              
                              {/* Host / IP */}
                              <div className="alerts-meta-block">
                                <span className="meta-block-label">Host / IP</span>
                                <span className="meta-block-val">{hostLabel(alert)}</span>
                              </div>
                              <div className="alerts-meta-separator" />
                              
                              {/* Detected */}
                              <div className="alerts-meta-block">
                                <span className="meta-block-label">Detected</span>
                                <span className="meta-block-val">{formatDateTime(alert.createdAt) || '-'}</span>
                              </div>
                              <div className="alerts-meta-separator" />

                              {/* Status Badge */}
                              <div className="alerts-meta-status-container">
                                <span className={`alerts-detail-status-pill ${alert.status?.toUpperCase() === 'RESOLVED' ? 'resolved' : ''}`}>
                                  {alert.status?.toUpperCase() === 'RESOLVED' ? 'Resolved' : 'Active'}
                                </span>
                              </div>
                            </div>

                            {/* Frame 1000005357 (Logs box) */}
                            {alert.errorLog && (
                              <div className="alerts-detail-log-box">
                                <pre>{alert.errorLog}</pre>
                              </div>
                            )}

                          </div>
                        </div>
                      </div>
                    </div>
                  </article>
                ))
              )}
            </div>
          </section>

        </div>
      </div>
    </div>
  );
}

function severityTone(severity?: string) {
  const normalized = severity?.toUpperCase();
  if (normalized === 'CRITICAL') return 'bad';
  if (normalized === 'WARNING') return 'warn';
  return 'info';
}

function sourceLabel(source?: string) {
  if (!source) return 'Runtime';
  return source.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

function clusterLabel(alert: AlertRow) {
  if (alert.clusterName && alert.clusterName !== '-') return alert.clusterName;
  // clusterId is Tantor's internal UUID. Never display it as the Kafka
  // cluster identity; the separate Cluster ID field contains kafkaClusterId.
  return '-';
}

function hostLabel(alert: AlertRow) {
  const ip = alert.hostIp && alert.hostIp !== '-' ? alert.hostIp : '';
  // hostId is the management-system identity, often a UUID. Alerts must
  // present the real network address rather than exposing that internal ID.
  return ip || '-';
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
