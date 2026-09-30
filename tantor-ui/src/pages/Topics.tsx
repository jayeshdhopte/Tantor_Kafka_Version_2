import { useCallback, useEffect, useMemo, useState } from 'react';
import { createPortal } from 'react-dom';
import { useNavigate, useParams } from 'react-router-dom';
import {
  AlertTriangle, Check, ChevronLeft, ChevronRight, Copy, Database, Download,
  Plus, RefreshCw, Search, Trash2, X
} from 'lucide-react';
import { usePermissions } from '../hooks/usePermissions';
import { AnchoredMenu } from '../components/AnchoredMenu';
import { TopicActionConfirmationModal } from '../components/TopicActionConfirmationModal';
import { topicActionCopy, type TopicActionKind } from '../components/topicActionTypes';
import './Topics.css';

interface TopicSummary {
  name: string;
  partitionCount: number;
  replicationFactor: number;
  underReplicated: number;
  messageCount: number;
  internal: boolean;
}

interface PaginatedResponse {
  content: TopicSummary[];
  page: number;
  size: number;
  totalElements: number;
  totalPages: number;
  hasNext: boolean;
}

type PendingTopicAction = { kind: TopicActionKind; names: string[] };

async function apiError(response: Response) {
  const body = await response.json().catch(() => null);
  return body?.message || body?.error || 'Request failed (HTTP ' + response.status + ')';
}


export function Topics() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { canManage } = usePermissions();
  const [data, setData] = useState<PaginatedResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const liveSettingsKey = 'tantor:topics-live:' + (id || 'default');
  const [autoRefresh, setAutoRefresh] = useState(() => window.localStorage.getItem(liveSettingsKey) === 'true');
  const [refreshInterval, setRefreshInterval] = useState(() => {
    const savedInterval = Number(window.localStorage.getItem(liveSettingsKey + ':interval'));
    return [5, 10, 15, 30, 60].includes(savedInterval) ? savedInterval : 15;
  });
  const [showIntervalDropdown, setShowIntervalDropdown] = useState(false);
  const [liveDropdownAnchor, setLiveDropdownAnchor] = useState<HTMLDivElement | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const [size, setSize] = useState(10);
  const [search, setSearch] = useState('');
  const [includeInternal, setIncludeInternal] = useState(false);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [pendingAction, setPendingAction] = useState<PendingTopicAction | null>(null);
  const [acting, setActing] = useState(false);
  const [showCreate, setShowCreate] = useState(false);
  const [newTopic, setNewTopic] = useState({
    name: '',
    partitions: 1,
    replicationFactor: 1,
    cleanupPolicy: 'delete',
    minInsyncReplicas: '',
    retentionMs: '',
    maxPartitionSize: '',
    maxMessageBytes: ''
  });
  const [creating, setCreating] = useState(false);

  const quickOptions = [
    { label: '1 hour', ms: '3600000' },
    { label: '3 hours', ms: '10800000' },
    { label: '6 hours', ms: '21600000' },
    { label: '12 hours', ms: '43200000' },
    { label: '1 day', ms: '86400000' },
    { label: '2 days', ms: '172800000' },
    { label: '7 days', ms: '604800000' },
    { label: '4 weeks', ms: '2419200000' }
  ];

  const cleanupPolicyOptions = [
    { value: 'delete', label: 'Delete' },
    { value: 'compact', label: 'Compact' }
  ];


  const fetchTopics = useCallback(async (quiet = false) => {
    if (!id) return;
    if (quiet) setRefreshing(true);
    else setLoading(true);
    setError(null);
    try {
      const res = await fetch(`/api/v1/clusters/${id}/topics?includeInternal=${includeInternal}&page=${page}&size=${size}&search=${encodeURIComponent(search)}`);
      if (!res.ok) {
        throw new Error(`Failed to load topics: ${res.statusText}`);
      }
      const jsonData = await res.json();
      setData(jsonData);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : 'Failed to load topics');
    } finally {
      if (quiet) setRefreshing(false);
      else setLoading(false);
    }
  }, [id, includeInternal, page, search, size]);

  // eslint-disable-next-line react-hooks/set-state-in-effect -- fetch synchronizes Kafka data
  useEffect(() => { fetchTopics(); }, [fetchTopics]);
  useEffect(() => {
    if (!autoRefresh) return;
    const timer = window.setInterval(() => fetchTopics(true), refreshInterval * 1000);
    return () => window.clearInterval(timer);
  }, [autoRefresh, fetchTopics, refreshInterval]);

  const updateLiveSettings = (enabled: boolean, interval = refreshInterval) => {
    window.localStorage.setItem(liveSettingsKey, String(enabled));
    window.localStorage.setItem(liveSettingsKey + ':interval', String(interval));
    setAutoRefresh(enabled);
    setRefreshInterval(interval);
  };

  const visibleNames = useMemo(() => data?.content.map(topic => topic.name) || [], [data]);
  const allVisibleSelected = visibleNames.length > 0 && visibleNames.every(name => selected.has(name));

  const toggleAll = () => {
    if (!canManage) return;
    setSelected(current => {
      const next = new Set(current);
      if (allVisibleSelected) visibleNames.forEach(name => next.delete(name));
      else visibleNames.forEach(name => next.add(name));
      return next;
    });
  };

  const toggleTopic = (name: string) => {
    if (!canManage) return;
    setSelected(current => {
      const next = new Set(current);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  };

  const createTopic = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!canManage) return;
    if (!id || !newTopic.name.trim()) return;
    setCreating(true);
    setError(null);
    try {
      const configs: Record<string, string> = {};
      if (newTopic.cleanupPolicy) configs['cleanup.policy'] = newTopic.cleanupPolicy;
      if (newTopic.minInsyncReplicas.trim()) configs['min.insync.replicas'] = newTopic.minInsyncReplicas.trim();
      if (newTopic.retentionMs.trim()) configs['retention.ms'] = newTopic.retentionMs.trim();
      if (newTopic.maxPartitionSize.trim()) configs['retention.bytes'] = newTopic.maxPartitionSize.trim();
      if (newTopic.maxMessageBytes.trim()) configs['max.message.bytes'] = newTopic.maxMessageBytes.trim();

      const response = await fetch('/api/v1/clusters/' + id + '/topics', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: newTopic.name.trim(),
          partitions: newTopic.partitions,
          replicationFactor: newTopic.replicationFactor,
          configs
        })
      });
      if (!response.ok) throw new Error(await apiError(response));
      setShowCreate(false);
      setNewTopic({
        name: '',
        partitions: 1,
        replicationFactor: 1,
        cleanupPolicy: 'delete',
        minInsyncReplicas: '',
        retentionMs: '',
        maxPartitionSize: '',
        maxMessageBytes: ''
      });
      setNotice('Topic created successfully.');
      await fetchTopics();
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : 'Failed to create topic');
    } finally {
      setCreating(false);
    }
  };

  const runAction = async () => {
    if (!canManage) return;
    if (!id || !pendingAction) return;
    setActing(true);
    setError(null);
    try {
      for (const name of pendingAction.names) {
        const encoded = encodeURIComponent(name);
        let endpoint = '/api/v1/clusters/' + id + '/topics/' + encoded;
        let method = 'DELETE';
        if (pendingAction.kind === 'clear') endpoint += '/messages';
        if (pendingAction.kind === 'recreate') {
          endpoint += '/recreate';
          method = 'POST';
        }
        const response = await fetch(endpoint, { method });
        if (!response.ok) throw new Error(name + ': ' + await apiError(response));
      }
      setNotice(topicActionCopy[pendingAction.kind].button + ' completed.');
      setPendingAction(null);
      setSelected(new Set());
      await fetchTopics();
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : 'Topic action failed');
    } finally {
      setActing(false);
    }
  };

  const exportCsv = () => {
    if (!data) return;
    const rows = [
      ['Topic name', 'Partitions', 'Out of sync replicas', 'Replication factor', 'Messages', 'Internal'],
      ...data.content.map(topic => [
        topic.name, topic.partitionCount, topic.underReplicated,
        topic.replicationFactor, topic.messageCount, topic.internal
      ])
    ];
    const csv = rows.map(row => row.map(value => {
      const text = String(value);
      return '"' + text.replaceAll('"', '""') + '"';
    }).join(',')).join('\\n');
    const url = URL.createObjectURL(new Blob([csv], { type: 'text/csv;charset=utf-8' }));
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = 'tantor-topics.csv';
    anchor.click();
    URL.revokeObjectURL(url);
  };

  const copySelected = async () => {
    await navigator.clipboard.writeText(Array.from(selected).join('\\n'));
    setNotice('Selected topic names copied.');
  };

  return (
    <section className="topics-page animate-fade-in">
      <div className="topics-title-row">
        <div>
          <h2>Topics</h2>
          <p>Browse, inspect, and manage the streams in this cluster.</p>
        </div>
        <div className="topics-title-actions">
          <button className="topic-button secondary" onClick={() => fetchTopics(Boolean(data))} disabled={loading || refreshing} aria-label="Refresh topics" title="Refresh">
            <RefreshCw size={16} className={loading || refreshing ? 'spin' : ''} />
          </button>
          <div ref={setLiveDropdownAnchor} style={{ position: 'relative', height: '40px', display: 'flex', alignItems: 'center' }}>
            <div
              onClick={() => setShowIntervalDropdown(!showIntervalDropdown)}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 'var(--space-2)',
                background: '#F8FAFC',
                border: `1px solid ${autoRefresh ? 'var(--button-primary)' : 'var(--border-subtle)'}`,
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
                display: 'inline-block',
                flexShrink: 0
              }} />
              <span style={{ fontSize: '14px', fontWeight: 600, color: '#334155', whiteSpace: 'nowrap' }}>Live</span>
              <div style={{
                width: '16px',
                height: '16px',
                borderRadius: '4px',
                border: '1px solid #CBD5E1',
                background: autoRefresh ? '#3B82F6' : '#fff',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                marginLeft: '4px',
                flexShrink: 0
              }}>
                {autoRefresh && <Check size={12} strokeWidth={3} color="#fff" />}
              </div>
              <span style={{ fontSize: '13px', fontWeight: 500, color: '#64748B', marginLeft: '2px', whiteSpace: 'nowrap' }}>
                {refreshInterval}s
              </span>
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
                      updateLiveSettings(true, sec);
                      setShowIntervalDropdown(false);
                    }}
                  >
                    <span className="live-pill-dot active" />
                    Live | {sec} Sec
                  </div>
                ))}
                <div className="dropdown-divider" />
                <div
                  className={`live-dropdown-item ${!autoRefresh ? 'paused' : ''}`}
                  onClick={() => {
                    updateLiveSettings(!autoRefresh);
                    setShowIntervalDropdown(false);
                  }}
                >
                  <span className="live-pill-dot" />
                  {autoRefresh ? 'Pause Live Feed' : 'Resume Live Feed'}
                </div>
              </AnchoredMenu>
            )}
          </div>
          <button className="topic-button outline" onClick={exportCsv} disabled={!data?.content.length}>
            <Download size={16} /> Export CSV
          </button>
          {canManage && (
            <button className="topic-button filled" onClick={() => setShowCreate(true)}>
              <Plus size={16} /> Add Topic
            </button>
          )}
        </div>
      </div>

      <div className="topics-toolbar">
        <label className="topic-search">
          <Search size={18} />
          <input
            value={search}
            onChange={event => {
              setSearch(event.target.value);
              setPage(0);
            }}
            placeholder="Search by topic name"
          />
          {search && <button aria-label="Clear search" onClick={() => setSearch('')}><X size={15} /></button>}
        </label>

        <div className="topics-toolbar-right">
          <label className="internal-toggle">
            <input
              type="checkbox"
              checked={includeInternal}
              onChange={event => {
                setIncludeInternal(event.target.checked);
                setPage(0);
              }}
            />
            <span aria-hidden="true" />
            Show internal topics
          </label>
        </div>
      </div>

      {canManage && selected.size > 0 && (
        <div className="bulk-actions">
          <strong className="bulk-actions-count">{selected.size} selected</strong>
          <button
            className="bulk-action-button destructive"
            onClick={() => setPendingAction({ kind: 'remove', names: Array.from(selected) })}
          >
            <Trash2 size={15} /> Delete selected
          </button>
          <button className="bulk-action-button neutral" onClick={copySelected}>
            <Copy size={15} /> Copy names
          </button>
          <button
            className="bulk-action-button primary"
            onClick={() => setPendingAction({ kind: 'clear', names: Array.from(selected) })}
          >
            <Database size={15} /> Clear messages
          </button>
        </div>
      )}

      {error && (
        <div className="topic-alert error"><AlertTriangle size={18} /><span>{error}</span><button onClick={() => setError(null)}><X size={15} /></button></div>
      )}
      {notice && (
        <div className="topic-alert success"><span>{notice}</span><button onClick={() => setNotice(null)}><X size={15} /></button></div>
      )}

      <div className="topics-table-card">
        <table className="topics-table">
          <thead>
            <tr>
              <th className="th-topic-name">
                <div className="table-header-with-checkbox">
                  {canManage && <input type="checkbox" checked={allVisibleSelected} onChange={toggleAll} />}
                  Topic Name
                </div>
              </th>
              <th>Partiation</th>
              <th>Out of Sync Replica</th>
              <th>Replication Factor</th>
              <th>Message</th>
            </tr>
          </thead>
          <tbody>
            {loading && !data ? (
              <tr><td colSpan={5}><div className="topic-empty"><RefreshCw className="spin" size={24} /> Loading topics…</div></td></tr>
            ) : !data?.content.length ? (
              <tr><td colSpan={5} className="empty-state-cell">
                <div className="topic-empty">
                  <div className="figma-empty-illustration">
                    <div className="illustration-card">
                      <div className="icon-bar grey"></div><div className="icon-bar purple"></div>
                    </div>
                    <div className="illustration-card">
                      <div className="icon-bar grey"></div><div className="icon-bar purple"></div>
                    </div>
                    <div className="illustration-card">
                      <div className="icon-bar grey"></div><div className="icon-bar purple"></div>
                    </div>
                  </div>
                  <strong>No topics found</strong>
                  <span>{search ? 'Try a different search.' : 'Create a topic to start streaming messages.'}</span>
                </div>
              </td></tr>
            ) : data.content.map(topic => (
              <tr key={topic.name} onClick={() => navigate('/clusters/' + id + '/topics/' + encodeURIComponent(topic.name))}>
                <td>
                  <div className="topic-name-cell">
                    {canManage && (
                      <input type="checkbox" checked={selected.has(topic.name)} onChange={event => { event.stopPropagation(); toggleTopic(topic.name); }} onClick={event => event.stopPropagation()} />
                    )}
                    {topic.internal && <span className="internal-badge">IN</span>}
                    <strong>{topic.name}</strong>
                  </div>
                </td>
                <td>{topic.partitionCount?.toLocaleString() ?? '-'}</td>
                <td>
                  <span className={(topic.underReplicated ?? 0) > 0 ? 'health danger' : 'health healthy'}>
                    <i /> {topic.underReplicated?.toLocaleString() ?? '-'}
                  </span>
                </td>
                <td>{topic.replicationFactor ?? '-'}</td>
                <td>{topic.messageCount?.toLocaleString() ?? '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>

        {data && data.totalElements > 0 && (
          <footer className="topics-pagination">
            <span>
              {data.totalElements.toLocaleString()} topic{data.totalElements === 1 ? '' : 's'} · page {data.page + 1} of {Math.max(data.totalPages, 1)}
            </span>
            <div>
              <select value={size} onChange={event => { setSize(Number(event.target.value)); setPage(0); }} aria-label="Rows per page">
                <option value={10}>10 rows</option>
                <option value={25}>25 rows</option>
                <option value={50}>50 rows</option>
                <option value={100}>100 rows</option>
              </select>
              <button disabled={page === 0} onClick={() => setPage(current => Math.max(0, current - 1))}><ChevronLeft size={17} /></button>
              <button disabled={!data.hasNext} onClick={() => setPage(current => current + 1)}><ChevronRight size={17} /></button>
            </div>
          </footer>
        )}
      </div>

      {canManage && showCreate && createPortal(
        <div className="topic-modal-backdrop" role="presentation" onMouseDown={() => setShowCreate(false)}>
          <div className="topic-modal create-topic-modal figma-topic-modal" role="dialog" aria-modal="true" onMouseDown={event => event.stopPropagation()}>
            <header className="create-topic-header">
              <div className="modal-title-area">
                <h2>Create Resource</h2>
                <h3>Add a topic</h3>
                <p>Configure the topic and its Kafka-level retention and delivery settings.</p>
              </div>
              <button className="create-topic-close" onClick={() => setShowCreate(false)} aria-label="Close modal">
                <X size={20} />
              </button>
            </header>

            <form onSubmit={createTopic} className="create-topic-form">
              <div className="figma-topic-modal-body">
                <div className="form-section-header">
                  <span>Topic Details</span>
                </div>

                <div className="form-grid-row">
                  <label className="figma-form-field full-width">
                    <span>Topic name *</span>
                    <input
                      autoFocus
                      required
                      value={newTopic.name}
                      onChange={e => setNewTopic(curr => ({ ...curr, name: e.target.value }))}
                      placeholder="orders.created"
                    />
                  </label>
                </div>

                <div className="form-grid-row-2">
                  <label className="figma-form-field">
                    <span>Number of partitions *</span>
                    <input
                      type="number"
                      min={1}
                      required
                      value={newTopic.partitions}
                      onChange={e => setNewTopic(curr => ({ ...curr, partitions: Number(e.target.value) }))}
                      placeholder="1"
                    />
                  </label>

                  <label className="figma-form-field">
                    <span>Cleanup policy</span>
                    <select
                      value={newTopic.cleanupPolicy}
                      onChange={e => setNewTopic(curr => ({ ...curr, cleanupPolicy: e.target.value }))}
                    >
                      {cleanupPolicyOptions.map(opt => (
                        <option key={opt.value} value={opt.value}>{opt.label}</option>
                      ))}
                    </select>
                  </label>
                </div>

                <div className="form-grid-row-2">
                  <label className="figma-form-field">
                    <span>Minimum in-sync replicas</span>
                    <input
                      value={newTopic.minInsyncReplicas}
                      onChange={e => setNewTopic(curr => ({ ...curr, minInsyncReplicas: e.target.value }))}
                      placeholder="Use broker default"
                    />
                  </label>

                  <label className="figma-form-field">
                    <span>Replication factor *</span>
                    <input
                      type="number"
                      min={1}
                      required
                      value={newTopic.replicationFactor}
                      onChange={e => setNewTopic(curr => ({ ...curr, replicationFactor: Number(e.target.value) }))}
                      placeholder="1"
                    />
                  </label>
                </div>

                <div className="form-grid-row">
                  <label className="figma-form-field full-width">
                    <span>Time to retain data (milliseconds)</span>
                    <input
                      value={newTopic.retentionMs}
                      onChange={e => setNewTopic(curr => ({ ...curr, retentionMs: e.target.value }))}
                      placeholder="Use broker default"
                    />
                  </label>
                </div>

                <div className="quick-options-container">
                  <span className="quick-options-label">Quick options:</span>
                  <div className="quick-options-row">
                    {quickOptions.map(opt => (
                      <button
                        key={opt.label}
                        type="button"
                        className={`quick-option-btn ${newTopic.retentionMs === opt.ms ? 'active' : ''}`}
                        onClick={() => setNewTopic(curr => ({ ...curr, retentionMs: opt.ms }))}
                      >
                        {opt.label}
                      </button>
                    ))}
                  </div>
                </div>

                <div className="form-grid-row-2">
                  <label className="figma-form-field">
                    <span>Max partition size</span>
                    <input
                      value={newTopic.maxPartitionSize}
                      onChange={e => setNewTopic(curr => ({ ...curr, maxPartitionSize: e.target.value }))}
                      placeholder="Not set"
                    />
                  </label>

                  <label className="figma-form-field">
                    <span>Maximum message size (bytes)</span>
                    <input
                      value={newTopic.maxMessageBytes}
                      onChange={e => setNewTopic(curr => ({ ...curr, maxMessageBytes: e.target.value }))}
                      placeholder="Use broker default"
                    />
                  </label>
                </div>
              </div>

              <footer className="create-topic-footer">
                <button type="button" className="topic-button outline cancel-btn" onClick={() => setShowCreate(false)}>
                  Cancel
                </button>
                <button className="topic-button filled create-btn" disabled={creating}>
                  {creating ? 'Creating…' : 'Create topic'}
                </button>
              </footer>
            </form>
          </div>
        </div>,
        document.body
      )}

      {canManage && pendingAction && (
        <TopicActionConfirmationModal
          action={pendingAction.kind}
          topicNames={pendingAction.names}
          acting={acting}
          onClose={() => setPendingAction(null)}
          onConfirm={runAction}
        />
      )}
    </section>
  );
}
