import { useCallback, useEffect, useMemo, useState } from 'react';
import { createPortal } from 'react-dom';
import { useNavigate, useParams, useSearchParams } from 'react-router-dom';
import {
  AlertTriangle, AlertOctagon, ArrowLeft, BarChart3, ChevronDown, ChevronRight,
  Edit3, Gauge, MessageSquare, MoreVertical, RefreshCw, Search, Settings2,
  ShieldCheck, Users, X, Plus
} from 'lucide-react';
import { usePermissions } from '../hooks/usePermissions';
import { CustomSelect } from '../components/CustomSelect';
import { AnchoredMenu } from '../components/AnchoredMenu';
import { TopicActionConfirmationModal } from '../components/TopicActionConfirmationModal';
import './TopicDetails.css';

type Tab = 'overview' | 'messages' | 'consumers' | 'settings' | 'statistics' | 'acls';

interface PartitionDetail {
  partition: number;
  leader: number | null;
  replicas: number[];
  inSyncReplicas: number[];
  underReplicated: boolean;
  firstOffset: number;
  nextOffset: number;
  messageCount: number;
}

interface TopicDetail {
  name: string;
  internal: boolean;
  partitionCount: number;
  replicationFactor: number;
  underReplicated: number;
  inSyncReplicas: number;
  totalReplicas: number;
  messageCount: number;
  storedBytes: number;
  segmentCount: number | null;
  cleanupPolicy: string;
  partitions: PartitionDetail[];
}

interface TopicMessage {
  partition: number;
  offset: number;
  timestamp: number;
  key: string | null;
  value: string | null;
  keySize: number;
  valueSize: number;
  headers: Record<string, string[]>;
}

interface MessageResponse {
  messages: TopicMessage[];
  count: number;
  bytes: number;
  elapsedMs: number;
}

interface MessageFilters {
  order?: string;
  partition?: string;
  search?: string;
}

interface ConsumerGroup {
  groupId: string;
  activeConsumers: number;
  lag: number;
  coordinator: string | null;
  state: string;
}

interface TopicConfig {
  name: string;
  value: string | null;
  defaultValue: string | null;
  source: string;
  readOnly: boolean;
  sensitive: boolean;
}

interface AclRow {
  principal: string;
  host: string;
  operation: string;
  permissionType: string;
  patternType: string;
  resourceName: string;
}

interface SizeStatistics {
  total: number;
  min: number;
  max: number;
  average: number;
  p50: number;
  p75: number;
  p95: number;
  p99: number;
  p999: number;
}

interface TopicStatistics {
  analyzedAt: number;
  sampleLimit: number;
  truncated: boolean;
  messageCount: number;
  minOffset: number;
  maxOffset: number;
  minTimestamp: number;
  maxTimestamp: number;
  nullKeys: number;
  uniqueKeys: number;
  nullValues: number;
  uniqueValues: number;
  keySize: SizeStatistics;
  valueSize: SizeStatistics;
  partitions: Array<{ partition: number; totalMessages: number; minOffset: number; maxOffset: number }>;
}

const tabs: Array<{ id: Tab; label: string; icon: typeof Gauge }> = [
  { id: 'overview', label: 'Overview', icon: Gauge },
  { id: 'messages', label: 'Messages', icon: MessageSquare },
  { id: 'consumers', label: 'Consumers', icon: Users },
  { id: 'settings', label: 'Settings', icon: Settings2 },
  { id: 'statistics', label: 'Statistics', icon: BarChart3 }
];

async function responseError(response: Response) {
  const body = await response.json().catch(() => null);
  return body?.message || body?.error || 'Request failed (HTTP ' + response.status + ')';
}

function formatBytes(value: number) {
  if (value < 0) return 'Unavailable';
  if (value < 1024) return value.toLocaleString() + ' B';
  const units = ['KB', 'MB', 'GB', 'TB'];
  let amount = value;
  let unit = -1;
  do {
    amount /= 1024;
    unit += 1;
  } while (amount >= 1024 && unit < units.length - 1);
  return amount.toLocaleString(undefined, { maximumFractionDigits: 1 }) + ' ' + units[unit];
}

function formatDate(value: number) {
  return value > 0 ? new Date(value).toLocaleString() : '-';
}

export function TopicDetails() {
  const { id, topicName: rawTopicName } = useParams<{ id: string; topicName: string }>();
  const topicName = rawTopicName ? decodeURIComponent(rawTopicName) : '';
  const navigate = useNavigate();
  const { canManage } = usePermissions();
  const [searchParams, setSearchParams] = useSearchParams();
  const requestedTab = searchParams.get('tab') as Tab | null;
  const activeTab: Tab = tabs.some(tab => tab.id === requestedTab) ? requestedTab as Tab : 'overview';

  const [detail, setDetail] = useState<TopicDetail | null>(null);
  const [loadingDetail, setLoadingDetail] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [actionMenu, setActionMenu] = useState(false);
  const [actionMenuAnchor, setActionMenuAnchor] = useState<HTMLDivElement | null>(null);
  const [confirmAction, setConfirmAction] = useState<'clear' | 'recreate' | 'remove' | null>(null);
  const [acting, setActing] = useState(false);
  const [showProduce, setShowProduce] = useState(false);
  const [producing, setProducing] = useState(false);
  const [produceForm, setProduceForm] = useState({ partition: '', key: '', value: '' });

  const [messages, setMessages] = useState<MessageResponse | null>(null);
  const [messagesLoading, setMessagesLoading] = useState(false);
  const [messageOrder, setMessageOrder] = useState('newest');
  const [messagePartition, setMessagePartition] = useState('');
  const [messageSearch, setMessageSearch] = useState('');
  const [expandedMessage, setExpandedMessage] = useState<string | null>(null);

  const [consumers, setConsumers] = useState<ConsumerGroup[]>([]);
  const [consumerSearch, setConsumerSearch] = useState('');
  const [configs, setConfigs] = useState<TopicConfig[]>([]);
  const [configSearch, setConfigSearch] = useState('');
  const [editingConfig, setEditingConfig] = useState<TopicConfig | null>(null);
  const [configValue, setConfigValue] = useState('');
  const [savingConfig, setSavingConfig] = useState(false);
  const [showUnsavedWarning, setShowUnsavedWarning] = useState(false);
  const [statistics, setStatistics] = useState<TopicStatistics | null>(null);
  const [statisticsLoading, setStatisticsLoading] = useState(false);
  const [acls, setAcls] = useState<AclRow[]>([]);
  const [tabLoading, setTabLoading] = useState(false);

  const [keyDeserializer, setKeyDeserializer] = useState('string');
  const [valueDeserializer, setValueDeserializer] = useState('string');

  const orderOptions = [
    { value: 'newest', label: 'Newest first' },
    { value: 'oldest', label: 'Oldest first' }
  ];

  const partitionOptions = useMemo(() => {
    const opts = [{ value: '', label: 'All partitions' }];
    if (detail?.partitions) {
      detail.partitions.forEach(p => {
        opts.push({ value: String(p.partition), label: `Partition ${p.partition}` });
      });
    }
    return opts;
  }, [detail]);

  const keyDeserializerOptions = [
    { value: 'string', label: 'Key: String' },
    { value: 'raw', label: 'Key: Raw UTF-8' }
  ];

  const valueDeserializerOptions = [
    { value: 'string', label: 'Value: String' },
    { value: 'raw', label: 'Value: Raw UTF-8' }
  ];

  const baseUrl = '/api/v1/clusters/' + id + '/topics/' + encodeURIComponent(topicName);

  const loadDetail = useCallback(async () => {
    if (!id || !topicName) return;
    setLoadingDetail(true);
    try {
      const res = await fetch(baseUrl);
      if (!res.ok) throw new Error(`Failed to load topic: ${res.statusText}`);
      setDetail(await res.json());
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : 'Failed to load topic');
    } finally {
      setLoadingDetail(false);
    }
  }, [baseUrl, id, topicName]);

  const loadMessages = useCallback(async (filters: MessageFilters = {}) => {
    if (!id || !topicName) return;
    const order = filters.order ?? messageOrder;
    const partition = filters.partition ?? messagePartition;
    const search = filters.search ?? messageSearch;

    setMessagesLoading(true);
    setError(null);
    try {
      const url = new URL(`${window.location.origin}${baseUrl}/messages`);
      if (partition !== '') url.searchParams.set('partitions', partition);
      if (search.trim()) url.searchParams.set('search', search.trim());
      url.searchParams.set('order', order);

      const res = await fetch(url.toString());
      if (!res.ok) throw new Error(`Failed to browse messages: ${res.statusText}`);
      setMessages(await res.json());
      setExpandedMessage(null);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : 'Failed to browse messages');
    } finally {
      setMessagesLoading(false);
    }
  }, [baseUrl, id, messageOrder, messagePartition, messageSearch, topicName]);

  const changeMessageOrder = (order: string) => {
    setMessageOrder(order);
    void loadMessages({ order });
  };

  const changeMessagePartition = (partition: string) => {
    setMessagePartition(partition);
    void loadMessages({ partition });
  };

  const loadSimpleTab = useCallback(async (tab: 'consumers' | 'configs' | 'acls') => {
    setTabLoading(true);
    setError(null);
    try {
      const res = await fetch(`${baseUrl}/${tab}`);
      if (!res.ok) throw new Error(`Failed to load ${tab}: ${res.statusText}`);
      const data = await res.json();
      if (tab === 'consumers') setConsumers(data);
      if (tab === 'configs') setConfigs(data);
      if (tab === 'acls') setAcls(data);
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : 'Failed to load ' + tab);
    } finally {
      setTabLoading(false);
    }
  }, [baseUrl]);

  const loadStatistics = useCallback(async () => {
    setStatisticsLoading(true);
    setError(null);
    try {
      const res = await fetch(`${baseUrl}/statistics`);
      if (!res.ok) throw new Error(`Failed to analyze topic: ${res.statusText}`);
      setStatistics(await res.json());
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : 'Failed to analyze topic');
    } finally {
      setStatisticsLoading(false);
    }
  }, [baseUrl]);

  // eslint-disable-next-line react-hooks/set-state-in-effect -- fetch synchronizes Kafka data
  useEffect(() => { loadDetail(); }, [loadDetail]);
  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- fetch synchronizes tab data
    if (activeTab === 'messages' && !messages) loadMessages();
    if (activeTab === 'consumers' && consumers.length === 0) loadSimpleTab('consumers');
    if (activeTab === 'settings' && configs.length === 0) loadSimpleTab('configs');
    if (activeTab === 'statistics' && !statistics) loadStatistics();
    if (activeTab === 'acls' && acls.length === 0) loadSimpleTab('acls');
  }, [activeTab, acls.length, configs.length, consumers.length, loadMessages, loadSimpleTab, loadStatistics, messages, statistics]);

  const changeTab = (tab: Tab) => setSearchParams(tab === 'overview' ? {} : { tab });

  const runAction = async () => {
    if (!canManage) return;
    if (!confirmAction) return;
    setActing(true);
    setError(null);
    try {
      let url = baseUrl;
      let method = 'DELETE';
      if (confirmAction === 'clear') url += '/messages';
      if (confirmAction === 'recreate') {
        url += '/recreate';
        method = 'POST';
      }
      const response = await fetch(url, { method });
      if (!response.ok) throw new Error(await responseError(response));
      if (confirmAction === 'remove') {
        navigate('/clusters/' + id + '/topics');
        return;
      }
      setConfirmAction(null);
      setMessages(null);
      setStatistics(null);
      await loadDetail();
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : 'Topic action failed');
    } finally {
      setActing(false);
    }
  };

  const produceMessage = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!canManage) return;
    setProducing(true);
    setError(null);
    try {
      const response = await fetch(baseUrl + '/messages', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          partition: produceForm.partition === '' ? null : Number(produceForm.partition),
          key: produceForm.key === '' ? null : produceForm.key,
          value: produceForm.value,
          headers: {}
        })
      });
      if (!response.ok) throw new Error(await responseError(response));
      setShowProduce(false);
      setProduceForm({ partition: '', key: '', value: '' });
      setMessages(null);
      setStatistics(null);
      await loadDetail();
      if (activeTab === 'messages') await loadMessages();
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : 'Failed to produce message');
    } finally {
      setProducing(false);
    }
  };

  const saveConfig = async () => {
    if (!canManage) return;
    if (!editingConfig) return;
    setSavingConfig(true);
    try {
      const response = await fetch(baseUrl + '/configs/' + encodeURIComponent(editingConfig.name), {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ value: configValue })
      });
      if (!response.ok) throw new Error(await responseError(response));
      setEditingConfig(null);
      await loadSimpleTab('configs');
      await loadDetail();
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : 'Failed to update setting');
    } finally {
      setSavingConfig(false);
    }
  };

  const handleCancelConfigEdit = () => {
    if (editingConfig && configValue !== (editingConfig.value || '')) {
      setShowUnsavedWarning(true);
    } else {
      setEditingConfig(null);
    }
  };

  const filteredConsumers = useMemo(() => consumers.filter(group =>
    group.groupId.toLowerCase().includes(consumerSearch.toLowerCase())), [consumerSearch, consumers]);
  const filteredConfigs = useMemo(() => configs.filter(config =>
    config.name.toLowerCase().includes(configSearch.toLowerCase())), [configSearch, configs]);

  if (loadingDetail && !detail) {
    return <div className="topic-detail-state"><RefreshCw className="spin" /> Loading topic...</div>;
  }

  if (!detail) {
    return <div className="topic-detail-state error"><AlertTriangle /> {error || 'Topic was not found.'}</div>;
  }

  return (
    <section className="topic-detail-page animate-fade-in">
      <div className="topic-detail-heading">
        <div>
          <button className="topic-back" onClick={() => navigate('/clusters/' + id + '/topics')}><ArrowLeft size={15} /> Topics</button>
          <div className="topic-detail-title">
            <h2>{detail.name}</h2>
            {detail.internal && <span className="topic-type-badge">Internal</span>}
          </div>
        </div>
        {canManage && <div className="topic-heading-actions">
          <button className="topic-detail-button primary" onClick={() => setShowProduce(true)}><Plus size={16} /> Produce message</button>
          <div ref={setActionMenuAnchor} className="detail-menu-wrap">
            <button className="detail-icon-button" aria-label="Topic actions" onClick={() => setActionMenu(current => !current)}><MoreVertical size={19} /></button>
            {actionMenu && actionMenuAnchor && (
              <AnchoredMenu
                anchor={actionMenuAnchor}
                className="detail-action-menu"
                onClose={() => setActionMenu(false)}
                minWidth={180}
              >
                <button onClick={() => { setConfirmAction('clear'); setActionMenu(false); }}>Clear messages</button>
                <button onClick={() => { setConfirmAction('recreate'); setActionMenu(false); }}>Recreate topic</button>
                <button onClick={() => { setConfirmAction('remove'); setActionMenu(false); }}>Remove topic</button>
              </AnchoredMenu>
            )}
          </div>
        </div>}
      </div>

      <nav className="topic-detail-tabs" aria-label="Topic sections">
        {tabs.map(tab => (
          <button key={tab.id} className={activeTab === tab.id ? 'active' : ''} onClick={() => changeTab(tab.id)}>
            {tab.label}
          </button>
        ))}
      </nav>

      {error && <div className="detail-alert"><AlertTriangle size={17} /><span>{error}</span><button onClick={() => setError(null)}><X size={15} /></button></div>}

      <div className="topic-tab-content">
        {activeTab === 'overview' && <OverviewTab detail={detail} />}

        {activeTab === 'messages' && (
          <div className="messages-tab">
            <div className="message-toolbar">
              <CustomSelect
                value={messageOrder}
                onChange={changeMessageOrder}
                options={orderOptions}
                width="135px"
              />
              <CustomSelect
                value={messagePartition}
                onChange={changeMessagePartition}
                options={partitionOptions}
                width="145px"
              />
              <CustomSelect
                value={keyDeserializer}
                onChange={setKeyDeserializer}
                options={keyDeserializerOptions}
                width="130px"
              />
              <CustomSelect
                value={valueDeserializer}
                onChange={setValueDeserializer}
                options={valueDeserializerOptions}
                width="140px"
              />
              <label><Search size={16} /><input value={messageSearch} onChange={event => setMessageSearch(event.target.value)} onKeyDown={event => { if (event.key === 'Enter') void loadMessages(); }} placeholder="Search key or value" /></label>
              <button className="message-refresh-btn" onClick={() => void loadMessages()} disabled={messagesLoading} aria-label="Refresh messages"><RefreshCw className={messagesLoading ? 'spin' : ''} size={15} /></button>
            </div>

            <div className="detail-table-wrap">
              <table className="detail-table messages-table">
                <thead><tr><th>Offset</th><th>Partition</th><th>Timestamp</th><th>Key preview</th><th>Value preview</th></tr></thead>
                <tbody>
                  {messagesLoading && !messages ? <LoadingRow columns={5} /> : !messages?.messages.length ? <EmptyRow columns={5} text="No messages matched this request." /> : messages.messages.map(message => {
                    const rowId = message.partition + '-' + message.offset;
                    const expanded = expandedMessage === rowId;
                    return [
                      <tr key={rowId} onClick={() => setExpandedMessage(expanded ? null : rowId)}>
                        <td className="offset-cell-wrapper">
                          <span className="expand-chevron-inline">
                            {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                          </span>
                          <span>{message.offset}</span>
                        </td>
                        <td>{message.partition}</td><td>{formatDate(message.timestamp)}</td>
                        <td className="preview-cell">{message.key ?? <span className="null-value">null</span>}</td>
                        <td className="preview-cell">{message.value ?? <span className="null-value">null</span>}</td>
                      </tr>,
                      expanded && <tr className="message-expanded" key={rowId + '-expanded'}><td colSpan={5}>
                        <div><section><h4>Key | {formatBytes(message.keySize)}</h4><pre>{message.key ?? 'null'}</pre></section><section><h4>Value | {formatBytes(message.valueSize)}</h4><pre>{message.value ?? 'null'}</pre></section></div>
                        {Object.keys(message.headers).length > 0 && <section><h4>Headers</h4><pre>{JSON.stringify(message.headers, null, 2)}</pre></section>}
                      </td></tr>
                    ];
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {activeTab === 'consumers' && (
          <div>
            <div className="tab-toolbar"><label><Search size={16} /><input value={consumerSearch} onChange={event => setConsumerSearch(event.target.value)} placeholder="Search by consumer name" /></label><button onClick={() => loadSimpleTab('consumers')} aria-label="Refresh consumers" title="Refresh"><RefreshCw size={15} /></button></div>
            <div className="detail-table-wrap"><table className="detail-table consumers-table"><thead><tr><th>Consumer Group ID</th><th>Active Consumers</th><th>Consumer Lag</th><th>Coordinator</th><th>State</th></tr></thead>
              <tbody>{tabLoading && consumers.length === 0 ? <LoadingRow columns={5} /> : filteredConsumers.length === 0 ? <EmptyRow columns={5} text="No consumer groups use this topic." /> : filteredConsumers.map(group => <tr key={group.groupId}><td>{group.groupId}</td><td>{group.activeConsumers}</td><td>{group.lag.toLocaleString()}</td><td>{group.coordinator || '-'}</td><td>{group.state.charAt(0).toUpperCase() + group.state.slice(1).toLowerCase()}</td></tr>)}</tbody>
            </table></div>
          </div>
        )}

        {activeTab === 'settings' && (
          <div>
            <div className="tab-toolbar">
              <label>
                <Search size={16} />
                <input value={configSearch} onChange={event => setConfigSearch(event.target.value)} placeholder="Search key or value" />
              </label>
              <button onClick={() => loadSimpleTab('configs')} disabled={tabLoading} aria-label="Refresh settings">
                <RefreshCw className={tabLoading ? 'spin' : ''} size={15} />
              </button>
            </div>
            <div className="detail-table-wrap"><table className="detail-table settings-table"><thead><tr><th>Key</th><th>Value</th><th>Default Value</th><th>Source</th><th /></tr></thead>
              <tbody>{tabLoading && configs.length === 0 ? <LoadingRow columns={5} /> : filteredConfigs.map(config => <tr key={config.name}><td>{config.name}</td><td>{config.sensitive ? '******' : config.value ?? '-'}</td><td>{config.defaultValue ?? '-'}</td><td>{config.source.toLowerCase().split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ')}</td><td className="setting-actions">{canManage && !config.readOnly && !config.sensitive && <button title="Edit setting" onClick={() => { setEditingConfig(config); setConfigValue(config.value || ''); }}><Edit3 size={16} /></button>}</td></tr>)}</tbody>
            </table></div>
          </div>
        )}

        {activeTab === 'statistics' && (
          <div className="statistics-tab">
            <div className="tab-toolbar" style={{ marginBottom: '24px' }}>
              <label style={{ width: '612px', flex: 'none' }}>
                <Search size={16} />
                <input placeholder="Search key or value" disabled />
              </label>
              <button onClick={loadStatistics} disabled={statisticsLoading} aria-label="Refresh statistics">
                <RefreshCw className={statisticsLoading ? 'spin' : ''} size={15} />
              </button>
            </div>
            <div className="statistics-figma-heading">
              <h3 style={{ margin: 0, color: 'var(--button-primary-hover)', fontSize: 'var(--text-md)', fontWeight: 'var(--font-medium)', fontFamily: 'Satoshi, sans-serif' }}>Messages</h3>
            </div>
            {statisticsLoading && !statistics ? (
              <div className="analysis-loading"><RefreshCw className="spin" /> Reading topic messages...</div>
            ) : (
              statistics && <StatisticsView statistics={statistics} />
            )}
          </div>
        )}

        {activeTab === 'acls' && (
          <div>
            <div className="tab-toolbar"><div><ShieldCheck size={17} /> Topic access control</div><button onClick={() => loadSimpleTab('acls')} aria-label="Refresh access control" title="Refresh"><RefreshCw size={15} /></button></div>
            <div className="detail-table-wrap"><table className="detail-table"><thead><tr><th>Principal</th><th>Host</th><th>Operation</th><th>Permission</th><th>Pattern</th></tr></thead>
              <tbody>{tabLoading && acls.length === 0 ? <LoadingRow columns={5} /> : acls.length === 0 ? <EmptyRow columns={5} text="No ACL entries match this topic." /> : acls.map((acl, index) => <tr key={acl.principal + acl.operation + index}><td><strong>{acl.principal}</strong></td><td>{acl.host}</td><td>{acl.operation}</td><td><span className={'permission-pill ' + acl.permissionType.toLowerCase()}>{acl.permissionType}</span></td><td>{acl.patternType} | {acl.resourceName}</td></tr>)}</tbody>
            </table></div>
          </div>
        )}
      </div>

      {canManage && showProduce && createPortal(
        <div className="topic-modal-backdrop" onMouseDown={() => setShowProduce(false)}>
          <div className="topic-modal config-modal figma-topic-modal" onMouseDown={event => event.stopPropagation()} style={{ width: '480px' }}>
            <header className="create-topic-header">
              <div className="modal-title-area">
                <h2>Write to topic</h2>
                <h3 style={{ textTransform: 'none', color: 'var(--button-primary)', fontSize: '15px' }}>Produce message</h3>
              </div>
              <button className="create-topic-close" onClick={() => setShowProduce(false)} aria-label="Close modal">
                <X size={20} />
              </button>
            </header>
            <form onSubmit={produceMessage}>
              <div className="figma-topic-modal-body" style={{ padding: '24px 32px', display: 'flex', flexDirection: 'column', gap: 'var(--space-4)' }}>
                <label className="figma-form-field full-width">
                  <span>Partition</span>
                  <select value={produceForm.partition} onChange={event => setProduceForm(current => ({ ...current, partition: event.target.value }))}>
                    <option value="">Automatic</option>
                    {detail.partitions.map(partition => (
                      <option key={partition.partition} value={partition.partition}>Partition {partition.partition}</option>
                    ))}
                  </select>
                </label>
                <label className="figma-form-field full-width">
                  <span>Key (Optional)</span>
                  <input
                    value={produceForm.key}
                    onChange={event => setProduceForm(current => ({ ...current, key: event.target.value }))}
                    placeholder="message key"
                  />
                </label>
                <label className="figma-form-field full-width">
                  <span>Value</span>
                  <input
                    required
                    value={produceForm.value}
                    onChange={event => setProduceForm(current => ({ ...current, value: event.target.value }))}
                    placeholder="message Value"
                  />
                </label>
              </div>
              <footer className="create-topic-footer">
                <button type="button" className="topic-button outline cancel-btn" onClick={() => setShowProduce(false)}>
                  Cancel
                </button>
                <button className="topic-button filled create-btn" disabled={producing}>
                  {producing ? 'Producing...' : 'Produce message'}
                </button>
              </footer>
            </form>
          </div>
        </div>,
        document.body
      )}

      {canManage && confirmAction && (
        <TopicActionConfirmationModal
          action={confirmAction}
          topicNames={[detail.name]}
          acting={acting}
          onClose={() => setConfirmAction(null)}
          onConfirm={runAction}
        />
      )}
      {canManage && editingConfig && createPortal(
        <div className="topic-modal-backdrop" onMouseDown={handleCancelConfigEdit}>
          <div className="topic-modal config-modal figma-topic-modal" onMouseDown={event => event.stopPropagation()} style={{ width: '480px' }}>
            <header className="create-topic-header">
              <div className="modal-title-area">
                <h2>Topic setting</h2>
                <h3 style={{ textTransform: 'none', color: 'var(--button-primary)', fontSize: '15px' }}>
                  {editingConfig.name.charAt(0).toUpperCase() + editingConfig.name.slice(1)}
                </h3>
              </div>
              <button className="create-topic-close" onClick={handleCancelConfigEdit} aria-label="Close modal">
                <X size={20} />
              </button>
            </header>
            <div className="figma-topic-modal-body" style={{ padding: '24px 32px' }}>
              <label className="figma-form-field full-width">
                <span>Value</span>
                <input
                  autoFocus
                  value={configValue}
                  onChange={event => setConfigValue(event.target.value)}
                />
              </label>
              <p style={{ margin: '8px 0 0 0', fontSize: 'var(--text-sm)', color: 'var(--text-tertiary)', fontFamily: 'Satoshi, sans-serif' }}>
                Default value {editingConfig.defaultValue ?? '-1'}
              </p>
            </div>
            <footer className="create-topic-footer">
              <button type="button" className="topic-button outline cancel-btn" onClick={handleCancelConfigEdit}>
                Cancel
              </button>
              <button className="topic-button filled create-btn" onClick={saveConfig} disabled={savingConfig}>
                {savingConfig ? 'Saving...' : 'Save setting'}
              </button>
            </footer>
          </div>
        </div>,
        document.body
      )}

      {showUnsavedWarning && createPortal(
        <div className="topic-modal-backdrop" onMouseDown={() => setShowUnsavedWarning(false)}>
          <div className="topic-modal figma-topic-modal figma-confirm-modal" onMouseDown={event => event.stopPropagation()} style={{ width: '543px', borderRadius: '16px', padding: 0 }}>
            <div className="confirm-modal-banner">
              <button onClick={() => setShowUnsavedWarning(false)} className="confirm-modal-close-btn" aria-label="Close warning">
                <X size={20} />
              </button>
            </div>
            <div className="confirm-modal-body">
              <div className="confirm-modal-title-row">
                <AlertOctagon size={24} color="#FFFFFF" fill="var(--color-danger)" style={{ marginRight: '8px' }} />
                <h2>Your details are not saved.</h2>
              </div>
              <p className="confirm-modal-desc">
                Would you like to save the settings?
              </p>
              <div className="confirm-modal-footer">
                <button type="button" className="confirm-btn-outline" onClick={() => { setShowUnsavedWarning(false); setEditingConfig(null); }}>
                  Discard
                </button>
                <button className="confirm-btn-filled" onClick={() => { setShowUnsavedWarning(false); saveConfig(); }}>
                  Save settings
                </button>
              </div>
            </div>
          </div>
        </div>,
        document.body
      )}
    </section>
  );
}

function OverviewTab({ detail }: { detail: TopicDetail }) {
  const cards = [
    ['Partitions', detail.partitionCount.toLocaleString()],
    ['Replication factor', detail.replicationFactor.toLocaleString()],
    ['Under-replicated', detail.underReplicated.toLocaleString()],
    ['In-sync replicas', detail.inSyncReplicas + ' of ' + detail.totalReplicas],
    ['Stored data', formatBytes(detail.storedBytes)],
    ['Messages', detail.messageCount.toLocaleString()],
    ['Cleanup policy', detail.cleanupPolicy.toUpperCase()],
    ['Segment count', detail.segmentCount === null ? 'Not exposed' : detail.segmentCount.toLocaleString()]
  ];
  return (
    <div className="overview-tab">
      <div className="topic-overview-container" style={{ marginBottom: '24px' }}>
        <div className="topic-metric-grid">
          {cards.map(([label, value], idx) => (
            <article key={idx} style={{ display: 'flex', flexDirection: 'column', gap: '4px', padding: 'var(--space-4)' }}>
              <span style={{ fontSize: 'var(--text-xs)', color: 'var(--text-tertiary)', fontFamily: 'Satoshi, sans-serif' }}>{label}</span>
              <strong style={{ fontSize: '18px', color: 'var(--button-primary-active)', fontFamily: 'Satoshi, sans-serif' }}>{value}</strong>
            </article>
          ))}
        </div>
      </div>
      <div className="overview-note">
        <Gauge size={16} />
        <span>Stored data is calculated from broker replica log sizes. Kafka does not expose physical segment count through the Admin API.</span>
      </div>
      <div className="detail-table-wrap">
        <table className="detail-table">
          <thead>
            <tr>
              <th>Partition ID</th>
              <th>Leader</th>
              <th>Replicas</th>
              <th>In-sync Replicas</th>
              <th>First Offset</th>
              <th>Next Offset</th>
              <th>Messages</th>
            </tr>
          </thead>
          <tbody>
            {detail.partitions.map(partition => (
              <tr key={partition.partition}>
                <td><strong>{partition.partition}</strong></td>
                <td>{partition.leader ?? '-'}</td>
                <td>{partition.replicas.join(', ')}</td>
                <td className={partition.underReplicated ? 'replicas-warning' : 'replicas-ok'}>{partition.inSyncReplicas.join(', ')}</td>
                <td>{partition.firstOffset.toLocaleString()}</td>
                <td>{partition.nextOffset.toLocaleString()}</td>
                <td>{partition.messageCount.toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function StatisticsView({ statistics }: { statistics: TopicStatistics }) {
  return <div>
    <div className="statistics-banner-container">
      <div className="statistics-banner-row">
        <div className="stat-card-white">
          <span className="stat-card-label">Total messages</span>
          <strong className="stat-card-value">{statistics.messageCount.toLocaleString()}</strong>
        </div>
        <div className="stat-card-white">
          <span className="stat-card-label">Offset range</span>
          <strong className="stat-card-value">{statistics.minOffset + ' - ' + statistics.maxOffset}</strong>
        </div>
        <div className="stat-card-white timestamp-card" style={{ flexGrow: 1, minWidth: '321px' }}>
          <span className="stat-card-label">Timestamp range</span>
          <strong className="stat-card-value">{formatDate(statistics.minTimestamp) + ' - ' + formatDate(statistics.maxTimestamp)}</strong>
        </div>
        <div className="stat-card-white">
          <span className="stat-card-label">Null keys</span>
          <strong className="stat-card-value">{statistics.nullKeys.toLocaleString()}</strong>
        </div>
        <div className="stat-card-white">
          <span className="stat-card-label">Unique keys</span>
          <strong className="stat-card-value">{statistics.uniqueKeys.toLocaleString()}</strong>
        </div>
        <div className="stat-card-white">
          <span className="stat-card-label">Null values</span>
          <strong className="stat-card-value">{statistics.nullValues.toLocaleString()}</strong>
        </div>
        <div className="stat-card-white">
          <span className="stat-card-label">Unique values</span>
          <strong className="stat-card-value">{statistics.uniqueValues.toLocaleString()}</strong>
        </div>
      </div>
    </div>

    <SizeStatSection title="Key size" stats={statistics.keySize} />
    <SizeStatSection title="Value size" stats={statistics.valueSize} />

    <div className="detail-table-wrap">
      <table className="detail-table statistics-table">
        <thead>
          <tr>
            <th>Partition ID</th>
            <th>Total Messages</th>
            <th>Min Offset</th>
            <th>Max Offset</th>
          </tr>
        </thead>
        <tbody>
          {statistics.partitions.map(partition => (
            <tr key={partition.partition}>
              <td>{partition.partition}</td>
              <td>{partition.totalMessages.toLocaleString()}</td>
              <td>{partition.minOffset.toLocaleString()}</td>
              <td>{partition.maxOffset.toLocaleString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  </div>;
}

function SizeStatSection({ title, stats }: { title: string; stats: SizeStatistics }) {
  return (
    <div className="size-stat-section">
      <h3 style={{ margin: '16px 0 8px 0', color: 'var(--button-primary-hover)', fontSize: 'var(--text-md)', fontWeight: 'var(--font-medium)', fontFamily: 'Satoshi, sans-serif' }}>{title}</h3>
      <div className="statistics-banner-container">
        <div className="statistics-banner-row">
          <div className="stat-card-white">
            <span className="stat-card-label">Total size</span>
            <strong className="stat-card-value">{formatBytes(stats.total)}</strong>
          </div>
          <div className="stat-card-white">
            <span className="stat-card-label">Minimum size</span>
            <strong className="stat-card-value">{formatBytes(stats.min)}</strong>
          </div>
          <div className="stat-card-white">
            <span className="stat-card-label">Maximum size</span>
            <strong className="stat-card-value">{formatBytes(stats.max)}</strong>
          </div>
          <div className="stat-card-white">
            <span className="stat-card-label">Average size</span>
            <strong className="stat-card-value">{formatBytes(Math.round(stats.average))}</strong>
          </div>
          <div className="stat-card-white" style={{ minWidth: '101px' }}>
            <span className="stat-card-label">50th percentile</span>
            <strong className="stat-card-value">{formatBytes(stats.p50)}</strong>
          </div>
        </div>
        <div className="statistics-banner-row" style={{ marginTop: '8px' }}>
          <div className="stat-card-white">
            <span className="stat-card-label">75th percentile</span>
            <strong className="stat-card-value">{formatBytes(stats.p75)}</strong>
          </div>
          <div className="stat-card-white">
            <span className="stat-card-label">95th percentile</span>
            <strong className="stat-card-value">{formatBytes(stats.p95)}</strong>
          </div>
          <div className="stat-card-white">
            <span className="stat-card-label">99th percentile</span>
            <strong className="stat-card-value">{formatBytes(stats.p99)}</strong>
          </div>
          <div className="stat-card-white" style={{ flexGrow: 1.5 }}>
            <span className="stat-card-label">99.9th percentile</span>
            <strong className="stat-card-value">{formatBytes(stats.p999)}</strong>
          </div>
        </div>
      </div>
    </div>
  );
}

function LoadingRow({ columns }: { columns: number }) {
  return <tr><td colSpan={columns}><div className="table-state"><RefreshCw className="spin" size={18} /> Loading live Kafka data...</div></td></tr>;
}

function EmptyRow({ columns, text }: { columns: number; text: string }) {
  return <tr><td colSpan={columns}><div className="table-state">{text}</div></td></tr>;
}
