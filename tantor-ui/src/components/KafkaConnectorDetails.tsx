import { RefreshButton } from './RefreshButton';
import { useEffect, useState } from 'react';
import { Activity, ArrowLeft, ArrowUpDown, ArrowUp, ArrowDown, Braces, CircleAlert, CirclePlay, MoreVertical, RefreshCw, RotateCw, Server, Users, Workflow } from 'lucide-react';
import { AnchoredMenu } from './AnchoredMenu';
import { ConnectorConfigPanel } from './ConnectorConfigPanel';
import { confirmAction } from './confirmUtils';
import { connectorStatusClass, connectorType, type ConnectorRow } from '../utils/kafkaConnect';

type Action = 'pause' | 'resume' | 'stop' | 'restart' | 'restart-all' | 'restart-failed' | 'delete' | 'task';
export function KafkaConnectorDetails({ clusterId, connectionId, name, initial, canManage, onBack, onChanged, onDeleted }: {
  clusterId: string; connectionId: string | null; name: string; initial?: ConnectorRow;
  canManage: boolean; onBack: () => void; onChanged: () => void; onDeleted: () => void;
}) {
  const [data, setData] = useState<ConnectorRow | undefined>(initial);
  const [tab, setTab] = useState<'tasks' | 'config' | 'topics'>('tasks');
  const [configDirty, setConfigDirty] = useState(false);
  const [taskMenu, setTaskMenu] = useState<{ id: number; anchor: HTMLElement } | null>(null);
  const [taskSort, setTaskSort] = useState({ key: 'id', ascending: true });
  const [revision, setRevision] = useState(0);
  const [loading, setLoading] = useState(true);
  const [acting, setActing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const base = `/api/v1/clusters/${clusterId}/data-services/kafka-connect/connectors/${encodeURIComponent(name)}`;
  const withConnection = (path: string) => connectionId ? path + (path.includes('?') ? '&' : '?') + new URLSearchParams({ connectionId }) : path;
  const url = withConnection(base + '/details');
  useEffect(() => {
    const abort = new AbortController();
    fetch(url, { signal: abort.signal }).then(async response => {
      const result = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(result.message || 'Unable to load connector details.');
      if (!abort.signal.aborted) { setData(result); setError(null); }
    }).catch(reason => { if (!abort.signal.aborted) setError(reason.message || 'Unable to load connector details.'); })
      .finally(() => { if (!abort.signal.aborted) setLoading(false); });
    return () => abort.abort();
  }, [url, revision]);
  const refresh = () => { setLoading(true); setRevision(current => current + 1); };
  const actionLabels: Record<Action, string> = { pause: 'Pause connector', resume: 'Resume connector', stop: 'Stop connector', restart: 'Restart connector',
    'restart-all': 'Restart connector and all tasks', 'restart-failed': 'Restart failed connector / tasks', delete: 'Delete connector', task: 'Restart task' };
  const perform = async (action: Action, taskId?: number) => {
    if (!canManage || acting) return;
    setActing(true); setError(null); setNotice(null);
    try {
      const description = action === 'delete' ? `Delete ${name}? This stops its tasks and removes its configuration. Kafka topic data is retained.`
        : `${actionLabels[action]}${taskId != null ? ` ${taskId}` : ''} for ${name}? Processing may be interrupted.`;
      if (!(await confirmAction(description, { title: actionLabels[action], confirmLabel: actionLabels[action] }))) return;
      let suffix = '/' + action, method = 'PUT';
      if (action === 'delete') { suffix = ''; method = 'DELETE'; }
      if (action.startsWith('restart')) {
        suffix = `/restart?includeTasks=${action !== 'restart'}&onlyFailed=${action === 'restart-failed'}`; method = 'POST';
      }
      if (action === 'task') { suffix = `/tasks/${taskId}/restart`; method = 'POST'; }
      const response = await fetch(withConnection(base + suffix), { method });
      const result = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(result.message || `${actionLabels[action]} request failed.`);
      if (action === 'delete') { onDeleted(); onChanged(); return; }
      setNotice(`${actionLabels[action]} request accepted. Refresh to check the latest state.`);
      refresh(); onChanged();
    } catch (reason) { setError(reason instanceof Error ? reason.message : String(reason)); }
    finally { setActing(false); }
  };
  const tasks = data?.status?.tasks || [];
  const sortedTasks = [...tasks].sort((a, b) => String(a[taskSort.key as keyof typeof a] ?? '').localeCompare(String(b[taskSort.key as keyof typeof b] ?? ''), undefined, { numeric: true }) * (taskSort.ascending ? 1 : -1));

  const running = tasks.filter(task => task.state === 'RUNNING').length;
  const failed = tasks.filter(task => task.state === 'FAILED').length;
  return <section className="ds-kc-details-page" aria-label={`Connector ${name}`}>
    <header className="ds-kc-detail-header">
      <div><button className="ds-connector-link" onClick={onBack} disabled={acting || configDirty}><ArrowLeft size={16} /> Connectors</button><h2>{name}</h2></div>
      <div className="ds-inline-actions">
        <RefreshButton className="ds-button outlined" onClick={refresh} disabled={loading || acting || configDirty}><RefreshCw size={16} className={loading ? 'spin' : ''} /> Refresh details</RefreshButton>
        {canManage && <select className="ds-kc-action-select" aria-label="Connector action" value="" disabled={acting || loading || configDirty || !data}
          onChange={event => void perform(event.target.value as Action)}><option value="" disabled>Manage connector</option>
          {(['pause', 'resume', 'stop', 'restart', 'restart-all', 'restart-failed', 'delete'] as Action[]).map(action => <option key={action} value={action}>{actionLabels[action]}</option>)}
        </select>}
      </div>
    </header>
    {taskMenu && <AnchoredMenu anchor={taskMenu.anchor} className="ds-kc-task-popover" onClose={() => setTaskMenu(null)}><div role="menu" aria-label={`Task ${taskMenu.id} actions`}><button autoFocus role="menuitem" className="ds-kc-task-restart" disabled={acting || loading || configDirty} onClick={() => { const id = taskMenu.id; setTaskMenu(null); void perform('task', id); }}><RotateCw size={15} /> Restart task {taskMenu.id}</button></div></AnchoredMenu>}
    {error && <div className="ds-alert" role="alert">{error}</div>}
    {notice && <div className="ds-settings-success" role="status">{notice}</div>}
    {configDirty && <p role="status">You have unsaved configuration edits. Submit or discard them before refreshing, managing the connector, or returning to the list.</p>}
    {loading && <p role="status">Loading current connector details...</p>}
    {data && <>
      <div className="ds-kc-detail-summary">
        <div><span><Server size={15} aria-hidden="true" />Worker</span><strong>{data.worker || data.status?.connector?.worker_id || 'Unavailable'}</strong></div>
        <div><span><Workflow size={15} aria-hidden="true" />Type</span><strong>{connectorType(data)}</strong></div>
        <div className="ds-kc-summary-class"><span><Braces size={15} aria-hidden="true" />Class</span><strong>{data.class || 'Unavailable'}</strong></div>
        <div><span><Activity size={15} aria-hidden="true" />State</span><strong><span className={connectorStatusClass(data.state)}>{data.state}</span></strong></div>
        <div className="ds-kc-task-counter ds-kc-tasks-running"><span><CirclePlay size={17} aria-hidden="true" />Tasks running</span><strong>{running}</strong></div><div className={`ds-kc-task-counter${failed > 0 ? ' ds-kc-tasks-failed' : ''}`}><span><CircleAlert size={17} aria-hidden="true" />Tasks failed</span><strong>{failed}</strong></div>

        {data.consumerGroup && <div className="ds-kc-summary-group"><span><Users size={15} aria-hidden="true" />Consumer group ({data.consumerGroupSource || 'configured'})</span><strong>{data.consumerGroup}</strong></div>}
      </div>
      {data.status?.connector?.trace && <details className="ds-kc-connector-trace"><summary>Connector error trace</summary><pre>{data.status.connector.trace}</pre></details>}
      <div className="ds-connector-detail-tabs" role="tablist" aria-label="Connector details">
        {(['tasks', 'config', 'topics'] as const).map(value => <button type="button" role="tab" id={`connector-tab-${value}`} aria-controls={`connector-panel-${value}`}
          aria-selected={tab === value} className={tab === value ? 'active' : ''} key={value} onClick={() => setTab(value)}>{value.charAt(0).toUpperCase() + value.slice(1)}</button>)}
      </div>
      <div className={`ds-kc-detail-panel${tab === 'config' ? ' ds-kc-config-panel' : ''}`} role="tabpanel" id={`connector-panel-${tab}`} aria-labelledby={`connector-tab-${tab}`}>
        {tab === 'tasks' && <table className="ds-table ds-kc-table ds-kc-task-table"><thead><tr>{[['id', 'ID'], ['worker_id', 'Worker'], ['state', 'State'], ['trace', 'Trace']].map(([key, label]) =>
            <th key={key} aria-sort={taskSort.key === key ? taskSort.ascending ? 'ascending' : 'descending' : 'none'}><button className="ds-kc-sort" aria-label={`Sort tasks by ${label.toLowerCase()}`}
              onClick={() => setTaskSort(current => ({ key, ascending: current.key === key ? !current.ascending : true }))}>{label}{taskSort.key === key ? taskSort.ascending ? <ArrowUp size={14} /> : <ArrowDown size={14} /> : <ArrowUpDown size={14} />}</button></th>)}{canManage && <th>Actions</th>}</tr></thead>
          <tbody>{tasks.length ? sortedTasks.map(task => <tr key={task.id}><td>{task.id}</td><td>{task.worker_id || 'Unavailable'}</td>
            <td><span className={connectorStatusClass(task.state)}>{task.state}</span></td>
            <td>{task.trace ? <details><summary>View trace</summary><pre className="ds-connector-task-trace">{task.trace}</pre></details> : '-'}</td>
            {canManage && <td><button className="ds-kc-task-menu" aria-label={`Task ${task.id} actions`} aria-expanded={taskMenu?.id === task.id} aria-haspopup="menu" disabled={acting || loading || configDirty} onClick={event => setTaskMenu(taskMenu?.id === task.id ? null : { id: task.id, anchor: event.currentTarget })}><MoreVertical size={18} /></button></td>}</tr>)
            : <tr><td colSpan={canManage ? 5 : 4} className="ds-empty">No tasks reported for this connector.</td></tr>}</tbody></table>}
        <div hidden={tab !== 'config'}>
          <ConnectorConfigPanel key={JSON.stringify(data.config)} config={data.config || {}} name={name} url={withConnection(base + '/config')}
            canManage={canManage} busy={acting || loading} onDirty={setConfigDirty} onSaved={() => { setNotice('Connector configuration saved. Tasks may be restarting.'); refresh(); onChanged(); }} />
        </div>
        {tab === 'topics' && (data.topicsAvailable ? <table className="ds-table ds-kc-table"><thead><tr><th>Topic</th></tr></thead><tbody>
          {data.topics?.length ? data.topics.map(topic => <tr key={topic}><td>{topic}</td></tr>) : <tr><td className="ds-empty">No tracked topics found.</td></tr>}</tbody></table>
          : <div className="ds-alert" role="alert">{data.topicsError || 'Tracked topics are unavailable for this Kafka Connect worker.'}</div>)}
      </div>
    </>}
  </section>;
}
