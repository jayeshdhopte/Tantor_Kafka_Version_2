import { useCallback, useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { History, RefreshCw } from 'lucide-react';
interface Change { key: string; oldValue: string | null; newValue: string | null; previousSource: string | null }
interface Node { nodeId: string; hostId: string | null; role: string | null; mode: string; hostname: string | null; ipAddress: string | null; configPath: string | null; status: string; changes: Change[] }
interface Version { version: number; jobId: string; status: string; requestedBy: string | null; createdAt: string; completedAt: string | null; retryCount: number; nodes: Node[] }
interface Result { items: Version[]; total: number; page: number; hasMore: boolean }
const value = (text: string | null, deleted = false) => text === null ? deleted ? 'Removed; inherited / Kafka default' : 'Unset / inherited' : text === '' ? '(Empty)' : text;
const status = (text: string) => text.replaceAll('_', ' ');
const changeTags = (node: Node) => [
  node.mode === 'STATIC' || node.mode === 'DELETE' ? 'Static' : node.mode === 'DYNAMIC' ? 'Dynamic' : 'Mode not recorded',
  node.role?.toUpperCase() === 'BROKER' ? 'Broker' : node.role?.toUpperCase() === 'CONTROLLER' ? 'Controller' : node.role || 'Role not recorded',
  ...(node.mode === 'DELETE' ? ['Deletion'] : []),
];
export function ConfigChangeHistory({ clusterId }: { clusterId: string }) {
  const [page, setPage] = useState(0);
  const [result, setResult] = useState<Result | null>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [search, setSearch] = useState('');
  const load = useCallback(async (signal?: AbortSignal) => {
    setLoading(true);
    try {
      const response = await fetch(`/api/v1/clusters/${clusterId}/config/change/history?page=${page}`, { signal });
      if (!response.ok) throw new Error('Unable to load configuration history.');
      const data = await response.json() as Result;
      if (!signal?.aborted) { setResult(data); setError(''); }
    } catch (reason) { if (!signal?.aborted) setError(reason instanceof Error ? reason.message : 'Unable to load history.'); }
    finally { if (!signal?.aborted) setLoading(false); }
  }, [clusterId, page]);
  useEffect(() => {
    const controller = new AbortController();
    void Promise.resolve().then(() => load(controller.signal));
    const timer = window.setInterval(() => void load(controller.signal), 30000);
    return () => { controller.abort(); window.clearInterval(timer); };
  }, [load]);
  const needle = search.toLowerCase().trim();
  const items = result?.items.filter(item => !needle || item.nodes.some(node => node.changes.some(change => change.key.toLowerCase().includes(needle)))) || [];
  return <section className="cc-card node-config-section cc-history" aria-label="Configuration change history">
    <div className="cc-history-heading"><div className="cc-section-heading node-config-section-title"><span><History size={17} /></span><div><h3>Configuration change history</h3><p>v1, v2 and later changes show the recorded old value → requested new value on each node. Status confirms whether each change completed.</p></div></div><button className="cc-refresh" disabled={loading} onClick={() => void load()}><RefreshCw size={15} className={loading ? 'spin' : ''} /> Refresh history</button></div>
    <div className="cc-config-toolbar"><input type="search" aria-label="Search properties in history" placeholder="Search properties on this history page…" value={search} onChange={event => setSearch(event.target.value)} /><span>{result?.total ?? 0} changes</span></div>
    {error && <p className="cc-error" role="alert">{error}</p>}
    {!result && loading && <p>Loading configuration history…</p>}
    {result && !items.length && <p>{needle ? 'No matching properties on this page.' : 'No configuration changes have been submitted yet.'}</p>}
    <div className="cc-history-list">{items.map((item, index) => <details key={item.jobId} open={index === 0} className="cc-history-version">
      <summary><strong>v{item.version}</strong><span className={`cc-history-status ${item.status.toLowerCase()}`}>{status(item.status)}</span><span>{new Date(item.createdAt).toLocaleString()}</span><span>{item.nodes.length} nodes</span><span className="cc-history-tags" aria-label="Change types and roles">{[...new Set(item.nodes.flatMap(changeTags))].map(tag => <span className="cc-history-tag" key={tag}>{tag}</span>)}</span></summary>
      <div className="cc-history-meta"><span>Requested by: {item.requestedBy || 'Not recorded'}</span><span>Completed: {item.completedAt ? new Date(item.completedAt).toLocaleString() : 'Not completed'}</span>{item.retryCount > 0 && <span>Retries: {item.retryCount}</span>}<Link to={`/jobs/${item.jobId}`}>View job and backup logs</Link></div>
      {item.nodes.map((node, n) => <div key={`${node.nodeId}-${n}`} className="cc-history-node"><div className="cc-history-node-heading"><strong>Node {node.nodeId} · {node.hostname || node.hostId || 'Host not recorded'} ({node.ipAddress || 'IP not reported'})</strong><span className={`cc-history-status ${node.status.toLowerCase()}`}>{status(node.status)}</span><span className="cc-history-tags">{changeTags(node).map(tag => <span className="cc-history-tag" key={tag}>{tag}</span>)}</span><span>{node.mode === 'DELETE' ? 'Deletion · rolling restart' : node.mode === 'STATIC' ? 'File update · rolling restart' : node.mode === 'DYNAMIC' ? 'Dynamic update' : 'Operation not recorded'}</span></div>{node.configPath && <p className="cc-history-path">{node.configPath}</p>}
        <div className="cc-table-wrap"><table className="cc-history-table"><thead><tr><th>Property</th><th>Old value</th><th aria-label="Changes to">→</th><th>Requested new value</th></tr></thead><tbody>{node.changes.map(change => <tr key={change.key}><td><code>{change.key}</code>{change.previousSource && <small>Previous broker dynamic override</small>}</td><td><code>{value(change.oldValue)}</code></td><td>→</td><td><code>{value(change.newValue, node.mode === 'DELETE')}</code></td></tr>)}</tbody></table></div>
        {node.changes.length === 0 && <p>No editable property details were recorded for this step.</p>}
      </div>)}
    </details>)}</div>
    {result && result.total > 20 && <div className="cc-history-pagination"><button className="cc-refresh" disabled={!page || loading} onClick={() => setPage(current => current - 1)}>Newer changes</button><span>Page {page + 1}</span><button className="cc-refresh" disabled={!result.hasMore || loading} onClick={() => setPage(current => current + 1)}>Older changes</button></div>}
  </section>;
}
