import { useEffect, useMemo, useRef, useState } from 'react';
import { useParams } from 'react-router-dom';
import { formatIstDateTime } from '../utils/istLogTime';
import { getValidToken } from '../services/KeycloakService';
import './ClusterLiveLogs.css';

type Source = { id: string; hostId: string; role: string; hostname?: string; nodeId?: number };
type Entry = { id: string; time: string; level: string; message: string };
const roles: Record<string, string> = {
  broker: 'Broker', controller: 'Controller', broker_controller: 'Broker / Controller',
  broker_zookeeper: 'Broker',
};
const serviceGroups = [
  { label: 'Brokers', roles: ['broker', 'broker_controller', 'broker_zookeeper'] },
  { label: 'Controllers', roles: ['controller', 'broker_controller'] },
];
const inputTime = (offset: number) => new Date(Date.now() + 19800000 + offset).toISOString().slice(0, 16);
const levels = ['FATAL', 'ERROR', 'WARN', 'INFO', 'DEBUG', 'TRACE'];
const ist = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/;

async function get<T>(url: string, signal: AbortSignal): Promise<T> {
  const token = await getValidToken();
  const response = await fetch(url, {
    signal, cache: 'no-store', headers: token ? { Authorization: `Bearer ${token}` } : {},
  });
  if (!response.ok) {
    const body = await response.json().catch(() => null);
    throw new Error(body?.message || `Could not load logs (HTTP ${response.status}).`);
  }
  return response.json();
}

function entries(journal: string): Entry[] {
  return journal.split('\n').flatMap(line => {
    if (!line) return [];
    try {
      const data = JSON.parse(line);
      const millis = Number(data.__REALTIME_TIMESTAMP) / 1000;
      if (typeof data.__CURSOR !== 'string' || !Number.isFinite(millis)) return [];
      const message = typeof data.MESSAGE === 'string' ? data.MESSAGE
        : Array.isArray(data.MESSAGE) && data.MESSAGE.every((byte: unknown) => typeof byte === 'number')
          ? new TextDecoder().decode(Uint8Array.from(data.MESSAGE)) : '';
      const statedLevel = message.match(/^(?:\[?\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[^ ]*\]?\s+)?\[?(FATAL|ERROR|WARN|INFO|DEBUG|TRACE)\b/);
      const priority = Number(data.PRIORITY ?? 6);
      const level = statedLevel?.[1] ?? (priority <= 2 ? 'FATAL' : priority <= 3 ? 'ERROR'
        : priority === 4 ? 'WARN' : priority >= 7 ? 'DEBUG' : 'INFO');
      return [{ id: data.__CURSOR, time: new Date(millis).toISOString(), level, message }];
    } catch {
      return [];
    }
  });
}

export function ClusterLiveLogs() {
  const { id } = useParams();
  const base = `/api/v1/ui/clusters/${id}/live-logs`;
  const [sources, setSources] = useState<Source[]>([]);
  const [source, setSource] = useState('');
  const [hours, setHours] = useState('1');
  const [from, setFrom] = useState(() => inputTime(-3600000));
  const [to, setTo] = useState(() => inputTime(0));
  const [group, setGroup] = useState(0);
  const [interval, setIntervalSeconds] = useState(15);
  const consoleRef = useRef<HTMLDivElement>(null);
  const [level, setLevel] = useState('');
  const [search, setSearch] = useState('');
  const [following, setFollowing] = useState(false);
  const [refresh, setRefresh] = useState(0);
  const [result, setResult] = useState<{ key: string; rows: Entry[] }>();
  const [problem, setProblem] = useState<{ key: string; message: string }>();
  const [inventoryError, setInventoryError] = useState('');
  const custom = hours === 'custom';
  const valid = !custom || (ist.test(from) && ist.test(to) && from <= to);
  const key = JSON.stringify([base, source, hours, from, to, refresh]);
  const rows = result?.key === key ? result.rows : undefined;
  const error = problem?.key === key ? problem.message : '';

  useEffect(() => {
    const abort = new AbortController();
    void get<Source[]>(base + '/sources', abort.signal).then(data => {
      setSources(data);
      const first = serviceGroups.findIndex(item => data.some(node => item.roles.includes(node.role)));
      setGroup(Math.max(first, 0));
      setSource(first < 0 ? '' : data.find(node => serviceGroups[first].roles.includes(node.role))?.id || '');
      setInventoryError(data.length ? '' : 'No managed log sources are available.');
    }).catch(err => { if (!abort.signal.aborted) setInventoryError(err.message); });
    return () => abort.abort();
  }, [base]);

  useEffect(() => {
    if (!source || !valid || !sources.some(item => item.id === source)) return;
    if (!following && result?.key === key) return;
    const abort = new AbortController();
    let timer: number | undefined;
    async function read() {
      const start = custom ? new Date(from + ':00+05:30') : new Date(Date.now() - Number(hours) * 3600000);
      const query = new URLSearchParams({ source, from: start.toISOString() });
      if (custom) query.set('to', new Date(to + ':59.999+05:30').toISOString());
      try {
        const data = await get<{ journal: string }>(base + '?' + query, abort.signal);
        if (abort.signal.aborted) return;
        setResult({ key, rows: entries(data.journal) });
        setProblem(undefined);
      } catch (err) {
        if (!abort.signal.aborted) setProblem({ key, message: err instanceof Error ? err.message : 'Could not load logs.' });
      }
      if (following && !custom && !abort.signal.aborted) timer = window.setTimeout(read, interval * 1000);
    }
    void read();
    return () => {
      abort.abort();
      window.clearTimeout(timer);
    };
  // Reuse the displayed snapshot when pausing. A result update must not restart polling.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [base, custom, following, from, hours, interval, key, source, sources, to, valid]);

  const filtered = useMemo(() => (rows ?? []).filter(row =>
    (!level || row.level === level) && row.message.toLowerCase().includes(search.trim().toLowerCase())),
  [rows, level, search]);

  function goToLatest() {
    const panel = consoleRef.current;
    if (panel) panel.scrollTop = panel.scrollHeight;
  }

  useEffect(() => {
    if (following) goToLatest();
  }, [rows, following]);

  function download() {
    const content = filtered.map(row => `${formatIstDateTime(row.time)} ${row.level} ${row.message}`).join('\n');
    const url = URL.createObjectURL(new Blob([content], { type: 'text/plain;charset=utf-8' }));
    const link = document.createElement('a');
    link.href = url;
    link.download = 'tantor-cluster-logs.txt';
    link.click();
    window.setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  return <section className="cluster-live-logs">
    <header><div><h2>Service Logs</h2><p>Read the latest 200 journal entries for a managed service.</p></div>
      <div className="cluster-live-actions">
        <button disabled={!filtered.length} onClick={download}>Download</button>
        <button disabled={!source || !valid} onClick={() => setRefresh(value => value + 1)}>Refresh</button>
        <label>Refresh interval <select value={interval} onChange={event => setIntervalSeconds(Number(event.target.value))}>
          {[5, 15, 30].map(value => <option value={value} key={value}>{value} seconds</option>)}
        </select></label>
        <button disabled={custom || !source} aria-pressed={following} onClick={() => setFollowing(value => !value)}>
          {following ? 'Pause' : 'Follow live'}
        </button>
      </div>
    </header>
    <div className="cluster-live-services" aria-label="Log services">
      {serviceGroups.map((item, index) => {
        const nodes = sources.filter(node => item.roles.includes(node.role));
        return <button key={item.label} disabled={!nodes.length} aria-pressed={index === group} onClick={() => {
          setGroup(index);
          setSource(nodes[0]?.id || '');
        }}>{item.label}</button>;
      })}
    </div>
    <div className="cluster-live-filters">
      <label>Node<select value={source} onChange={event => setSource(event.target.value)}>
        {!sources.length && <option value="">No sources</option>}
        {sources.filter(item => serviceGroups[group].roles.includes(item.role)).map(item => <option key={item.id} value={item.id}>
          {item.hostname || item.hostId} · {roles[item.role]}{item.nodeId == null ? '' : ` #${item.nodeId}`}
        </option>)}
      </select></label>
      <label>Level<select value={level} onChange={event => setLevel(event.target.value)}>
        <option value="">All levels</option>{levels.map(value => <option key={value}>{value}</option>)}
      </select></label>
      <label>Time range<select value={hours} onChange={event => {
        setHours(event.target.value);
        if (event.target.value === 'custom') setFollowing(false);
      }}>
        <option value="0.08333333333333333">Last 5 minutes</option><option value="0.25">Last 15 minutes</option>
        <option value="1">Last hour</option><option value="6">Last 6 hours</option>
        <option value="12">Last 12 hours</option><option value="24">Last 24 hours</option><option value="custom">Custom range</option>
      </select></label>
      <label>From (IST)<input type="datetime-local" value={from} max={to || undefined} onChange={event => {
        setFrom(event.target.value);
        setHours('custom');
        setFollowing(false);
      }} /></label>
      <label>To (IST)<input type="datetime-local" value={to} min={from || undefined} onChange={event => {
        setTo(event.target.value);
        setHours('custom');
        setFollowing(false);
      }} /></label>
      <label>Search messages<input type="search" value={search} onChange={event => setSearch(event.target.value)} /></label>
    </div>
    {(inventoryError || error) && <p role="alert">{inventoryError || error}</p>}
    <div className="cluster-live-toolbar">
      <span>{filtered.length} of {rows?.length ?? 0} entries</span>
      <button disabled={!filtered.length} onClick={goToLatest}>Go to latest</button>
    </div>
    <div className="cluster-live-console" ref={consoleRef} tabIndex={0} aria-label="Service journal">
      {!valid ? <p>Choose a start and end time in IST.</p> : !rows ? <p>{source && !error ? 'Reading journal…' : 'No journal loaded.'}</p>
        : filtered.length === 0 ? <p>No entries match the current filters.</p>
          : filtered.map(row => <div key={row.id} className="cluster-live-entry">
            <time dateTime={row.time}>{formatIstDateTime(row.time)}</time>
            <span data-level={row.level}>{row.level}</span><span>{row.message}</span>
          </div>)}
    </div>
  </section>;
}
