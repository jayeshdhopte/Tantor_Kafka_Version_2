import { RefreshButton } from './RefreshButton';
import { useCallback, useEffect, useState } from 'react';
import { RefreshCw } from 'lucide-react';
import './KafkaGroupPanel.css';

interface Group { groupId: string; protocol: string; groupType: string; state: string; classification: string }
export function KafkaGroupPanel({ clusterId, kind }: { clusterId?: string; kind: 'schema' | 'connect' | 'other' }) {
  const [groups, setGroups] = useState<Group[]>([]);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [revision, setRevision] = useState(0);
  const refresh = useCallback(() => setRevision(value => value + 1), []);
  useEffect(() => {
    if (!clusterId) return;
    const controller = new AbortController();
    void (async () => {
      setLoading(true); setError(''); setGroups([]);
      try {
        const response = await fetch(`/api/v1/clusters/${clusterId}/groups`, { signal: controller.signal });
        const body = await response.json();
        if (!response.ok) throw new Error(body.message || 'Unable to read group metadata.');
        if (!controller.signal.aborted) setGroups(body as Group[]);
      } catch (failure) {
        if (!controller.signal.aborted) setError(failure instanceof Error ? failure.message : 'Unable to read group metadata.');
      } finally { if (!controller.signal.aborted) setLoading(false); }
    })();
    return () => controller.abort();
  }, [clusterId, revision]);
  const visible = groups.filter(group => kind === 'schema' ? group.classification === 'SCHEMA_REGISTRY_COORDINATION'
    : kind === 'connect' ? group.classification === 'CONNECT_COORDINATION' : ['OTHER', 'UNKNOWN'].includes(group.classification));
  const title = kind === 'other' ? 'Other and unclassified Kafka groups' : 'Cluster service coordination groups';
  return <details className="kafka-group-panel">
    <summary>{title}{error ? ' — metadata unavailable' : loading ? ' — loading' : ` (${visible.length})`}</summary>
    <div className="kafka-group-panel-body">
      <div className="kafka-group-panel-toolbar"><p>{kind === 'other' ? 'These groups are listed separately because their protocol is unrecognized or they use a different group type.' : 'These groups coordinate service instances. They are separate from application consumer groups.'}</p>
        <RefreshButton type="button" onClick={refresh} disabled={loading} aria-label={`Refresh ${title.toLowerCase()}`}><RefreshCw size={15} className={loading ? 'spin' : ''} /> Refresh</RefreshButton></div>
      {loading ? <p role="status">Reading Kafka group metadata…</p> : error ? <p role="alert">{error}</p> : visible.length === 0 ? <p>No matching groups reported by Kafka.</p> :
        <div className="kafka-group-panel-scroll"><table><thead><tr><th>Group ID</th><th>Protocol</th><th>Group type</th><th>State</th></tr></thead><tbody>{visible.map(group => <tr key={group.groupId}><td>{group.groupId}</td><td>{group.protocol || 'Not reported'}</td><td>{group.groupType}</td><td>{group.state}</td></tr>)}</tbody></table></div>}
    </div>
  </details>;
}
