import { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import { Loader2 } from 'lucide-react';
import './ClusterNodes.css';

type ClusterType = 'INTERNAL' | 'EXTERNAL';
type KafkaMode = 'KRAFT' | 'ZOOKEEPER' | 'UNKNOWN';
type NodeRole = 'BROKER' | 'CONTROLLER' | 'BROKER_CONTROLLER';
type AgentStatus = 'ONLINE' | 'OFFLINE' | 'NOT_ENROLLED' | 'UNKNOWN';
type TelemetryStatus = 'LIVE' | 'STALE' | 'UNAVAILABLE' | 'UNKNOWN';

interface CanonicalClusterIdentity {
  clusterUuid: string;
  kafkaClusterId: string | null;
  type: ClusterType;
  mode: KafkaMode;
}

interface CanonicalNodeIdentity {
  clusterUuid: string;
  kafkaClusterId: string;
  nodeId: number;
  role: NodeRole;
}

interface CanonicalNode {
  identity: CanonicalNodeIdentity;
  host: string | null;
  hostname: string | null;
  ipAddress: string | null;
  agentStatus: AgentStatus;
  telemetryStatus: TelemetryStatus;
}

interface CanonicalClusterNodesResponse {
  cluster: CanonicalClusterIdentity;
  nodes: CanonicalNode[];
}

const readableRole = (role: NodeRole) =>
  role === 'BROKER_CONTROLLER'
    ? 'Broker + Controller'
    : role.charAt(0) + role.slice(1).toLowerCase();

const readableStatus = (status: AgentStatus | TelemetryStatus) =>
  status.replaceAll('_', ' ').toLowerCase().replace(/\b\w/g, letter => letter.toUpperCase());

const statusClass = (status: AgentStatus | TelemetryStatus) =>
  status.toLowerCase().replaceAll('_', '-');

export function ClusterNodes() {
  const { id } = useParams<{ id: string }>();
  const [, setCluster] = useState<CanonicalClusterIdentity | null>(null);
  const [nodes, setNodes] = useState<CanonicalNode[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const abortController = new AbortController();

    const loadNodes = async () => {
      try {
        const res = await fetch(`/api/v1/clusters/${id}/nodes`, {
          signal: abortController.signal,
        });
        const data = await res.json().catch(() => null);
        if (!res.ok) {
          throw new Error(data?.message || `Failed to fetch nodes (${res.status})`);
        }
        const canonical = data as CanonicalClusterNodesResponse;
        if (!canonical?.cluster || !Array.isArray(canonical.nodes)) {
          throw new Error('The server returned an invalid canonical node response.');
        }
        setCluster(canonical.cluster);
        setNodes(canonical.nodes);
        setError(null);
      } catch (requestError) {
        if (abortController.signal.aborted) return;
        console.error(requestError);
        setCluster(null);
        setNodes([]);
        setError(requestError instanceof Error ? requestError.message : 'Failed to fetch nodes');
      } finally {
        if (!abortController.signal.aborted) setLoading(false);
      }
    };

    void loadNodes();
    return () => abortController.abort();
  }, [id]);

  if (loading && nodes.length === 0) {
    return <div className="state-center"><Loader2 className="spin" /> Loading nodes...</div>;
  }

  return (
    <div className="cluster-nodes-page animate-fade-in">
      <header className="page-header">
        <div>
          <h2 className="cluster-section-heading">Cluster Nodes</h2>

        </div>
      </header>

      {error && <div className="cluster-nodes-error">{error}</div>}

      <div className="cluster-nodes-table-wrap">
        <table className="cluster-nodes-table">
          <thead><tr>
            <th>Node ID</th>
            <th>Hostname</th>
            <th>IP Address</th>
            <th>Role</th>
            <th>Agent status</th>
            <th>Telemetry status</th>
          </tr></thead>
          <tbody>
            {nodes.map(node => (
              <tr key={`${node.identity.clusterUuid}-${node.identity.kafkaClusterId}-${node.identity.nodeId}-${node.identity.role}`}>
                <td><code>{node.identity.nodeId}</code></td>
                <td><span className="cluster-node-host">{node.hostname || 'Not reported'}</span></td>
                <td><span className="cluster-node-host">{node.ipAddress || 'Not reported'}</span></td>
                <td><span className="cluster-node-role">{readableRole(node.identity.role)}</span></td>
                <td>
                  <span className={`cluster-node-status ${statusClass(node.agentStatus)}`}>
                    {readableStatus(node.agentStatus)}
                  </span>
                </td>
                <td>
                  <span className={`cluster-node-status ${statusClass(node.telemetryStatus)}`}>
                    {readableStatus(node.telemetryStatus)}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {!error && nodes.length === 0 && (
          <div className="empty-state">No canonical Kafka nodes are recorded for this cluster.</div>
        )}
      </div>
    </div>
  );
}
