import { useState, useEffect, useCallback } from 'react';
import { createPortal } from 'react-dom';
import { Trash2, X } from 'lucide-react';
import '../pages/Hosts.css';
import { confirmAction, notifyAction } from './confirmUtils';

type AgentConnectivityModalProps = {
  onClose: () => void;
};

interface HostRecord {
  id?: string;
  hostname?: string;
  status?: string;
  agentStatus?: string;
  ipAddresses?: string;
  lastHeartbeat?: string;
  agentName?: string;
  agentPath?: string;
  [key: string]: unknown;
}

export function AgentConnectivityModal({ onClose }: AgentConnectivityModalProps) {
  const [hosts, setHosts] = useState<HostRecord[]>([]);
  const [selectedPendingIds, setSelectedPendingIds] = useState<Record<string, boolean>>({});
  const [connectingAgents, setConnectingAgents] = useState(false);

  const fetchHosts = useCallback(async () => {
    try {
      const res = await fetch('/api/v1/ui/hosts');
      if (res.ok) setHosts(await res.json());
    } catch (e) {
      console.error(e);
    }
  }, []);

  useEffect(() => {
    void (async () => { await fetchHosts(); })();
    const t = setInterval(() => { void (async () => { await fetchHosts(); })(); }, 5000);
    return () => clearInterval(t);
  }, [fetchHosts]);

  const deleteHost = async (id: string) => {
    if (!(await confirmAction('Disconnect this node? It will move back to discovered nodes and can be connected again.'))) return;
    try {
      const res = await fetch(`/api/v1/ui/hosts/${id}`, { method: 'DELETE' });
      if (res.ok) {
        fetchHosts();
      } else {
        const errorData = await res.json().catch(() => ({}));
        notifyAction(errorData.message || 'Failed to disconnect node.');
      }
    } catch (e) {
      console.error(e);
      notifyAction('An error occurred while disconnecting the node.');
    }
  };

  const parseIpList = (raw: unknown): string[] => {
    if (Array.isArray(raw)) return raw.map(String).map(ip => ip.trim()).filter(Boolean);
    if (typeof raw === 'string' && raw.startsWith('[')) {
      try {
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) return parsed.map(String).map(ip => ip.trim()).filter(Boolean);
      } catch { /* ignore */ }
    }
    if (typeof raw === 'string') return raw.split(',').map(ip => ip.trim()).filter(Boolean);
    return [];
  };

  const displayIp = (raw: unknown) => {
    const ips = parseIpList(raw);
    return ips.find(ip => ip.startsWith('192.168.'))
      || ips.find(ip => !ip.startsWith('127.') && !ip.startsWith('172.'))
      || ips[0]
      || 'Unknown';
  };

  const activeHosts = hosts.filter(h => h.status !== 'PENDING');
  const activeHostIps = new Set(activeHosts.map(host => displayIp(host.ipAddresses)));
  const pendingHosts = Array.from(
    hosts
      .filter(h => h.status === 'PENDING' && !activeHostIps.has(displayIp(h.ipAddresses)))
      .reduce<Map<string, HostRecord>>((byIp, host) => {
        const ip = displayIp(host.ipAddresses);
        const existing = byIp.get(ip);
        const heartbeat = Date.parse(String(host.lastHeartbeat ?? '')) || 0;
        const existingHeartbeat = Date.parse(String(existing?.lastHeartbeat ?? '')) || 0;
        const expectedSuffix = `-${ip.split('.').pop()}`;
        const isCanonicalId = String(host.id || '').endsWith(expectedSuffix);
        const existingIsCanonicalId = String(existing?.id || '').endsWith(expectedSuffix);

        if (
          !existing
          || (isCanonicalId && !existingIsCanonicalId)
          || (isCanonicalId === existingIsCanonicalId && heartbeat > existingHeartbeat)
        ) {
          byIp.set(ip, host);
        }
        return byIp;
      }, new Map<string, HostRecord>())
      .values(),
  );
  const selectedCount = pendingHosts.filter(host => selectedPendingIds[String(host.id)]).length;
  const allPendingSelected = pendingHosts.length > 0 && selectedCount === pendingHosts.length;

  const togglePendingHost = (id: string) => {
    setSelectedPendingIds(prev => ({ ...prev, [id]: !prev[id] }));
  };

  const toggleAllPendingHosts = () => {
    if (allPendingSelected) {
      setSelectedPendingIds({});
      return;
    }
    setSelectedPendingIds(Object.fromEntries(pendingHosts.map(host => [String(host.id), true])));
  };

  const connectSelectedAgents = async () => {
    const selectedIds = pendingHosts.filter(host => selectedPendingIds[String(host.id)]).map(host => host.id);
    if (selectedIds.length === 0) return;
    setConnectingAgents(true);
    try {
      const results = await Promise.allSettled(
        selectedIds.map(id => fetch(`/api/v1/ui/hosts/${id}/approve`, { method: 'POST' }))
      );
      const failed = results.filter(result => result.status === 'rejected'
        || (result.status === 'fulfilled' && !result.value.ok)).length;
      if (failed > 0) {
        notifyAction(`${failed} agent${failed === 1 ? '' : 's'} could not be connected. Refreshing the list now.`);
      }
      setSelectedPendingIds({});
      await fetchHosts();
    } finally {
      setConnectingAgents(false);
    }
  };

  return createPortal(
    <div className="modal-overlay" onClick={onClose} style={{ zIndex: 9999 }}>
      <div className="modal" onClick={e => e.stopPropagation()} style={{ padding: 0, overflow: 'hidden', maxWidth: '720px' }}>
        <div className="modal-header" style={{ padding: '24px 32px 16px 32px', margin: 0, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <h2 style={{ fontSize: '1.25rem', fontWeight: 'var(--font-medium)', color: '#111827', margin: 0 }}>Agent Connectivity</h2>
          <button className="modal-close" onClick={onClose} style={{ border: 'none', background: 'transparent', padding: 0, color: '#9CA3AF' }}>
            <X size={20} />
          </button>
        </div>

        {/* Separator line below header */}
        <div style={{ height: '1px', background: '#F1F5F9', margin: '0 0 0 0' }} />

        <div style={{ padding: '20px 32px 8px 32px' }}>
          {pendingHosts.length === 0 ? (
            <div className="empty-pending">
              No new nodes discovered. Run the agent script on a VM to discover it.
            </div>
          ) : (
            <div>
              {/* Section label row */}
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
                <span style={{ fontSize: '11px', color: '#94A3B8', fontWeight: 'var(--font-semibold)', textTransform: 'uppercase', letterSpacing: '0.07em' }}>
                  Discovered Nodes
                </span>
                <label style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)', margin: 0, cursor: 'pointer' }}>
                  <input
                    type="checkbox"
                    checked={allPendingSelected}
                    onChange={toggleAllPendingHosts}
                    style={{ width: '16px', height: '16px', accentColor: '#8B5CF6', cursor: 'pointer' }}
                  />
                  <span style={{ fontSize: 'var(--text-sm)', color: '#1E293B', fontWeight: 'var(--font-medium)' }}>Select all</span>
                </label>
              </div>

              {/* Node cards inside grey container */}
              <div style={{ background: '#F8FAFC', borderRadius: '10px', padding: 'var(--space-3)', display: 'flex', flexDirection: 'column', gap: 'var(--space-2)', maxHeight: '360px', overflowY: 'auto' }}>
                {pendingHosts.map(host => (
                  <div
                    key={String(host.id)}
                    onClick={() => togglePendingHost(String(host.id))}
                    style={{
                      background: "var(--bg-surface)",
                      border: selectedPendingIds[String(host.id)] ? '1.5px solid #8B5CF6' : '1px solid var(--border-subtle)',
                      borderRadius: 'var(--radius-md)',
                      padding: '12px 14px',
                      display: 'flex',
                      alignItems: 'center',
                      gap: '12px',
                      cursor: 'pointer',
                      boxShadow: '0 1px 3px rgba(0,0,0,0.04)',
                    }}
                  >
                    <label onClick={e => e.stopPropagation()} style={{ margin: 0, display: 'flex', flexShrink: 0 }}>
                      <input
                        type="checkbox"
                        checked={!!selectedPendingIds[String(host.id)]}
                        onChange={() => togglePendingHost(String(host.id))}
                        style={{ width: '16px', height: '16px', accentColor: '#8B5CF6', cursor: 'pointer' }}
                      />
                    </label>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <p style={{ fontSize: 'var(--text-base)', color: '#1E293B', fontWeight: 'var(--font-medium)', margin: '0 0 2px 0' }}>{host.agentName || host.hostname}</p>
                      <p style={{ fontSize: 'var(--text-xs)', color: '#94A3B8', margin: 0 }}>{displayIp(host.ipAddresses)} &nbsp;|&nbsp; {host.agentPath || 'Path unavailable'}</p>
                    </div>
                    <button
                      title="Reject & remove"
                      onClick={e => { e.stopPropagation(); deleteHost(String(host.id)); }}
                      style={{ border: 'none', background: 'transparent', color: '#CBD5E1', padding: '4px', cursor: 'pointer', display: 'flex', alignItems: 'center', flexShrink: 0 }}
                      onMouseEnter={e => (e.currentTarget.style.color = '#EF4444')}
                      onMouseLeave={e => (e.currentTarget.style.color = '#CBD5E1')}
                    >
                      <Trash2 size={16} />
                    </button>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>

        <div className="modal-footer" style={{ margin: '0', borderTop: '1px solid #F1F5F9', padding: '20px 32px', display: 'flex', justifyContent: 'flex-end', gap: '12px', background: "var(--bg-surface)" }}>
          <button
            className="btn"
            onClick={onClose}
            style={{ background: "var(--bg-surface)", border: '1px solid #CBD5E1', color: 'var(--text-muted)', padding: '8px 24px', borderRadius: 'var(--radius-md)', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-base)' }}
          >
            Cancel
          </button>
          <button
            className="btn btn-primary-action"
            disabled={selectedCount === 0 || connectingAgents}
            onClick={connectSelectedAgents}
            style={{
              background: "var(--bg-surface)",
              border: '1px solid #8B5CF6',
              color: '#8B5CF6',
              padding: '8px 24px',
              borderRadius: 'var(--radius-md)',
              fontWeight: 'var(--font-medium)',
              fontSize: 'var(--text-base)',
              opacity: (selectedCount === 0 || connectingAgents) ? 0.5 : 1
            }}
          >
            {connectingAgents ? 'Connecting...' : 'Connect'}
          </button>
        </div>
      </div>
    </div>
    , document.body);
}
