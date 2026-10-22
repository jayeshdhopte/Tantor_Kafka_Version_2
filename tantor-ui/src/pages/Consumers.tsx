import { useState, useEffect, useCallback, useRef } from 'react';
import { createPortal } from 'react-dom';
import { useParams } from 'react-router-dom';
import { Users, Search, ChevronLeft, ChevronRight, X, ArrowUp, RefreshCw } from 'lucide-react';
import './Consumers.css';
import { KafkaGroupPanel } from '../components/KafkaGroupPanel';

interface ConsumerGroupSummaryDto {
  groupId: string;
  state: string;
  coordinatorId?: number | null;
  membersCount: number;
  totalLag: number;
  health: 'HEALTHY' | 'WARNING' | 'INACTIVE' | 'CRITICAL';
  lastUpdated: number;
}

interface PaginatedResponse<T> {
  content: T[];
  page: number;
  size: number;
  totalElements: number;
  totalPages: number;
  hasNext: boolean;
}

interface PartitionLagDto {
  topic: string;
  partition: number;
  currentOffset: number;
  logEndOffset: number;
  lag: number;
}

interface ConsumerGroupMemberDto {
  memberId: string;
  clientId: string;
  host: string;
  partitions: PartitionLagDto[];
}

interface ConsumerGroupDetailDto {
  groupId: string;
  state: string;
  coordinatorId?: number | null;
  members: ConsumerGroupMemberDto[];
}

export function Consumers() {
  const { id } = useParams<{ id: string }>();
  const [data, setData] = useState<PaginatedResponse<ConsumerGroupSummaryDto> | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [page, setPage] = useState(0);
  const [size] = useState(10);
  const [searchInput, setSearchInput] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [sortBy, setSortBy] = useState('totalLag');
  const [refreshInterval, setRefreshInterval] = useState(0);
  const activeRequest = useRef<{ key: string; controller: AbortController } | null>(null);

  // Detail Modal State
  const [selectedGroupId, setSelectedGroupId] = useState<string | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailData, setDetailData] = useState<ConsumerGroupDetailDto | null>(null);

  const fetchGroups = useCallback(async () => {
    const key = JSON.stringify([id, page, size, searchQuery, sortBy]);
    if (activeRequest.current?.key === key) return;
    activeRequest.current?.controller.abort();
    const controller = new AbortController();
    activeRequest.current = { key, controller };
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`/api/v1/clusters/${id}/consumer-groups?page=${page}&size=${size}&search=${encodeURIComponent(searchQuery)}&sortBy=${sortBy}`, { signal: controller.signal });
      if (!res.ok) {
        const errorData = await res.json().catch(() => ({}));
        throw new Error(errorData.message || `Failed to load consumer groups (HTTP ${res.status})`);
      }
      const json = await res.json();
      if (!controller.signal.aborted && activeRequest.current?.controller === controller) setData(json);
    } catch (e: unknown) {
      if (!controller.signal.aborted && activeRequest.current?.controller === controller) {
        console.error(e);
        setError(e instanceof Error ? e.message : 'Failed to load consumer groups');
      }
    } finally {
      if (activeRequest.current?.controller === controller) {
        activeRequest.current = null;
        setLoading(false);
      }
    }
  }, [id, page, size, searchQuery, sortBy]);

  useEffect(() => {
    void (async () => { await fetchGroups(); })();
    return () => { activeRequest.current?.controller.abort(); activeRequest.current = null; };
  }, [fetchGroups]);

  useEffect(() => {
    if (!refreshInterval) return;
    const timer = window.setInterval(() => {
      if (document.visibilityState === 'visible' && !activeRequest.current) void fetchGroups();
    }, refreshInterval * 1000);
    return () => window.clearInterval(timer);
  }, [fetchGroups, refreshInterval]);

  const handleSearchSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setPage(0);
    setSearchQuery(searchInput);
  };

  const handleSort = (column: string) => {
    setSortBy(column);
    setPage(0);
  };

  const handleRowClick = async (groupId: string) => {
    setSelectedGroupId(groupId);
    setDetailLoading(true);
    setDetailData(null);
    try {
      const res = await fetch(`/api/v1/clusters/${id}/consumer-groups/${encodeURIComponent(groupId)}`);
      if (res.ok) {
        setDetailData(await res.json());
      }
    } catch (e) {
      console.error(e);
    } finally {
      setDetailLoading(false);
    }
  };

  return (
    <div className="consumers-tab animate-fade-in">
      <div className="consumers-header">
        <h2 className="cluster-section-heading">Consumer Groups</h2>
        <div className="consumer-groups-toolbar">
          <form className="consumer-search-form" onSubmit={handleSearchSubmit}>
            <label className="consumer-search-field">
              <Search size={16} />
              <input
                type="text"
                placeholder="Search key or value"
                value={searchInput}
                onChange={(event) => setSearchInput(event.target.value)}
              />
            </label>
          </form>
          <div className="consumer-refresh-controls">
          <label className="consumer-refresh-rate"><span>Refresh rate:</span>
            <select aria-label="Consumer groups refresh rate" value={refreshInterval} onChange={event => setRefreshInterval(Number(event.target.value))}>
              <option value={0}>Off</option>
              {[2, 5, 10, 15].map(seconds => <option key={seconds} value={seconds}>{seconds} sec</option>)}
            </select>
          </label>
          <button
            type="button"
            className="consumer-refresh-button"
            onClick={fetchGroups}
            disabled={loading}
            aria-label="Refresh consumer groups"
          >
            <RefreshCw className={loading ? 'spin' : ''} size={15} />
          </button>
          </div>
        </div>
      </div>

      <KafkaGroupPanel clusterId={id} kind="other" />
      {error && (
        <div className="error-alert" style={{ marginBottom: '1rem' }}>
          {error}
        </div>
      )}

      <div className="table-card">
        {loading && !data ? (
          <div className="empty-state">Loading consumer groups...</div>
        ) : data?.content.length === 0 ? (
          <div className="empty-state">
            <Users size={32} style={{ color: 'var(--text-secondary)' }} />
            <p className="empty-title">No active consumer groups</p>
            <p className="empty-subtitle">There are no consumers matching your search criteria.</p>
          </div>
        ) : (
          <>
            <div className="consumer-table-scroll">
              <table className="consumer-groups-table">
                <thead>
                  <tr>
                    <th>
                      <button type="button" className="consumer-sort-button" onClick={() => handleSort('groupId')}>
                        Group ID <ArrowUp size={14} aria-hidden="true" />
                      </button>
                    </th>
                    <th>
                      <button type="button" className="consumer-sort-button" onClick={() => handleSort('state')}>
                        State
                      </button>
                    </th>
                    <th>Members</th>
                    <th>
                      <button type="button" className="consumer-sort-button" onClick={() => handleSort('totalLag')}>
                        Total Lag
                      </button>
                    </th>
                    <th>Coordinator ID</th>
                    <th>Last Updated</th>
                  </tr>
                </thead>
                <tbody>
                  {data?.content.map(g => (
                    <tr key={g.groupId} className="consumer-group-row" onClick={() => handleRowClick(g.groupId)}>
                      <td className="consumer-group-id" title={g.groupId}>{g.groupId}</td>
                      <td>{g.state}</td>
                      <td>{g.membersCount < 0 ? '—' : g.membersCount}</td>
                      <td>{g.totalLag < 0 ? 'Unavailable' : g.totalLag}</td>
                      <td>{g.coordinatorId ?? 'Unavailable'}</td>
                      <td className="consumer-last-updated">
                        {new Date(g.lastUpdated).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Pagination Controls */}
            {data && data.totalPages > 1 && (
              <div className="pagination">
                <span style={{ fontSize: '0.875rem', color: 'var(--text-secondary)' }}>
                  Showing {page * size + 1} to {Math.min((page + 1) * size, data.totalElements)} of {data.totalElements} results
                </span>
                <div className="pagination-controls">
                  <button
                    className="pagination-btn"
                    disabled={page === 0}
                    onClick={() => setPage(p => p - 1)}
                  >
                    <ChevronLeft size={16} /> Previous
                  </button>
                  <button
                    className="pagination-btn"
                    disabled={!data.hasNext}
                    onClick={() => setPage(p => p + 1)}
                  >
                    Next <ChevronRight size={16} />
                  </button>
                </div>
              </div>
            )}
          </>
        )}
      </div>

      {/* Detail Modal — rendered via portal to escape scroll container stacking context */}
      {selectedGroupId ? createPortal(
        <div className="modal-overlay" onClick={() => setSelectedGroupId(null)} style={{
          fontFamily: 'Satoshi, Inter, sans-serif'
        }}>
          <div className="modal-content" role="dialog" aria-modal="true" aria-label="Consumer Group Details" onClick={e => e.stopPropagation()} style={{
            background: "var(--bg-surface)",
            borderRadius: '16px',
            width: '100%',
            maxWidth: '780px',
            boxShadow: '0 10px 25px rgba(0,0,0,0.1)',
            overflow: 'hidden',
            padding: 0
          }}>
            {/* Modal Header */}
            <div style={{ padding: '24px 32px 12px 32px' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <h3 style={{ margin: 0, fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-semibold)', fontSize: 'var(--text-xl)', color: 'var(--button-primary-active)' }}>Consumer Group Details</h3>
                <button aria-label="Close consumer group details" onClick={() => setSelectedGroupId(null)} style={{ background: 'none', border: 'none', cursor: 'pointer', padding: '4px', display: 'flex', alignItems: 'center' }}>
                  <X size={20} color="#94a3b8" />
                </button>
              </div>
              <span style={{ fontSize: 'var(--text-sm)', color: 'var(--text-muted)', display: 'block', marginTop: '2px' }}>{selectedGroupId}</span>
            </div>

            <div className="modal-body" style={{ padding: 0 }}>
              {detailLoading ? (
                <div style={{ padding: '32px', textAlign: 'center', color: 'var(--text-muted)' }}>Loading details...</div>
              ) : detailData ? (
                <div>
                  <div className="consumer-detail-summary">
                    <article><span>State</span><strong style={{ color: detailData.state === 'Stable' ? '#10B981' : '#EF4444' }}>{detailData.state}</strong></article>
                    <article><span>Total Members</span><strong>{detailData.members.length}</strong></article>
                    <article><span>Coordinator ID</span><strong>{detailData.coordinatorId ?? 'Unavailable'}</strong></article>
                  </div>

                  {/* Section Title */}
                  <div style={{ padding: '0 32px 12px 32px' }}>
                    <h4 style={{ margin: 0, fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-semibold)', fontSize: 'var(--text-md)', color: 'var(--button-primary-hover)' }}>Members &amp; Partitions</h4>
                  </div>

                  {/* Members List */}
                  <div style={{ padding: '0 32px 32px 32px', maxHeight: '420px', overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: 'var(--space-6)' }}>
                    {detailData.members.length === 0 ? (
                      <p style={{ color: 'var(--text-muted)', fontSize: 'var(--text-base)', margin: 0 }}>No active members or assigned partitions found for this group.</p>
                    ) : (
                      detailData.members.map((m, i) => (
                        <div key={i} style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
                          {/* Member Headers */}
                          <div style={{ display: 'flex', gap: '48px', fontSize: 'var(--text-sm)', color: 'var(--button-primary-active)' }}>
                            <div>
                              <div style={{ fontWeight: 'var(--font-medium)', color: 'var(--text-muted)', marginBottom: '4px' }}>Member ID</div>
                              <div style={{ fontFamily: 'monospace' }}>{m.memberId}</div>
                            </div>
                            <div>
                              <div style={{ fontWeight: 'var(--font-medium)', color: 'var(--text-muted)', marginBottom: '4px' }}>Client ID</div>
                              <div>{m.clientId}</div>
                            </div>
                            <div>
                              <div style={{ fontWeight: 'var(--font-medium)', color: 'var(--text-muted)', marginBottom: '4px' }}>Host</div>
                              <div>{m.host}</div>
                            </div>
                          </div>

                          {/* Partitions Table */}
                          <div style={{ border: '1px solid var(--border-subtle)', borderRadius: 'var(--radius-lg)', overflow: 'hidden' }}>
                            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 'var(--text-base)', fontFamily: 'Satoshi, sans-serif' }}>
                              <thead>
                                <tr style={{ background: '#F9F9FB', borderBottom: '1px solid var(--border-subtle)' }}>
                                  <th style={{ padding: '12px 16px', textAlign: 'left', fontWeight: 'var(--font-medium)', color: 'var(--button-primary-active)' }}>Assigned Topic</th>
                                  <th style={{ padding: '12px 16px', textAlign: 'left', fontWeight: 'var(--font-medium)', color: 'var(--button-primary-active)' }}>Assigned Partition</th>
                                  <th style={{ padding: '12px 16px', textAlign: 'left', fontWeight: 'var(--font-medium)', color: 'var(--button-primary-active)' }}>Current Offset</th>
                                  <th style={{ padding: '12px 16px', textAlign: 'left', fontWeight: 'var(--font-medium)', color: 'var(--button-primary-active)' }}>Lag</th>
                                </tr>
                              </thead>
                              <tbody>
                                {m.partitions.length === 0 ? (
                                  <tr>
                                    <td colSpan={4} style={{ padding: 'var(--space-4)', textAlign: 'center', color: 'var(--text-muted)' }}>No partitions assigned</td>
                                  </tr>
                                ) : (
                                  m.partitions.map((p, idx) => (
                                    <tr key={`${p.topic}-${p.partition}`} style={{ borderBottom: idx === m.partitions.length - 1 ? 'none' : '1px solid var(--border-subtle)' }}>
                                      <td style={{ padding: '12px 16px', color: 'var(--button-primary-active)' }}>{p.topic}</td>
                                      <td style={{ padding: '12px 16px', color: 'var(--button-primary-active)' }}>{p.partition}</td>
                                      <td style={{ padding: '12px 16px', color: 'var(--button-primary-active)' }}>
                                        {p.currentOffset === -1 ? 'Unknown' : p.currentOffset.toLocaleString()}
                                      </td>
                                      <td style={{ padding: '12px 16px', fontWeight: p.lag > 0 ? 600 : 400, color: p.lag > 0 ? '#F97316' : 'var(--button-primary-active)' }}>
                                        {p.lag === -1 ? 'Unknown' : p.lag.toLocaleString()}
                                      </td>
                                    </tr>
                                  ))
                                )}
                              </tbody>
                            </table>
                          </div>
                        </div>
                      ))
                    )}
                  </div>
                </div>
              ) : (
                <div style={{ padding: '32px', textAlign: 'center', color: '#ef4444' }}>Failed to load group details.</div>
              )}
            </div>
          </div>
        </div>
        , document.body) : null}
    </div>
  );
}
