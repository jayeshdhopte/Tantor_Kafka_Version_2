import { useState, useEffect, useCallback } from 'react';
import { createPortal } from 'react-dom';
import { useParams } from 'react-router-dom';
import { Users, Search, ChevronLeft, ChevronRight, X, ArrowUp, RefreshCw } from 'lucide-react';
import './Consumers.css';

interface ConsumerGroupSummaryDto {
  groupId: string;
  state: string;
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

  // Detail Modal State
  const [selectedGroupId, setSelectedGroupId] = useState<string | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailData, setDetailData] = useState<ConsumerGroupDetailDto | null>(null);

  const fetchGroups = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`/api/v1/clusters/${id}/consumer-groups?page=${page}&size=${size}&search=${encodeURIComponent(searchQuery)}&sortBy=${sortBy}`);
      if (!res.ok) {
        const errorData = await res.json().catch(() => ({}));
        throw new Error(errorData.message || `Failed to load consumer groups (HTTP ${res.status})`);
      }
      const json = await res.json();
      setData(json);
    } catch (e: unknown) {
      console.error(e);
      setError(e instanceof Error ? e.message : 'Failed to load consumer groups');
    } finally {
      setLoading(false);
    }
  }, [id, page, size, searchQuery, sortBy]);

  useEffect(() => {
    void (async () => { await fetchGroups(); })();
  }, [fetchGroups]);

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
                    <th>Health</th>
                    <th>Last Updated</th>
                  </tr>
                </thead>
                <tbody>
                  {data?.content.map(g => (
                    <tr key={g.groupId} className="consumer-group-row" onClick={() => handleRowClick(g.groupId)}>
                      <td className="consumer-group-id" title={g.groupId}>{g.groupId}</td>
                      <td>{g.state}</td>
                      <td>{g.membersCount}</td>
                      <td>{g.totalLag}</td>
                      <td>{g.health.charAt(0) + g.health.slice(1).toLowerCase()}</td>
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
          <div className="modal-content" onClick={e => e.stopPropagation()} style={{
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
                <button onClick={() => setSelectedGroupId(null)} style={{ background: 'none', border: 'none', cursor: 'pointer', padding: '4px', display: 'flex', alignItems: 'center' }}>
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
                  {/* Info Cards */}
                  <div style={{ display: 'flex', gap: 'var(--space-4)', padding: '0 32px 24px 32px' }}>
                    <div style={{
                      boxSizing: 'border-box',
                      display: 'flex',
                      flexDirection: 'column',
                      justifyContent: 'center',
                      alignItems: 'flex-start',
                      padding: '16px 20px',
                      gap: 'var(--space-2)',
                      width: '280px',
                      height: '84px',
                      background: '#F9F9FB',
                      border: '1px solid var(--border-subtle)',
                      borderRadius: 'var(--radius-md)'
                    }}>
                      <span style={{ fontSize: 'var(--text-xs)', fontWeight: 'var(--font-medium)', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>State</span>
                      <span style={{
                        fontSize: '18px',
                        fontWeight: 'var(--font-semibold)',
                        color: detailData.state === 'Stable' ? '#10B981' : '#EF4444'
                      }}>{detailData.state}</span>
                    </div>
                    <div style={{
                      boxSizing: 'border-box',
                      display: 'flex',
                      flexDirection: 'column',
                      justifyContent: 'center',
                      alignItems: 'flex-start',
                      padding: '16px 20px',
                      gap: 'var(--space-2)',
                      width: '280px',
                      height: '84px',
                      background: '#F9F9FB',
                      border: '1px solid var(--border-subtle)',
                      borderRadius: 'var(--radius-md)'
                    }}>
                      <span style={{ fontSize: 'var(--text-xs)', fontWeight: 'var(--font-medium)', color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>Total Members</span>
                      <span style={{ fontSize: '18px', fontWeight: 'var(--font-semibold)', color: '#10B981' }}>{detailData.members.length}</span>
                    </div>
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
                                  <th style={{ padding: '12px 16px', textAlign: 'left', fontWeight: 'var(--font-medium)', color: 'var(--button-primary-active)' }}>Topic</th>
                                  <th style={{ padding: '12px 16px', textAlign: 'left', fontWeight: 'var(--font-medium)', color: 'var(--button-primary-active)' }}>Partition</th>
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
