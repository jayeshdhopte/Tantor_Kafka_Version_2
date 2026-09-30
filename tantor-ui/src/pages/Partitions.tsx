import { useState, useEffect, useCallback } from 'react';
import { useParams } from 'react-router-dom';
import { Database, Search, ChevronLeft, ChevronRight, ArrowUp, ArrowDown, RefreshCw } from 'lucide-react';
import './Partitions.css';


interface PartitionSummaryDto {
  topicName: string;
  partitionId: number;
  leaderBroker: number;
  leaderHostname: string;
  replicaBrokers: number[];
  isrBrokers: number[];
  earliestOffset: number;
  latestOffset: number;
  messageCount: number;
  underReplicated: boolean;
  health: string;
}

interface PaginatedResponse {
  content: PartitionSummaryDto[];
  page: number;
  size: number;
  totalElements: number;
  totalPages: number;
  hasNext: boolean;
}

export function Partitions() {
  const { id } = useParams<{ id: string }>();
  const [data, setData] = useState<PaginatedResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [page, setPage] = useState(0);
  const [size, setSize] = useState(10);
  const [searchInput, setSearchInput] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [sortBy, setSortBy] = useState('topicName');

  const fetchPartitions = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`/api/v1/clusters/${id}/partitions?page=${page}&size=${size}&search=${encodeURIComponent(searchQuery)}&sortBy=${sortBy}`);
      if (!res.ok) {
        const errorData = await res.json().catch(() => ({}));
        throw new Error(errorData.message || `Partitions are not available yet (HTTP ${res.status})`);
      }
      const json = await res.json();
      setData(json);
    } catch (e: unknown) {
      console.error(e);
      setError(e instanceof Error ? e.message : "Failed to load partitions");
    } finally {
      setLoading(false);
    }
  }, [id, page, size, searchQuery, sortBy]);

  useEffect(() => {
    void (async () => { await fetchPartitions(); })();
  }, [fetchPartitions]);

  // Handle Search submit
  const handleSearchSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setPage(0); // Reset to first page on new search
    setSearchQuery(searchInput);
  };

  const handleSort = (field: string) => {
    setSortBy(field);
    setPage(0);
  };

  const renderSortIcon = (field: string) => {
    if (sortBy === field) {
      if (field === 'messageCount') return <ArrowDown size={14} style={{ display: 'inline', marginLeft: 4 }} />;
      return <ArrowUp size={14} style={{ display: 'inline', marginLeft: 4 }} />;
    }
    return null;
  };

  return (
    <div className="partitions-tab animate-fade-in" style={{ width: '100%' }}>
      <div className="topics-header" style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-start', marginBottom: '24px', gap: 'var(--space-4)' }}>
        <h2 className="cluster-section-heading">Partitions Dashboard</h2>
        
        {/* -- Figma Toolbar Search Bar & Refresh Button -- */}
        <div className="tab-toolbar" style={{ display: 'flex', gap: 'var(--space-6)', alignItems: 'center', width: '100%', height: '40px' }}>
          <form onSubmit={handleSearchSubmit} style={{ margin: 0 }}>
            <label style={{
              boxSizing: 'border-box',
              display: 'flex',
              flexDirection: 'row',
              alignItems: 'center',
              padding: '6px 8px',
              gap: 'var(--space-2)',
              width: '612px',
              height: '36px',
              background: "var(--bg-surface)",
              border: '1px solid var(--border-default)',
              borderRadius: 'var(--radius-md)'
            }}>
              <Search size={16} style={{ color: 'var(--text-tertiary)' }} />
              <input 
                type="text" 
                placeholder="Search key or value" 
                value={searchInput}
                onChange={(e) => setSearchInput(e.target.value)}
                style={{
                  border: 'none',
                  outline: 'none',
                  fontFamily: 'Satoshi, sans-serif',
                  fontWeight: 'var(--font-regular)',
                  fontSize: 'var(--text-base)',
                  color: 'var(--text-heading)',
                  width: '100%',
                  background: 'transparent'
                }}
              />
            </label>
          </form>
          <button 
            type="button"
            onClick={fetchPartitions} 
            disabled={loading} 
            style={{
              boxSizing: 'border-box',
              display: 'flex',
              flexDirection: 'row',
              alignItems: 'center',
              justifyContent: 'center',
              padding: 'var(--space-2)',
              width: '40px',
              height: '40px',
              background: "var(--bg-surface)",
              border: '1px solid var(--border-default)',
              borderRadius: 'var(--radius-md)',
              cursor: 'pointer',
              color: 'var(--text-tertiary)'
            }}
            aria-label="Refresh partitions"
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

      <div className="table-card" style={{ backgroundColor: "var(--text-light)", borderRadius: 'var(--radius-lg)', border: '1px solid var(--border-default)', overflow: 'hidden', width: '100%' }}>
        {loading && !data ? (
          <div className="empty-state" style={{ padding: '4rem 2rem', textAlign: 'center' }}>Loading partitions from backend...</div>
        ) : !data || data.content.length === 0 ? (
          <div className="empty-state" style={{ padding: '4rem 2rem', textAlign: 'center' }}>
            <Database size={32} style={{ color: 'var(--text-secondary)', marginBottom: '1rem' }} />
            <p style={{ fontWeight: 'var(--font-semibold)', marginBottom: '0.5rem' }}>No partitions found</p>
            <p style={{ color: 'var(--text-secondary)' }}>Try adjusting your search criteria.</p>
          </div>
        ) : (
          <div style={{ overflowX: 'auto', width: '100%' }}>
            <div className="figma-table">
              {/* Header Row */}
              <div className="figma-table-header">
                <div style={{ boxSizing: 'border-box', display: 'flex', flexDirection: 'row', alignItems: 'center', padding: 'var(--space-4)', gap: '4px', flex: 1, height: '54px', color: 'var(--button-primary-active)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-md)', cursor: 'pointer' }} onClick={() => handleSort('topicName')}>
                  <span>Topic Name</span>{renderSortIcon('topicName')}
                </div>
                <div style={{ boxSizing: 'border-box', display: 'flex', flexDirection: 'row', alignItems: 'center', padding: 'var(--space-4)', gap: '4px', width: '100px', flex: 'none', height: '54px', color: 'var(--button-primary-active)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-md)', cursor: 'pointer' }} onClick={() => handleSort('partitionId')}>
                  <span>Partition</span>{renderSortIcon('partitionId')}
                </div>
                <div style={{ boxSizing: 'border-box', display: 'flex', flexDirection: 'row', alignItems: 'center', padding: 'var(--space-4)', gap: '4px', width: '220px', flex: 'none', height: '54px', color: 'var(--button-primary-active)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-md)', cursor: 'pointer' }} onClick={() => handleSort('leaderBroker')}>
                  <span>Leader Broker</span>{renderSortIcon('leaderBroker')}
                </div>
                <div style={{ boxSizing: 'border-box', display: 'flex', flexDirection: 'row', alignItems: 'center', padding: 'var(--space-4)', gap: '4px', width: '100px', flex: 'none', height: '54px', color: 'var(--button-primary-active)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-md)' }}>
                  Replicas
                </div>
                <div style={{ boxSizing: 'border-box', display: 'flex', flexDirection: 'row', alignItems: 'center', padding: 'var(--space-4)', gap: '4px', width: '100px', flex: 'none', height: '54px', color: 'var(--button-primary-active)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-md)' }}>
                  ISR
                </div>
                <div style={{ boxSizing: 'border-box', display: 'flex', flexDirection: 'row', alignItems: 'center', padding: 'var(--space-4)', gap: '4px', width: '130px', flex: 'none', height: '54px', color: 'var(--button-primary-active)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-md)', cursor: 'pointer' }} onClick={() => handleSort('messageCount')}>
                  <span>Offsets ( / L)</span>{renderSortIcon('messageCount')}
                </div>
                <div style={{ boxSizing: 'border-box', display: 'flex', flexDirection: 'row', alignItems: 'center', padding: 'var(--space-4)', gap: '4px', width: '150px', flex: 'none', height: '54px', color: 'var(--button-primary-active)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-md)', cursor: 'pointer' }} onClick={() => handleSort('messageCount')}>
                  <span>Message Count</span>{renderSortIcon('messageCount')}
                </div>
                <div style={{ boxSizing: 'border-box', display: 'flex', flexDirection: 'row', alignItems: 'center', padding: 'var(--space-4)', gap: '4px', width: '130px', flex: 'none', height: '54px', color: 'var(--button-primary-active)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-medium)', fontSize: 'var(--text-md)', cursor: 'pointer' }} onClick={() => handleSort('health')}>
                  <span>Health Status</span>{renderSortIcon('health')}
                </div>
              </div>

              {/* Table Body */}
              <div className="figma-table-body">
                {data.content.map((p, idx) => (
                  <div key={`${p.topicName}-${p.partitionId}-${idx}`} className="figma-table-row table-row-hover">
                    <div style={{ boxSizing: 'border-box', display: 'flex', flexDirection: 'row', alignItems: 'center', padding: '14px 16px', gap: '4px', flex: 1, height: '52px', color: 'var(--text-heading)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-regular)', fontSize: 'var(--text-base)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {p.topicName}
                    </div>
                    <div style={{ boxSizing: 'border-box', display: 'flex', flexDirection: 'row', alignItems: 'center', padding: '14px 16px', gap: '4px', width: '100px', flex: 'none', height: '52px', color: 'var(--text-heading)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-regular)', fontSize: 'var(--text-base)' }}>
                      {p.partitionId}
                    </div>
                    <div style={{ boxSizing: 'border-box', display: 'flex', flexDirection: 'row', alignItems: 'center', padding: '14px 16px', gap: '4px', width: '220px', flex: 'none', height: '52px', color: 'var(--text-heading)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-regular)', fontSize: 'var(--text-base)' }}>
                      <div style={{ display: 'flex', flexDirection: 'row', alignItems: 'center', gap: '4px' }}>
                        <span style={{ fontWeight: 'var(--font-medium)' }}>{p.leaderBroker === -1 ? 'None' : p.leaderBroker}</span>
                        <span style={{ fontSize: 'var(--text-xs)', color: 'var(--text-tertiary)' }}>({p.leaderHostname})</span>
                      </div>
                    </div>
                    <div style={{ boxSizing: 'border-box', display: 'flex', flexDirection: 'row', alignItems: 'center', padding: '14px 16px', gap: '4px', width: '100px', flex: 'none', height: '52px', color: 'var(--text-heading)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-regular)', fontSize: 'var(--text-base)' }}>
                      [{p.replicaBrokers.join(', ')}]
                    </div>
                    <div style={{ boxSizing: 'border-box', display: 'flex', flexDirection: 'row', alignItems: 'center', padding: '14px 16px', gap: '4px', width: '100px', flex: 'none', height: '52px', color: 'var(--text-heading)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-regular)', fontSize: 'var(--text-base)' }}>
                      [{p.isrBrokers.join(', ')}]
                    </div>
                    <div style={{ boxSizing: 'border-box', display: 'flex', flexDirection: 'row', alignItems: 'center', padding: '14px 16px', gap: '4px', width: '130px', flex: 'none', height: '52px', color: 'var(--text-heading)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-regular)', fontSize: 'var(--text-base)' }}>
                      {p.earliestOffset} / {p.latestOffset}
                    </div>
                    <div style={{ boxSizing: 'border-box', display: 'flex', flexDirection: 'row', alignItems: 'center', padding: '14px 16px', gap: '4px', width: '150px', flex: 'none', height: '52px', color: 'var(--text-heading)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-regular)', fontSize: 'var(--text-base)' }}>
                      {p.messageCount.toLocaleString()}
                    </div>
                    <div style={{ boxSizing: 'border-box', display: 'flex', flexDirection: 'row', alignItems: 'center', padding: '14px 16px', gap: '4px', width: '130px', flex: 'none', height: '52px', color: 'var(--text-heading)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-regular)', fontSize: 'var(--text-base)' }}>
                      {p.health === 'CRITICAL' && (
                        <span style={{ boxSizing: 'border-box', display: 'inline-flex', justifyContent: 'center', alignItems: 'center', padding: '4px 8px', gap: '10px', width: '63px', height: '27px', backgroundColor: 'rgba(239, 77, 95, 0.25)', borderRadius: '100px', color: 'var(--color-danger)', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-regular)', fontSize: 'var(--text-base)', lineHeight: '19px', textAlign: 'center' }}>
                          Offline
                        </span>
                      )}
                      {p.health === 'WARNING' && (
                        <span style={{ boxSizing: 'border-box', display: 'inline-flex', justifyContent: 'center', alignItems: 'center', padding: '4px 8px', gap: '10px', width: '63px', height: '27px', backgroundColor: 'rgba(224, 142, 64, 0.25)', borderRadius: '100px', color: '#E08E40', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-regular)', fontSize: 'var(--text-base)', lineHeight: '19px', textAlign: 'center' }}>
                          Warning
                        </span>
                      )}
                      {(p.health === 'HEALTHY' || !p.health) && (
                        <span style={{ boxSizing: 'border-box', display: 'inline-flex', justifyContent: 'center', alignItems: 'center', padding: '4px 8px', gap: '10px', width: '63px', height: '27px', backgroundColor: 'rgba(42, 199, 146, 0.25)', borderRadius: '100px', color: '#1F845A', fontFamily: 'Satoshi, sans-serif', fontWeight: 'var(--font-regular)', fontSize: 'var(--text-base)', lineHeight: '19px', textAlign: 'center' }}>
                          Healthy
                        </span>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

        {/* Pagination Controls */}
        {data && data.totalPages > 1 && (
          <div className="pagination" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '1rem', borderTop: '1px solid var(--border-default)' }}>
            <div style={{ fontSize: '0.875rem', color: 'var(--text-tertiary)' }}>
              Showing page {data.page + 1} of {data.totalPages === 0 ? 1 : data.totalPages} (Total {data.totalElements} partitions)
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
              <select 
                value={size} 
                onChange={(e) => { setSize(Number(e.target.value)); setPage(0); }}
                style={{ padding: '0.4rem', borderRadius: '6px', border: '1px solid var(--border-default)', backgroundColor: "var(--text-light)", color: 'var(--text-heading)' }}
              >
                <option value={10}>10 per page</option>
                <option value={25}>25 per page</option>
                <option value={50}>50 per page</option>
                <option value={100}>100 per page</option>
                <option value={500}>500 per page</option>
              </select>
              
              <div style={{ display: 'flex', gap: '0.5rem' }}>
                <button 
                  className="pagination-btn" 
                  disabled={data.page === 0} 
                  onClick={() => setPage(p => Math.max(0, p - 1))}
                  style={{ padding: '0.4rem', display: 'flex', alignItems: 'center', background: "var(--bg-surface)", border: '1px solid var(--border-default)', borderRadius: '6px', cursor: data.page === 0 ? 'not-allowed' : 'pointer', opacity: data.page === 0 ? 0.5 : 1 }}
                >
                  <ChevronLeft size={16} />
                </button>
                <button 
                  className="pagination-btn" 
                  disabled={!data.hasNext} 
                  onClick={() => setPage(p => p + 1)}
                  style={{ padding: '0.4rem', display: 'flex', alignItems: 'center', background: "var(--bg-surface)", border: '1px solid var(--border-default)', borderRadius: '6px', cursor: !data.hasNext ? 'not-allowed' : 'pointer', opacity: !data.hasNext ? 0.5 : 1 }}
                >
                  <ChevronRight size={16} />
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
