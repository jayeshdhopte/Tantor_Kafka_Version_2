import { useEffect, useMemo, useState, useCallback } from 'react';
import {
  History, Info, Package, Search,
  ChevronLeft, ChevronRight, RefreshCw
} from 'lucide-react';
import './AuditLogs.css';
import { AnchoredMenu } from '../components/AnchoredMenu';

const CustomRefreshIcon = ({ size = 24, color = "var(--text-tertiary)", className = "" }: { size?: number, color?: string, className?: string }) => (
  <svg 
    width={size} 
    height={size} 
    viewBox="0 0 24 24" 
    fill="none" 
    stroke={color} 
    strokeWidth="1.5" 
    strokeLinecap="round" 
    strokeLinejoin="round" 
    className={className}
    style={{ display: 'inline-block', verticalAlign: 'middle' }}
  >
    <path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8" />
    <path d="M3 3v5h5" />
    <path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16" />
    <path d="M16 16h5v5" />
  </svg>
);

const StackStarIcon = () => (
  <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M5 14.5H4.5a2 2 0 0 1-2-2V4.5a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2V5" />
    <rect x="9.5" y="9.5" width="12" height="12" rx="2" />
    <polygon points="15.5,11.7 16.4,14.2 19.1,14.3 17.0,16.0 17.7,18.6 15.5,17.1 13.3,18.6 14.0,16.0 11.9,14.3 14.6,14.2" fill="currentColor" stroke="none" />
  </svg>
);

const CheckCircleIcon = () => (
  <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="12" cy="12" r="9.5" />
    <path d="M9 12l2.5 2.5 4.5-4.5" />
  </svg>
);

const ReportIcon = () => (
  <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M8.5 3.5h7l5 5v7l-5 5h-7l-5-5v-7z" />
    <path d="M12 8v5M12 16h.01" />

  </svg>
);

interface AuditEvent {
  id: string;
  userName?: string;
  actor: string;
  category: string;
  action: string;
  resourceType: string;
  resourceId?: string;
  resource?: string;
  clusterId?: string;
  kafkaClusterId?: string;
  displayResourceId?: string;
  hostId?: string;
  hostIp?: string;
  hostName?: string;
  status: string;
  details?: unknown;
  createdAt?: string;
  createdTime?: string;
}

interface AuditResponse { events?: AuditEvent[] }

const title = (value: string) => value.replaceAll('_', ' ').toLowerCase().replace(/\b\w/g, letter => letter.toUpperCase());
const actorOf = (event: AuditEvent) => event.actor || event.userName || 'system';
const timeOf = (event: AuditEvent) => event.createdAt || event.createdTime || '';

const normalized = (value?: string) => (value || '').toUpperCase();

const isHostOnboardingEvent = (event: AuditEvent) => {
  const action = normalized(event.action);
  const category = normalized(event.category);
  return action.includes('ONBOARDING') || action.includes('REGISTER') || category === 'AGENT';
};

const isNoisyAuditEvent = (event: AuditEvent) => {
  const action = normalized(event.action);
  const resourceType = normalized(event.resourceType);
  return resourceType === 'JOB'
    || action === 'CLUSTER_CREATED'
    || action === 'CLUSTER_STATUS_CHANGED'
    || action === 'KAFKA_NODE_DEPLOYED'
    || action === 'KAFKA_NODE_DEPLOYMENT_FAILED'
    || action.endsWith('_REQUESTED');
};

const actionLabel = (event: AuditEvent) => {
  const action = normalized(event.action);
  if (action.includes('ONBOARDING') && normalized(event.status) === 'SUCCESS') return 'Host Registered';
  if (action.includes('ONBOARDING')) return 'Host Onboarding Requested';
  if (action === 'REGISTER' || action === 'HOST_REGISTERED') return 'Host Registered';
  return title(event.action || 'Captured');
};

const resourceTypeLabel = (event: AuditEvent) => {
  if (isHostOnboardingEvent(event)) return 'Agent';
  return title(event.resourceType || 'SYSTEM');
};

const resourceName = (event: AuditEvent) => {
  return event.resource || event.displayResourceId || event.resourceId || event.hostName || event.hostId || '';
};

const detailLabel = (event: AuditEvent) => {
  if (!event.details) return '-';
  if (typeof event.details === 'string') return event.details;
  try {
    return JSON.stringify(event.details);
  } catch {
    return String(event.details);
  }
};

interface CustomDropdownProps {
  value: string;
  options: { label: string; value: string }[];
  onChange: (val: string) => void;
}

function CustomDropdown({ value, options, onChange }: CustomDropdownProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [anchor, setAnchor] = useState<HTMLDivElement | null>(null);
  const selectedOption = options.find(opt => opt.value === value) || options[0];

  return (
    <div ref={setAnchor} className="custom-select-wrapper">
      <div
        className={`custom-select-trigger ${isOpen ? 'open' : ''}`}
        onClick={() => setIsOpen(prev => !prev)}
      >
        <span>{selectedOption ? selectedOption.label : value}</span>
        <span className="custom-select-arrow">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="var(--text-tertiary)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="6 9 12 15 18 9"></polyline>
          </svg>
        </span>
      </div>
      {isOpen && anchor && (
        <AnchoredMenu
          anchor={anchor}
          className="custom-select-options"
          onClose={() => setIsOpen(false)}
          align="start"
          matchAnchorWidth
        >
          {options.map(opt => (
            <div
              key={opt.value}
              className={`custom-select-option ${opt.value === value ? 'selected' : ''}`}
              onClick={() => {
                onChange(opt.value);
                setIsOpen(false);
              }}
            >
              {opt.label}
            </div>
          ))}
        </AnchoredMenu>
      )}
    </div>
  );
}

export function AuditLogs() {
  const [events, setEvents] = useState<AuditEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  // Draft filter state
  const [search, setSearch] = useState('');
  const [resourceId, setResourceId] = useState('');
  const [category, setCategory] = useState('ALL');
  const [status, setStatus] = useState('ALL');
  const [actor, setActor] = useState('ALL');
  const [from, setFrom] = useState('');
  const [to, setTo] = useState('');

  // Applied filter state
  const [appliedFilters, setAppliedFilters] = useState({
    search: '', resourceId: '', category: 'ALL', status: 'ALL', actor: 'ALL', from: '', to: ''
  });

  // Pagination state
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);

  const fetchLogs = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const results = await Promise.allSettled([
        fetch('/api/v1/ui/audit?size=500').then(async response => {
          if (!response.ok) throw new Error(`Management audit API returned ${response.status}`);
          return response.json() as Promise<AuditResponse>;
        }),
      ]);
      const combined = results.flatMap(result => result.status === 'fulfilled' ? result.value.events || [] : [])
        .filter(event => !isNoisyAuditEvent(event))
        .sort((left, right) => new Date(timeOf(right)).getTime() - new Date(timeOf(left)).getTime());
      setEvents(combined);
      const failures = results.filter(result => result.status === 'rejected').length;
      if (failures === results.length) throw new Error('Audit services are unavailable.');
      if (failures) setError('One audit source is temporarily unavailable; showing the remaining events.');
    } catch (caught) {
      setEvents([]);
      setError(caught instanceof Error ? caught.message : 'Unable to load audit trail.');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { void (async () => { await fetchLogs(); })(); }, [fetchLogs]);

  const categories = useMemo(() => Array.from(new Set(events.map(event => event.category))).sort(), [events]);
  const actors = useMemo(() => Array.from(new Set(events.map(actorOf).filter(Boolean))).sort(), [events]);

  const filtered = useMemo(() => events.filter(event => {
    if (appliedFilters.category !== 'ALL' && event.category !== appliedFilters.category) return false;
    if (appliedFilters.status !== 'ALL' && event.status !== appliedFilters.status) return false;
    if (appliedFilters.actor !== 'ALL' && actorOf(event) !== appliedFilters.actor) return false;
    if (appliedFilters.resourceId.trim()) {
      const needle = appliedFilters.resourceId.trim().toLowerCase();
      const resourceHaystack = [event.displayResourceId, event.kafkaClusterId, event.resourceId, event.resource, event.hostId, event.hostName, event.clusterId]
        .join(' ')
        .toLowerCase();
      if (!resourceHaystack.includes(needle)) return false;
    }
    const created = new Date(timeOf(event)).getTime();
    if (appliedFilters.from && created < new Date(appliedFilters.from).getTime()) return false;
    if (appliedFilters.to && created > new Date(appliedFilters.to).getTime()) return false;
    const haystack = [event.action, actionLabel(event), actorOf(event), event.resourceType, event.displayResourceId, event.kafkaClusterId, event.resourceId, event.resource, event.hostName, event.clusterId, detailLabel(event)].join(' ').toLowerCase();
    return !appliedFilters.search.trim() || haystack.includes(appliedFilters.search.trim().toLowerCase());
  }), [events, appliedFilters]);

  // Reset pagination when filters change
  useEffect(() => {
    Promise.resolve().then(() => setCurrentPage(1));
  }, [filtered.length]);

  const paginatedEvents = useMemo(() => {
    const start = (currentPage - 1) * pageSize;
    return filtered.slice(start, start + pageSize);
  }, [filtered, currentPage, pageSize]);

  const applyFilters = () => {
    setAppliedFilters({ search, resourceId, category, status, actor, from, to });
  };

  const resetFilters = () => {
    setSearch(''); setResourceId(''); setCategory('ALL'); setStatus('ALL'); setActor('ALL'); setFrom(''); setTo('');
    setAppliedFilters({ search: '', resourceId: '', category: 'ALL', status: 'ALL', actor: 'ALL', from: '', to: '' });
  };

  const summary = useMemo(() => ({
    total: events.length,
    success: events.filter(event => event.status === 'SUCCESS').length,
    failed: events.filter(event => event.status === 'FAILED').length,
  }), [events]);

  const totalResults = filtered.length;
  const startResult = totalResults === 0 ? 0 : (currentPage - 1) * pageSize + 1;
  const endResult = Math.min(currentPage * pageSize, totalResults);

  return <div className="audit-page animate-fade-in">
    <header className="audit-header">
      <div className="audit-header-title-row">
        <h1>Audit Trail</h1>
        <button className="btn-icon-only" onClick={fetchLogs} disabled={loading} title="Refresh">
          <RefreshCw size={14} className={loading ? 'spin' : ''} />
        </button>
      </div>
      <p className="audit-subtitle">Who performed each action, on which resource, and whether it succeeded.</p>
    </header>

    <div className="audit-summary-wrapper">
      <section className="audit-summary-grid">
        <article className="captured">
          <div className="card-header">
            <div className="icon-wrap"><StackStarIcon /></div>
            <span>Captured Events</span>
          </div>
          <strong>{summary.total}</strong>
        </article>
        <article className="success">
          <div className="card-header">
            <div className="icon-wrap"><CheckCircleIcon /></div>
            <span>Successful</span>
          </div>
          <strong>{summary.success}</strong>
        </article>
        <article className="failed">
          <div className="card-header">
            <div className="icon-wrap"><ReportIcon /></div>
            <span>Failed</span>
          </div>
          <strong>{summary.failed}</strong>
        </article>
      </section>
    </div>

    <section className="audit-readonly-note">
      <Info size={24} />
      <div>
        <strong>Read-only audit history</strong>
        <span>This screen has no edit or delete controls. Every application action creates a separate record.</span>
      </div>
    </section>

    <section className="audit-filters-container">
      <h3 className="section-heading">Audit Log Filters</h3>
      <div className="audit-filters-row-1">
        <label className="audit-search">
          <Search size={24} color="var(--text-tertiary)" />
          <input placeholder="Search configs..." value={search} onChange={e => setSearch(e.target.value)} />
        </label>
        <label className="audit-resource-id">

          <input placeholder="Resource ID" value={resourceId} onChange={e => setResourceId(e.target.value)} />
        </label>
        <div className="audit-filters-actions">
          <button type="button" className="btn-refresh" onClick={fetchLogs} disabled={loading} title="Refresh">
            <RefreshCw size={14} className={loading ? 'spin' : ''} />
          </button>
          <button className="btn-reset" onClick={resetFilters}>Reset</button>
          <button className="btn-apply" onClick={applyFilters}>Apply Filter</button>
        </div>
      </div>
      <div className="audit-filters-row-2">
        <div className="filter-group">
          <label>Event</label>
          <CustomDropdown
            value={category}
            options={[
              { label: 'All Event', value: 'ALL' },
              ...categories.map(item => ({ label: title(item), value: item }))
            ]}
            onChange={setCategory}
          />
        </div>
        <div className="filter-group">
          <label>Status</label>
          <CustomDropdown
            value={status}
            options={[
              { label: 'All', value: 'ALL' },
              { label: 'SUCCESS', value: 'SUCCESS' },
              { label: 'FAILED', value: 'FAILED' },
              { label: 'ATTEMPTED', value: 'ATTEMPTED' },
              { label: 'SCHEDULED', value: 'SCHEDULED' },
              { label: 'REQUESTED', value: 'REQUESTED' }
            ]}
            onChange={setStatus}
          />
        </div>
        <div className="filter-group">
          <label>Actors</label>
          <CustomDropdown
            value={actor}
            options={[
              { label: 'All', value: 'ALL' },
              ...actors.map(item => ({ label: item, value: item }))
            ]}
            onChange={setActor}
          />
        </div>
        <div className="filter-group">
          <label>From</label>
          <div>
            <input type="date" value={from} onChange={e => setFrom(e.target.value)} />
          </div>
        </div>
        <div className="filter-group">
          <label>To</label>
          <div>
            <input type="date" value={to} onChange={e => setTo(e.target.value)} />
          </div>
        </div>
      </div>
    </section>

    {error && <div className="audit-warning">{error}</div>}
    <section className="audit-ledger">
      <div className="audit-ledger-head">
        <div><History size={15} /><strong>Event Ledger</strong></div>
      </div>
      {loading ? <div className="audit-empty"><CustomRefreshIcon className="spin" /><p>Loading audit records...</p></div>
        : filtered.length === 0 ? <div className="audit-empty"><Info size={24} /><h3>No matching audit events</h3><p>Adjust the filters or perform an auditable operation.</p></div>
          : <div className="audit-table-wrap">
            <table className="audit-table">
              <thead><tr><th>Time</th><th>Event</th><th>Actor</th><th>Resource</th><th>Cluster ID</th><th>Details</th><th>Status</th></tr></thead>
              <tbody>{paginatedEvents.map(event => <AuditRow key={event.id} event={event} />)}</tbody>
            </table>
            <div className="audit-pagination">
              <span className="pagination-info">{startResult} to {endResult} of results</span>
              <div className="pagination-controls">
                <span>Show per page</span>
                <select value={pageSize} onChange={e => { setPageSize(Number(e.target.value)); setCurrentPage(1); }}>
                  <option value={10}>10</option>
                  <option value={20}>20</option>
                  <option value={50}>50</option>
                </select>
                <div className="pagination-nav">
                  <button className="page-btn" disabled={currentPage === 1} onClick={() => setCurrentPage(p => p - 1)}><ChevronLeft size={16} /></button>
                  <button className="page-btn" disabled={currentPage * pageSize >= totalResults} onClick={() => setCurrentPage(p => p + 1)}><ChevronRight size={16} /></button>
                </div>
              </div>
            </div>
          </div>}
    </section>
  </div>;
}

function AuditRow({ event }: { event: AuditEvent }) {
  const created = timeOf(event);
  const createdDate = created ? new Date(created) : null;
  const details = detailLabel(event);

  const formattedTime = createdDate && !Number.isNaN(createdDate.getTime())
    ? `${String(createdDate.getDate()).padStart(2, '0')}/${String(createdDate.getMonth() + 1).padStart(2, '0')}/${createdDate.getFullYear()} | ${createdDate.toLocaleTimeString('en-GB', { hour12: false })}`
    : '-';

  return <tr>
    <td><div className="audit-time"><span>{formattedTime}</span></div></td>
    <td><div className="audit-event"><span className={`category-dot ${event.category.toLowerCase()}`}>{event.category === 'PACKAGE' ? <Package size={12} /> : null}</span><div><span>{actionLabel(event)}</span></div></div></td>
    <td><div className="audit-actor"><span>{actorOf(event)}</span></div></td>
    <td><div className="audit-resource"><strong>{resourceTypeLabel(event)}</strong>{resourceName(event) && <small>{resourceName(event)}</small>}</div></td>
    <td><div className="audit-resource"><small title={event.clusterId ? `Internal UUID: ${event.clusterId}` : undefined}>{event.kafkaClusterId || event.clusterId || '-'}</small></div></td>
    <td><div className="audit-details-inline"><span title={details}>{details || '-'}</span></div></td>
    <td><span className={`audit-status ${event.status.toLowerCase()}`}>{event.status.charAt(0).toUpperCase() + event.status.slice(1).toLowerCase()}</span></td>
  </tr>
}
