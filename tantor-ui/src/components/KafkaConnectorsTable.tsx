import { useState } from 'react';
import { ArrowDown, ArrowUp, Download, Pause, Play, RotateCw, Search, Trash2 } from 'lucide-react';
import { SchemaSubjectFilter } from './SchemaSubjectFilter';
import { connectorCsv, connectorStatusClass, connectorType, downloadConnectFile, type ConnectorRow } from '../utils/kafkaConnect';

type FilterKey = 'type' | 'state' | 'class' | 'topics' | 'consumerGroup';
export function KafkaConnectorsTable({ rows, instance, loading, saving, canManage, onOpen, onAction }: {
  rows: ConnectorRow[]; instance: string; loading: boolean; saving: boolean; canManage: boolean;
  onOpen: (name: string) => void; onAction: (name: string, action: 'pause' | 'resume' | 'restart' | 'delete') => void;
}) {
  const [search, setSearch] = useState('');
  const [filters, setFilters] = useState<Record<FilterKey, string[]>>({ type: [], state: [], class: [], topics: [], consumerGroup: [] });
  const [sort, setSort] = useState({ key: 'name', ascending: true });
  const values = (row: ConnectorRow, key: string): string[] => key === 'type' ? [connectorType(row)]
    : key === 'topics' ? (row.topicsAvailable ? row.topics?.length ? row.topics : ['No tracked topics'] : ['Unavailable'])
    : key === 'connect' ? [instance] : [String(row[key as keyof ConnectorRow] || '-')];
  const query = search.trim().toLowerCase();
  const searched = rows.filter(row => [row.name, instance, connectorType(row), row.state, row.class, row.consumerGroup, ...(row.topics || [])]
    .some(value => String(value || '').toLowerCase().includes(query)));
  const matches = (row: ConnectorRow, except?: string) => Object.entries(filters).every(([key, selected]) =>
    key === except || !selected.length || values(row, key).some(value => selected.includes(value)));
  const visible = searched.filter(row => matches(row)).sort((a, b) => {
    const left = values(a, sort.key).join(', '), right = values(b, sort.key).join(', ');
    return left.localeCompare(right, undefined, { numeric: true }) * (sort.ascending ? 1 : -1);
  });
  const active = Boolean(search || Object.values(filters).some(value => value.length));
  const headers = [['name', 'Name'], ['connect', 'Connect'], ['type', 'Type'], ['class', 'Plugin'], ['topics', 'Topics'], ['state', 'Status'], ['consumerGroup', 'Consumer group']];
  return <div className="ds-kc-inventory">
    <div className="ds-subject-filters ds-kc-connector-filters" role="search" aria-label="Search and filter connectors">
      <label className="ds-subject-search"><Search size={18} /><input type="search" aria-label="Search connectors"
        placeholder="Search by name, status, type or plugin" value={search} onChange={event => setSearch(event.target.value)} /></label>
      {([['type', 'Type'], ['state', 'Status'], ['class', 'Plugin'], ['topics', 'Topics'], ['consumerGroup', 'Consumer group']] as [FilterKey, string][]).map(([key, label]) => {
        const counts: Record<string, number> = {};
        for (const row of searched.filter(row => matches(row, key))) for (const value of new Set(values(row, key))) counts[value] = (counts[value] || 0) + 1;
        const options = [...new Set([...rows.flatMap(row => values(row, key)), ...filters[key]])].sort();
        return <SchemaSubjectFilter key={key} label={label} selected={filters[key]} counts={counts}
          options={options.map(value => ({ value, label: value }))} onChange={selected => setFilters(current => ({ ...current, [key]: selected }))} />;
      })}
      {active && <button className="ds-subject-filter-reset" onClick={() => { setSearch(''); setFilters({ type: [], state: [], class: [], topics: [], consumerGroup: [] }); }}>Clear filters</button>}
      <span className="ds-subject-results" role="status">{visible.length} of {rows.length} connectors · {rows.reduce((sum, row) => sum + row.tasks, 0)} tasks</span>
      <button className="ds-button outlined" onClick={() => downloadConnectFile('tantor-connectors.csv', connectorCsv(visible, instance), 'text/csv;charset=utf-8')} disabled={!visible.length}><Download size={16} /> Export CSV</button>
    </div>
    <div className="ds-panel ds-kc-panel ds-kc-list-scroll"><table className="ds-table ds-kc-table ds-kc-expanded-table">
      <thead><tr>{headers.map(([key, label]) => <th key={key} aria-sort={sort.key === key ? sort.ascending ? 'ascending' : 'descending' : 'none'}>
        <button className="ds-kc-sort" aria-label={`Sort by ${label.toLowerCase()}`} onClick={() => setSort(current => ({ key, ascending: current.key === key ? !current.ascending : true }))}>
          {label}{sort.key === key && (sort.ascending ? <ArrowUp size={13} /> : <ArrowDown size={13} />)}</button></th>)}
        <th>Tasks running / total</th>{canManage && <th>Actions</th>}</tr></thead>
      <tbody>{loading && !rows.length ? <tr><td colSpan={canManage ? 9 : 8} className="ds-empty">Loading connectors...</td></tr>
        : visible.length ? visible.map(row => <tr key={row.name}>
          <td><button className="ds-connector-link" onClick={() => onOpen(row.name)}>{row.name}</button></td>
          <td>{instance}</td><td>{connectorType(row)}</td><td className="ds-kc-plugin-cell">{row.class || '-'}</td>
          <td>{values(row, 'topics').join(', ')}</td><td><span className={connectorStatusClass(row.state)}>{row.state}</span></td>
          <td title={row.consumerGroupSource === 'default' ? 'Default sink consumer group name; this does not indicate whether consumers are currently active.' : undefined}>{row.consumerGroup || '-'}</td>
          <td>{row.runningTasks} / {row.tasks}</td>{canManage && <td><div className="ds-inline-actions">
            {([['pause', Pause], ['resume', Play], ['restart', RotateCw], ['delete', Trash2]] as const).map(([action, Icon]) =>
              <button key={action} className={`ds-button${action === 'delete' ? ' danger' : ''}`} title={action} aria-label={`${action} ${row.name}`} disabled={saving} onClick={() => onAction(row.name, action)}><Icon size={15} /></button>)}
          </div></td>}</tr>) : <tr><td colSpan={canManage ? 9 : 8} className="ds-empty">{active ? 'No connectors match your search and filters.' : 'No connectors found.'}</td></tr>}
      </tbody></table></div>
  </div>;
}
