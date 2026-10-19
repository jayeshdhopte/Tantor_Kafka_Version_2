import { useEffect, useId, useRef, useState } from 'react';
import { ChevronDown, Search } from 'lucide-react';
import { AnchoredMenu } from './AnchoredMenu';

export function SchemaSubjectFilter({ label, options, selected, counts, onChange }: {
  label: string; options: { value: string; label: string }[]; selected: string[];
  counts: Record<string, number>; onChange: (values: string[]) => void;
}) {
  const [anchor, setAnchor] = useState<HTMLButtonElement | null>(null);
  const [query, setQuery] = useState('');
  const menuId = useId();
  const searchInput = useRef<HTMLInputElement>(null);
  useEffect(() => { if (anchor) searchInput.current?.focus(); }, [anchor]);
  const close = () => { setAnchor(null); setQuery(''); };
  const matches = options.filter(option => option.label.toLowerCase().includes(query.trim().toLowerCase()));
  return <>
    <button type="button" className={`ds-subject-filter-button${selected.length ? ' selected' : ''}`}
      aria-expanded={Boolean(anchor)} aria-controls={anchor ? menuId : undefined} aria-haspopup="dialog"
      onClick={event => anchor ? close() : setAnchor(event.currentTarget)}>
      {label}{selected.length > 0 && <span className="ds-subject-filter-badge">{selected.length}</span>}<ChevronDown size={14} />
    </button>
    {anchor && <AnchoredMenu anchor={anchor} align="start" className="ds-subject-filter-menu" minWidth={280} onClose={close}>
      <div id={menuId} role="dialog" aria-label={`${label} filter`} onKeyDown={event => {
        if (event.key === 'Escape') { close(); anchor.focus(); }
      }}>
        <label className="ds-subject-filter-search"><Search size={17} /><input ref={searchInput} aria-label={`Search ${label.toLowerCase()} options`}
          placeholder={label} value={query} onChange={event => setQuery(event.target.value)} /></label>
        <div className="ds-subject-filter-options">
          {matches.map(option => <label key={option.value} className={`ds-subject-filter-option${selected.includes(option.value) ? ' checked' : ''}`}>
            <input type="checkbox" checked={selected.includes(option.value)} onChange={event => onChange(event.target.checked
              ? [...selected, option.value] : selected.filter(value => value !== option.value))} />
            <span>{option.label}</span><small>{counts[option.value] || 0}</small>
          </label>)}
          {!matches.length && <p className="ds-subject-filter-empty">No matching options.</p>}
        </div>
        {selected.length > 0 && <div className="ds-subject-filter-footer"><button type="button" className="ds-subject-filter-reset" onClick={() => onChange([])}>Clear {label.toLowerCase()}</button></div>}
      </div>
    </AnchoredMenu>}
  </>;
}
