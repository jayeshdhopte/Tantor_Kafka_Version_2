import { useId, useRef, useState } from 'react';
import { ChevronDown } from 'lucide-react';
import { AnchoredMenu } from './AnchoredMenu';

export interface ConfigPropertyOption { value: string; disabled?: boolean }

export function ConfigPropertySelect({ value, onChange, options, disabled, label }: {
  value: string; onChange: (value: string) => void; options: ConfigPropertyOption[]; disabled: boolean; label: string;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const [anchor, setAnchor] = useState<HTMLDivElement | null>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  const menuId = useId();
  const close = () => { setOpen(false); trigger.current?.focus(); };
  const filtered = options.filter(option => option.value.toLowerCase().includes(query.trim().toLowerCase()));
  return <div className="cc-picker" ref={setAnchor}>
    <button ref={trigger} type="button" className="cc-picker-trigger" disabled={disabled} aria-label={label}
      aria-haspopup="dialog" aria-expanded={open} aria-controls={open ? menuId : undefined}
      onClick={() => { setQuery(''); setOpen(current => !current); }}>
      <span>{options.some(option => option.value === value) ? value : 'Select a supported property'}</span><ChevronDown size={16} />
    </button>
    {open && !disabled && anchor && <AnchoredMenu anchor={anchor} className="cc-picker-menu" onClose={close} align="start" matchAnchorWidth>
      <div id={menuId} role="dialog" aria-label={label}>
        <input autoFocus type="search" aria-label={`Search ${label.toLowerCase()}`} placeholder="Search properties…"
          value={query} onChange={event => setQuery(event.target.value)} />
        <div role="listbox" aria-label={label} className="cc-picker-options">
          {filtered.map(option => <button type="button" role="option" key={option.value} className="cc-picker-option"
            aria-selected={option.value === value} aria-disabled={!!option.disabled} disabled={!!option.disabled}
            onClick={() => { onChange(option.value); close(); }}>
            <span>{option.value}</span>{option.disabled && <small>Already added</small>}
          </button>)}
          {!filtered.length && <p className="cc-picker-empty">No supported properties match your search.</p>}
        </div>
      </div>
    </AnchoredMenu>}
  </div>;
}
