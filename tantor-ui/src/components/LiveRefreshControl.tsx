import { useId, useState, type KeyboardEvent } from 'react';
import { Check } from 'lucide-react';
import { AnchoredMenu } from './AnchoredMenu';
import { liveRefreshIntervals } from '../utils/liveRefresh';
import './LiveRefreshControl.css';

export function LiveRefreshControl({ enabled, seconds, onChange, label }: {
  enabled: boolean; seconds: number; onChange: (enabled: boolean, seconds: number) => void; label: string;
}) {
  const [anchor, setAnchor] = useState<HTMLButtonElement | null>(null);
  const [open, setOpen] = useState(false);
  const menuId = useId();
  const choose = (active: boolean, interval = seconds) => { onChange(active, interval); setOpen(false); anchor?.focus(); };
  const navigateMenu = (event: KeyboardEvent<HTMLDivElement>) => {
    const buttons = Array.from(event.currentTarget.querySelectorAll<HTMLButtonElement>('button'));
    const index = buttons.indexOf(document.activeElement as HTMLButtonElement);
    const next = event.key === 'Home' ? 0 : event.key === 'End' ? buttons.length - 1
      : event.key === 'ArrowDown' ? (index + 1) % buttons.length
      : event.key === 'ArrowUp' ? (index < 0 ? buttons.length - 1 : (index + buttons.length - 1) % buttons.length) : -1;
    if (next >= 0) { event.preventDefault(); buttons[next]?.focus(); }
    if (event.key === 'Escape') { setOpen(false); anchor?.focus(); }
  };
  return <div className="tab-live-refresh">
    <button ref={setAnchor} type="button" className={`tab-live-refresh-trigger${enabled ? ' active' : ''}`}
      aria-label={`${label} auto refresh: ${enabled ? `every ${seconds} seconds` : 'Off'}`}
      aria-haspopup="menu" aria-expanded={open} aria-controls={open ? menuId : undefined}
      onClick={() => setOpen(value => !value)}>
      <span className={`tab-live-refresh-dot${enabled ? ' active' : ''}`} aria-hidden="true" />
      <span className="tab-live-refresh-label">Live</span>
      <span className={`tab-live-refresh-check${enabled ? ' active' : ''}`} aria-hidden="true">{enabled && <Check size={12} strokeWidth={3} />}</span>
      <span className="tab-live-refresh-interval">{seconds}s</span>
    </button>
    {open && anchor && <AnchoredMenu anchor={anchor} className="tab-live-refresh-menu" minWidth={180} align="start" onClose={() => setOpen(false)}>
      <div id={menuId} role="menu" aria-label={`${label} refresh rate`} onKeyDown={navigateMenu}>
        {liveRefreshIntervals.map(interval => <button type="button" key={interval} role="menuitemradio"
          aria-checked={enabled && seconds === interval} autoFocus={interval === 5}
          className={`tab-live-refresh-option${enabled && seconds === interval ? ' selected' : ''}`} onClick={() => choose(true, interval)}>
          <span className="tab-live-refresh-dot active" aria-hidden="true" /> Live | {interval} Sec
        </button>)}
        <div className="tab-live-refresh-divider" role="separator" />
        <button type="button" role="menuitem" className="tab-live-refresh-option" onClick={() => choose(!enabled)}>
          <span className="tab-live-refresh-dot" aria-hidden="true" />{enabled ? 'Pause Live Feed' : 'Resume Live Feed'}
        </button>
      </div>
    </AnchoredMenu>}
  </div>;
}
