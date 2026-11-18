import { useLayoutEffect, useId, useRef, useState, type RefObject } from 'react';
import { createPortal } from 'react-dom';
import { CalendarDays, ChevronLeft, ChevronRight, Clock3 } from 'lucide-react';
import './LogDateTimePicker.css';

type Props = { label: string; value: string; min?: string; max?: string; onApply: (value: string) => void };
const pad = (n: number) => String(n).padStart(2, '0');
const todayIst = () => new Date(Date.now() + 19800000).toISOString().slice(0, 16);
const display = (value: string) => {
  const [date, time] = value.split('T');
  return `${date.split('-').reverse().join('-')} ${time}`;
};

export function LogDateTimePicker(props: Props) {
  const [open, setOpen] = useState(false);
  const trigger = useRef<HTMLButtonElement>(null);
  const id = useId();
  return <div className="log-date-field">
    <span id={id}>{props.label}</span>
    <button ref={trigger} type="button" className="log-date-trigger" aria-label={`${props.label}: ${display(props.value)}`}
      aria-haspopup="dialog" aria-expanded={open} onClick={() => setOpen(v => !v)}>
      <span>{display(props.value)}</span><CalendarDays size={16} aria-hidden="true" />
    </button>
    {open && createPortal(<DateTimePopup {...props} trigger={trigger} close={() => {
      setOpen(false);
      trigger.current?.focus();
    }} />, document.body)}
  </div>;
}

function DateTimePopup({ label, value, min, max, onApply, trigger, close }: Props & { trigger: RefObject<HTMLButtonElement | null>; close: () => void }) {
  const [draft, setDraft] = useState(value);
  const [month, setMonth] = useState(() => new Date(`${value.slice(0, 7)}-01T00:00:00Z`));
  const panel = useRef<HTMLDivElement>(null);
  const titleId = useId();
  const hintId = useId();
  const [position, setPosition] = useState({ left: 12, top: 12 });
  const date = draft.slice(0, 10);
  const hour = draft.slice(11, 13);
  const minute = draft.slice(14, 16);
  const valid = (!min || draft >= min) && (!max || draft <= max);
  const year = month.getUTCFullYear();
  const monthIndex = month.getUTCMonth();
  const start = new Date(Date.UTC(year, monthIndex, 1)).getUTCDay();
  const days = Array.from({ length: 42 }, (_, i) => new Date(Date.UTC(year, monthIndex, i - start + 1)));

  useLayoutEffect(() => {
    const place = () => {
      const rect = trigger.current?.getBoundingClientRect();
      if (!rect) return;
      const width = Math.min(440, window.innerWidth - 24);
      const height = Math.min(panel.current?.offsetHeight || 420, window.innerHeight - 24);
      const top = rect.bottom + 8 + height <= window.innerHeight - 12 ? rect.bottom + 8 : rect.top - height - 8;
      setPosition({ left: Math.max(12, Math.min(rect.left, window.innerWidth - width - 12)), top: Math.max(12, Math.min(top, window.innerHeight - height - 12)) });
    };
    place();
    panel.current?.querySelector<HTMLButtonElement>('[data-day-selected="true"]')?.focus({ preventScroll: true });
    panel.current?.querySelectorAll<HTMLElement>('[data-time-selected="true"]').forEach(item => {
      if (item.parentElement) item.parentElement.scrollTop = item.offsetTop - item.parentElement.offsetTop - 72;
    });
    const outside = (event: PointerEvent) => {
      if (!panel.current?.contains(event.target as Node) && !trigger.current?.contains(event.target as Node)) close();
    };
    document.addEventListener('pointerdown', outside);
    window.addEventListener('resize', place);
    window.addEventListener('scroll', place, true);
    return () => {
      document.removeEventListener('pointerdown', outside);
      window.removeEventListener('resize', place);
      window.removeEventListener('scroll', place, true);
    };
  // Opening creates a fresh draft; event handlers read the current DOM position.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const changeMonth = (delta: number) => setMonth(new Date(Date.UTC(year, monthIndex + delta, 1)));
  return <div ref={panel} className="log-date-popup" role="dialog" aria-modal="true" aria-labelledby={titleId}
    aria-describedby={hintId} style={position} onKeyDown={event => {
      if (event.key === 'Escape') { event.preventDefault(); close(); }
      if (event.key === 'Tab') {
        const controls = Array.from(panel.current!.querySelectorAll<HTMLButtonElement>('button:not(:disabled)'));
        const first = controls[0], last = controls[controls.length - 1];
        if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
        if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
      }
    }}>
    <div className="log-date-popup-heading"><CalendarDays size={17} aria-hidden="true" /><strong id={titleId}>{label}</strong><span>IST · 24-hour</span></div>
    <div className="log-date-popup-body">
      <div className="log-date-calendar">
        <div className="log-date-month"><strong>{month.toLocaleDateString('en-GB', { month: 'long', year: 'numeric', timeZone: 'UTC' })}</strong>
          <button type="button" aria-label="Previous month" onClick={() => changeMonth(-1)}><ChevronLeft size={16} /></button>
          <button type="button" aria-label="Next month" onClick={() => changeMonth(1)}><ChevronRight size={16} /></button>
        </div>
        <div className="log-date-days">{['Su', 'Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa'].map(day => <span key={day}>{day}</span>)}
          {days.map(day => {
            const key = day.toISOString().slice(0, 10);
            return <button key={key} type="button" data-day-selected={key === date} aria-pressed={key === date}
              data-outside={day.getUTCMonth() !== monthIndex} data-today={key === todayIst().slice(0, 10)}
              disabled={!!((min && key < min.slice(0, 10)) || (max && key > max.slice(0, 10)))}
              aria-label={day.toLocaleDateString('en-GB', { dateStyle: 'full', timeZone: 'UTC' })}
              onClick={() => setDraft(`${key}T${hour}:${minute}`)}>{day.getUTCDate()}</button>;
          })}
        </div>
        <button type="button" className="log-date-today" onClick={() => {
          const now = todayIst(); setDraft(now); setMonth(new Date(`${now.slice(0, 7)}-01T00:00:00Z`));
        }}>Today</button>
      </div>
      <div className="log-date-time"><div className="log-date-time-heading"><Clock3 size={14} aria-hidden="true" />Time</div>
        <div className="log-date-time-labels"><span>Hour</span><span>Min</span></div>
        <div className="log-date-wheels">
          <div className="log-date-wheel" role="group" aria-label="Hour">{Array.from({ length: 24 }, (_, i) => pad(i)).map(h => <button type="button" key={h}
            aria-label={`${h} hours`} aria-pressed={h === hour} data-time-selected={h === hour} onClick={() => setDraft(`${date}T${h}:${minute}`)}>{h}</button>)}</div>
          <div className="log-date-wheel" role="group" aria-label="Minute">{Array.from({ length: 60 }, (_, i) => pad(i)).map(m => <button type="button" key={m}
            aria-label={`${m} minutes`} aria-pressed={m === minute} data-time-selected={m === minute} onClick={() => setDraft(`${date}T${hour}:${m}`)}>{m}</button>)}</div>
        </div>
      </div>
    </div>
    <div className="log-date-preview" id={hintId}>{valid ? display(draft) + ' IST' : 'Start time must not be after end time.'}</div>
    <div className="log-date-footer"><button type="button" onClick={close}>Cancel</button><button type="button" className="log-date-confirm" disabled={!valid} onClick={() => { onApply(draft); close(); }}>Apply</button></div>
  </div>;
}
