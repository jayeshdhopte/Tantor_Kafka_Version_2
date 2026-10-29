import { useRef } from 'react';

const intervals = [0, 2, 5, 10, 15];
const label = (seconds: number) => seconds === 0 ? 'Off' : `${seconds} sec`;

export function ConsumerRefreshRate({ value, onChange }: { value: number; onChange: (seconds: number) => void }) {
  const select = useRef<HTMLSelectElement>(null);

  return <label className="consumer-refresh-rate" onClick={event => {
    const field = select.current;
    if (!field || field.contains(event.target as Node)) return;
    event.preventDefault();
    field.focus();
    // Open the original browser menu without changing its position or styling.
    if (typeof field.showPicker === 'function') field.showPicker();
    else field.click();
  }}>
    <span>Refresh rate:</span>
    <select ref={select} aria-label="Consumer groups refresh rate" value={value}
      onChange={event => onChange(Number(event.target.value))}>
      {intervals.map(seconds => <option key={seconds} value={seconds}>{label(seconds)}</option>)}
    </select>
  </label>;
}
