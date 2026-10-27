import './AutoRefreshControl.css';

export function AutoRefreshControl({ value, onChange, label }: { value: number; onChange: (value: number) => void; label: string }) {
  return <label className="tab-auto-refresh"><span>Refresh rate:</span>
    <select aria-label={`${label} refresh rate`} value={value} onChange={event => onChange(Number(event.target.value))}>
      <option value={0}>Off</option>{[5, 10, 30, 60].map(seconds => <option key={seconds} value={seconds}>{seconds}s</option>)}
    </select>
  </label>;
}
