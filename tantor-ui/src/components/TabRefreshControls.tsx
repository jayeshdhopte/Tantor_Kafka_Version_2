import type { ReactNode } from 'react';
import './TabRefreshControls.css';

export function TabRefreshControls({ children, className = '' }: { children: ReactNode; className?: string }) {
  return <div className={`tab-refresh-controls ${className}`.trim()}>{children}</div>;
}
