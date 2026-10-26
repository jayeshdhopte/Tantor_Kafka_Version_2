import { useEffect, useRef, useState, type ButtonHTMLAttributes } from 'react';
import './RefreshButton.css';

// Keep fast refreshes visible without delaying the existing data request.
export function RefreshButton({ onClick, className = '', disabled, children, ...props }: ButtonHTMLAttributes<HTMLButtonElement>) {
  const [refreshing, setRefreshing] = useState(false);
  const mounted = useRef(false);
  const inFlight = useRef(false);
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const finishDelay = useRef<(() => void) | undefined>(undefined);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      clearTimeout(timer.current);
      finishDelay.current?.();
    };
  }, []);

  return <button {...props} type={props.type ?? 'button'}
    className={`${className} refresh-feedback-button`.trim()}
    disabled={disabled || refreshing}
    aria-busy={props['aria-busy'] || refreshing}
    data-refreshing={refreshing}
    onClick={async event => {
      if (disabled || inFlight.current) return;
      inFlight.current = true;
      setRefreshing(true);
      const minimumSpin = new Promise<void>(resolve => {
        finishDelay.current = resolve;
        timer.current = setTimeout(resolve, 600);
      });
      try {
        await onClick?.(event);
      } finally {
        await minimumSpin;
        inFlight.current = false;
        finishDelay.current = undefined;
        if (mounted.current) setRefreshing(false);
      }
    }}>{children}</button>;
}
