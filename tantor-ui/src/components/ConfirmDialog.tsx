import { useEffect, useRef, useState, useCallback } from 'react';
import { createPortal } from 'react-dom';
import { type ConfirmRequest, CONFIRM_EVENT } from './confirmUtils';
import { X } from 'lucide-react';
import orangeBanner from '../assets/orange.png';
import './ConfirmDialog.css';

export function GlobalConfirmDialog() {
  const [request, setRequest] = useState<ConfirmRequest | null>(null);
  const confirmButtonRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    const handleRequest = (event: Event) => {
      setRequest((event as CustomEvent<ConfirmRequest>).detail);
    };
    window.addEventListener(CONFIRM_EVENT, handleRequest);
    return () => window.removeEventListener(CONFIRM_EVENT, handleRequest);
  }, []);

  const finish = useCallback((confirmed: boolean) => {
    if (!request) return;
    request.resolve?.(confirmed);
    setRequest(null);
  }, [request]);

  useEffect(() => {
    if (!request) return;
    confirmButtonRef.current?.focus();
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') finish(false);
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [request, finish]);

  if (!request) return null;

  return createPortal(
    <div className="app-confirm-backdrop" onMouseDown={() => finish(false)}>
      <section
        className="app-confirm-dialog"
        role="alertdialog"
        aria-modal="true"
        aria-labelledby="app-confirm-title"
        aria-describedby="app-confirm-message"
        onMouseDown={event => event.stopPropagation()}
      >
        <div className="app-confirm-banner">
          <img src={orangeBanner} alt="" aria-hidden="true" />
          <button type="button" onClick={() => finish(false)} aria-label="Close confirmation">
            <X size={20} />
          </button>
        </div>
        <div className="app-confirm-content">
          <h2 id="app-confirm-title">{request.title || 'Please confirm'}</h2>
          <p id="app-confirm-message">{request.message}</p>
          <div className="app-confirm-actions">
            {request.showCancel !== false && (
              <button type="button" className="app-confirm-cancel" onClick={() => finish(false)}>Cancel</button>
            )}
            <button ref={confirmButtonRef} type="button" className="app-confirm-primary" onClick={() => finish(true)}>
              {request.confirmLabel || 'OK'}
            </button>
          </div>
        </div>
      </section>
    </div>,
    document.body
  );
}
