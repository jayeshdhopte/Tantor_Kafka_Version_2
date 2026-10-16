import { useEffect, useRef } from 'react';
import { createPortal } from 'react-dom';
import { X } from 'lucide-react';
import './TopicActionConfirmationModal.css';

export function SchemaRegistrySettingModal({ title, description, detail, saving, error, onClose, onConfirm }: {
  title: string; description: string; detail: string; saving: boolean; error: string | null;
  onClose: () => void; onConfirm: () => void;
}) {
  const dialog = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null;
    dialog.current?.focus();
    return () => previous?.focus();
  }, []);
  return createPortal(<div className="topic-action-modal-backdrop" onMouseDown={() => !saving && onClose()}>
    <div ref={dialog} tabIndex={-1} className="topic-action-modal" role="alertdialog" aria-modal="true"
      aria-labelledby="sr-setting-title" aria-describedby="sr-setting-description"
      onMouseDown={event => event.stopPropagation()} onKeyDown={event => {
        if (event.key === 'Escape' && !saving) onClose();
        if (event.key === 'Tab') {
          const buttons = dialog.current?.querySelectorAll<HTMLButtonElement>('button:not(:disabled)');
          if (!buttons?.length) { event.preventDefault(); return; }
          const first = buttons[0], last = buttons[buttons.length - 1];
          if (event.shiftKey && (document.activeElement === first || document.activeElement === dialog.current)) {
            event.preventDefault(); last.focus();
          } else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
        }
      }}>
      <div className="topic-action-banner" aria-hidden="true" />
      <button className="topic-action-close" disabled={saving} onClick={onClose} aria-label="Close modal"><X size={22} /></button>
      <div className="topic-action-content">
        <div className="topic-action-title"><span className="topic-action-icon" aria-hidden="true" /><h3 id="sr-setting-title">{title}</h3></div>
        <p id="sr-setting-description">{description}</p><div className="topic-action-names">{detail}</div>
        {error && <p className="ds-alert" role="alert">{error}</p>}
        <footer><button className="topic-action-cancel" disabled={saving} onClick={onClose}>Cancel</button>
          <button className="topic-action-confirm" disabled={saving} onClick={onConfirm}>{saving ? 'Saving...' : 'Confirm change'}</button></footer>
      </div>
    </div>
  </div>, document.body);
}
