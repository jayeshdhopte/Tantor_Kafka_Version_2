import { useEffect, useId, useRef } from 'react';
import { createPortal } from 'react-dom';
import { X } from 'lucide-react';
import { ServiceDirectories } from './ServiceDirectories';

export function ServiceDirectoriesModal({ clusterId, serviceType, connectionName, returnFocus, onClose }: {
  clusterId?: string;
  serviceType: 'SCHEMA_REGISTRY' | 'KAFKA_CONNECT';
  connectionName: string;
  returnFocus: HTMLElement;
  onClose: () => void;
}) {
  const dialog = useRef<HTMLDivElement>(null);
  const titleId = useId();
  const serviceName = serviceType === 'SCHEMA_REGISTRY' ? 'Schema Registry' : 'Kafka Connect';
  useEffect(() => {
    dialog.current?.focus();
    const overflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    return () => {
      document.body.style.overflow = overflow;
      returnFocus.focus();
    };
  }, [returnFocus]);

  return createPortal(<div className="ds-modal-backdrop ds-sr-directories-backdrop" onMouseDown={onClose}>
    <div ref={dialog} className="ds-modal ds-sr-directories-modal" role="dialog" aria-modal="true"
      aria-labelledby={titleId} tabIndex={-1} onMouseDown={event => event.stopPropagation()}
      onKeyDown={event => {
        if (event.key === 'Escape') { event.stopPropagation(); onClose(); }
        if (event.key === 'Tab') {
          const buttons = dialog.current?.querySelectorAll<HTMLButtonElement>('button:not(:disabled)');
          if (!buttons?.length) { event.preventDefault(); return; }
          const first = buttons[0], last = buttons[buttons.length - 1];
          if (event.shiftKey && (document.activeElement === first || document.activeElement === dialog.current)) {
            event.preventDefault(); last.focus();
          } else if (!event.shiftKey && (document.activeElement === last || document.activeElement === dialog.current)) {
            event.preventDefault(); first.focus();
          }
        }
      }}>
      <div className="ds-modal-header">
        <h3 id={titleId}>{serviceName} Directories</h3>
        <button type="button" className="ds-icon-button" aria-label="Close directory preview" onClick={onClose}><X size={20} /></button>
      </div>
      <p className="ds-sr-directories-connection">{connectionName}</p>
      <ServiceDirectories clusterId={clusterId} serviceType={serviceType} preview />
    </div>
  </div>, document.body);
}
