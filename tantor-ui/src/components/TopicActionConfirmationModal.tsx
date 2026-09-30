import { X } from 'lucide-react';
import { createPortal } from 'react-dom';
import './TopicActionConfirmationModal.css';

import { type TopicActionKind, topicActionCopy } from './topicActionTypes';

interface TopicActionConfirmationModalProps {
  action: TopicActionKind;
  topicNames: string[];
  acting: boolean;
  onClose: () => void;
  onConfirm: () => void;
}

export function TopicActionConfirmationModal({
  action,
  topicNames,
  acting,
  onClose,
  onConfirm
}: TopicActionConfirmationModalProps) {
  const title = action === 'remove' && topicNames.length > 1 ? 'Remove these topics?' : topicActionCopy[action].title;
  const description = action === 'remove' && topicNames.length > 1
    ? 'The topics and all associated data will be permanently deleted.'
    : topicActionCopy[action].description;

  // Mount at the document root so the backdrop covers the full application,
  // matching the Dashboard's New Cluster modal.
  return createPortal(
    <div className="topic-action-modal-backdrop" role="presentation" onMouseDown={() => !acting && onClose()}>
      <div className="topic-action-modal" role="alertdialog" aria-modal="true" aria-labelledby="topic-action-confirmation-title" onMouseDown={event => event.stopPropagation()}>
        <div className="topic-action-banner" aria-hidden="true" />
        <button className="topic-action-close" onClick={onClose} disabled={acting} aria-label="Close modal"><X size={22} /></button>
        <div className="topic-action-content">
          <div className="topic-action-title">
            <span className="topic-action-icon" aria-hidden="true" />
            <h3 id="topic-action-confirmation-title">{title}</h3>
          </div>
          <p>{description}</p>
          <div className="topic-action-names">{topicNames.join(', ')}</div>
          <footer>
            <button className="topic-action-cancel" onClick={onClose} disabled={acting}>Cancel</button>
            <button className="topic-action-confirm" onClick={onConfirm} disabled={acting}>{acting ? 'Working...' : topicActionCopy[action].button}</button>
          </footer>
        </div>
      </div>
    </div>,
    document.body
  );
}
