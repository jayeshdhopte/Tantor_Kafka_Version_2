import { useEffect, useRef } from 'react';
import { createPortal } from 'react-dom';
import { Trash2 } from 'lucide-react';

type Props = { subjects: string[]; saving: boolean; onCancel: () => void; onConfirm: () => void };

export function BulkSchemaDeleteDialog({ subjects, saving, onCancel, onConfirm }: Props) {
  const dialog = useRef<HTMLDialogElement>(null);
  useEffect(() => {
    const element = dialog.current;
    const previousFocus = document.activeElement;
    element?.showModal();
    return () => {
      element?.close();
      if (previousFocus instanceof HTMLElement && previousFocus.isConnected) previousFocus.focus();
    };
  }, []);

  return createPortal(<dialog ref={dialog} className="ds-schema-bulk-dialog" aria-labelledby="schema-bulk-delete-title"
    onCancel={event => { event.preventDefault(); if (!saving) onCancel(); }}>
    <h2 id="schema-bulk-delete-title"><Trash2 size={20} /> Soft delete {subjects.length} {subjects.length === 1 ? 'subject' : 'subjects'}?</h2>
    <p>These subjects will move to the soft-deleted list. Kafka records will not be deleted.</p>
    <ul>{subjects.map(subject => <li key={subject}>{subject}</li>)}</ul>
    <div className="ds-schema-bulk-dialog-actions">
      <button type="button" className="ds-button outlined" onClick={onCancel} disabled={saving} autoFocus>Cancel</button>
      <button type="button" className="ds-button primary" onClick={onConfirm} disabled={saving}>
        <Trash2 size={16} />{saving ? 'Deleting...' : 'Soft delete'}
      </button>
    </div>
  </dialog>, document.body);
}
