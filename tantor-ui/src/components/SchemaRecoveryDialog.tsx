import { useEffect, useRef } from 'react';
import { createPortal } from 'react-dom';
import { Loader2, RotateCcw } from 'lucide-react';

type Props = {
  subject: string;
  version: number;
  schemaId?: number | null;
  schemaType: string;
  schema: string;
  saving: boolean;
  error: string | null;
  onCancel: () => void;
  onConfirm: () => void;
};

export function SchemaRecoveryDialog({ subject, version, schemaId, schemaType, schema, saving, error, onCancel, onConfirm }: Props) {
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

  return createPortal(<dialog ref={dialog} className="ds-schema-recovery-dialog"
    aria-labelledby="schema-recovery-title" aria-describedby="schema-recovery-description" aria-busy={saving}
    onCancel={event => { event.preventDefault(); if (!saving) onCancel(); }}>
    <header className="ds-schema-recovery-header">
      <RotateCcw size={22} aria-hidden="true" />
      <h2 id="schema-recovery-title">Recover schema version</h2>
    </header>
    <div className="ds-schema-recovery-body">
      <dl className="ds-schema-recovery-facts">
        <div className="ds-schema-recovery-subject"><dt>Subject</dt><dd>{subject}</dd></div>
        <div><dt>Original version</dt><dd>{version}</dd></div>
        <div><dt>Schema ID</dt><dd>{schemaId ?? 'Unavailable'}</dd></div>
        <div><dt>Type</dt><dd>{schemaType}</dd></div>
      </dl>
      <p id="schema-recovery-description">Re-register the original schema and its references. The registry assigns the resulting version. Only this schema is recovered; other deleted versions remain unchanged.</p>
      <section className="ds-schema-recovery-preview" aria-labelledby="schema-recovery-preview-title">
        <h3 id="schema-recovery-preview-title">Schema preview</h3>
        <pre tabIndex={0}>{schema}</pre>
      </section>
      {error && <p className="ds-alert" role="alert">{error}</p>}
    </div>
    <footer className="ds-schema-recovery-footer">
      <button type="button" className="ds-button outlined" disabled={saving} onClick={onCancel} autoFocus>Cancel</button>
      <button type="button" className="ds-button primary" disabled={saving} onClick={onConfirm}>
        {saving ? <Loader2 size={16} className="ds-schema-recovery-spinner" aria-hidden="true" /> : <RotateCcw size={16} aria-hidden="true" />}
        {saving ? 'Recovering...' : 'Recover schema'}
      </button>
    </footer>
  </dialog>, document.body);
}
