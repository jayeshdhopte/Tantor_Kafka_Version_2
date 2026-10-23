import { useCallback, useEffect, useRef, useState } from 'react';
import { MoreVertical, RotateCcw, Trash2 } from 'lucide-react';
import { AnchoredMenu } from './AnchoredMenu';

type Props = {
  subject: string;
  disabled: boolean;
  onDelete: () => void;
  onRecover: () => void;
};

export function DeletedSchemaSubjectActions({ subject, disabled, onDelete, onRecover }: Props) {
  const [anchor, setAnchor] = useState<HTMLButtonElement | null>(null);
  const menu = useRef<HTMLDivElement>(null);
  const close = useCallback(() => setAnchor(null), []);

  useEffect(() => {
    if (anchor) menu.current?.querySelector<HTMLButtonElement>('button')?.focus();
  }, [anchor]);

  return <>
    <button
      type="button"
      className="ds-deleted-subject-actions-trigger"
      aria-label={`Actions for ${subject}`}
      title="Subject actions"
      aria-haspopup="menu"
      aria-expanded={Boolean(anchor)}
      disabled={disabled}
      onClick={event => setAnchor(anchor ? null : event.currentTarget)}
    ><MoreVertical size={18} /></button>
    {anchor && !disabled && <AnchoredMenu anchor={anchor} className="ds-deleted-subject-actions-menu" minWidth={192} onClose={close}>
      <div ref={menu} role="menu" aria-label={`Actions for ${subject}`}
        onBlur={event => {
          if (!event.currentTarget.contains(event.relatedTarget as Node | null)) close();
        }}
        onKeyDown={event => {
          const buttons = Array.from(event.currentTarget.querySelectorAll<HTMLButtonElement>('button'));
          const current = buttons.indexOf(document.activeElement as HTMLButtonElement);
          if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
            event.preventDefault();
            buttons[(current + (event.key === 'ArrowDown' ? 1 : buttons.length - 1)) % buttons.length]?.focus();
          } else if (event.key === 'Escape') {
            event.stopPropagation();
            close();
            anchor.focus();
          }
        }}>
        <button type="button" role="menuitem" onClick={() => { close(); onRecover(); }}>
          <RotateCcw size={16} /> Recover
        </button>
        <button type="button" role="menuitem" className="danger" onClick={() => { close(); onDelete(); }}>
          <Trash2 size={16} /> Permanently delete
        </button>
      </div>
    </AnchoredMenu>}
  </>;
}
