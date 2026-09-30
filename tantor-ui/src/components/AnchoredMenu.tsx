import { useLayoutEffect, useRef, useState, type CSSProperties, type ReactNode } from 'react';
import { createPortal } from 'react-dom';

type AnchoredMenuProps = {
  anchor: HTMLElement;
  className: string;
  children: ReactNode;
  onClose: () => void;
  align?: 'start' | 'end';
  matchAnchorWidth?: boolean;
  minWidth?: number;
  gap?: number;
  placement?: 'auto' | 'above' | 'below';
};

export function AnchoredMenu({
  anchor,
  className,
  children,
  onClose,
  align = 'end',
  matchAnchorWidth = false,
  minWidth,
  gap = 6,
  placement = 'auto',
}: AnchoredMenuProps) {
  const menuRef = useRef<HTMLDivElement>(null);
  const [style, setStyle] = useState<CSSProperties>({
    position: 'fixed',
    visibility: 'hidden',
    width: matchAnchorWidth ? anchor.getBoundingClientRect().width : undefined,
  });

  useLayoutEffect(() => {
    const updatePosition = () => {
      const anchorRect = anchor.getBoundingClientRect();
      const menuRect = menuRef.current?.getBoundingClientRect();
      const menuWidth = matchAnchorWidth
        ? anchorRect.width
        : Math.max(menuRect?.width || 190, minWidth || 0);
      const menuHeight = menuRect?.height || 160;
      const viewportGap = 8;
      const spaceBelow = window.innerHeight - anchorRect.bottom;
      const spaceAbove = anchorRect.top;
      const openAbove = placement === 'above' || (placement === 'auto' && spaceBelow < Math.min(menuHeight + gap, 180) && spaceAbove > spaceBelow);
      const availableHeight = Math.max(
        96,
        (openAbove ? spaceAbove : spaceBelow) - gap - viewportGap,
      );
      const naturalLeft = align === 'start'
        ? anchorRect.left
        : anchorRect.right - menuWidth;
      const left = Math.min(
        window.innerWidth - menuWidth - viewportGap,
        Math.max(viewportGap, naturalLeft),
      );
      setStyle({
        position: 'fixed',
        top: openAbove ? 'auto' : anchorRect.bottom + gap,
        left,
        right: 'auto',
        bottom: openAbove ? window.innerHeight - anchorRect.top + gap : 'auto',
        width: matchAnchorWidth ? anchorRect.width : undefined,
        minWidth,
        maxWidth: 'calc(100vw - 16px)',
        maxHeight: Math.min(360, availableHeight),
        overflowY: 'auto',
        margin: 0,
        transform: 'none',
        visibility: 'visible',
        zIndex: 25000,
      });
    };

    const handlePointerDown = (event: MouseEvent) => {
      const target = event.target as Node;
      if (!anchor.contains(target) && !menuRef.current?.contains(target)) onClose();
    };

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
    };

    updatePosition();
    const frame = window.requestAnimationFrame(updatePosition);
    window.addEventListener('resize', updatePosition);
    window.addEventListener('scroll', updatePosition, true);
    document.addEventListener('mousedown', handlePointerDown);
    document.addEventListener('keydown', handleKeyDown);
    return () => {
      window.cancelAnimationFrame(frame);
      window.removeEventListener('resize', updatePosition);
      window.removeEventListener('scroll', updatePosition, true);
      document.removeEventListener('mousedown', handlePointerDown);
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, [align, anchor, gap, matchAnchorWidth, minWidth, onClose, placement]);

  return createPortal(
    <div ref={menuRef} className={className} style={style}>
      {children}
    </div>,
    document.body,
  );
}
