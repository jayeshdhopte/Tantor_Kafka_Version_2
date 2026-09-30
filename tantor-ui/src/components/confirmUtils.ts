const CONFIRM_EVENT = 'tantor:confirm';

export type ConfirmRequest = {
  message: string;
  title?: string;
  confirmLabel?: string;
  showCancel?: boolean;
  resolve?: (confirmed: boolean) => void;
};

export { CONFIRM_EVENT };

export const confirmAction = (
  message: string,
  options: { title?: string; confirmLabel?: string } = {},
): Promise<boolean> => {
  return new Promise<boolean>(resolve => {
    window.dispatchEvent(new CustomEvent<ConfirmRequest>(CONFIRM_EVENT, {
      detail: { message, resolve, ...options },
    }));
  });
};

export const notifyAction = (
  message: string,
  options: { title?: string; confirmLabel?: string } = {},
): void => {
  window.dispatchEvent(new CustomEvent<ConfirmRequest>(CONFIRM_EVENT, {
    detail: {
      message,
      title: options.title || 'Notice',
      confirmLabel: options.confirmLabel || 'OK',
      showCancel: false,
    },
  }));
};
