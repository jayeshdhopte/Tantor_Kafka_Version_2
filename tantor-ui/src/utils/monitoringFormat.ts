export const formatMonitoringNumber = (value?: number | null, digits = 0): string => {
  if (value === undefined || value === null || !Number.isFinite(value)) return 'N/A';
  if (value !== 0 && Math.abs(value) < 0.000001) {
    return value < 0 ? '>-0.000001' : '<0.000001';
  }
  const precision = value !== 0 && Math.abs(value) < 1
    ? Math.min(6, Math.max(digits, Math.ceil(-Math.log10(Math.abs(value))) + 1))
    : digits;
  return value.toLocaleString(undefined, { maximumFractionDigits: precision, minimumFractionDigits: digits });
};

export const formatMonitoringBytes = (value?: number | null): string => {
  if (value === undefined || value === null || !Number.isFinite(value)) return 'N/A';
  if (value <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let next = value;
  let unit = 0;
  while (next >= 1024 && unit < units.length - 1) {
    next /= 1024;
    unit += 1;
  }
  return `${formatMonitoringNumber(next, unit === 0 ? 0 : 2)} ${units[unit]}`;
};
