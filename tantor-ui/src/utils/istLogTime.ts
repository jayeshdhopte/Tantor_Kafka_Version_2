const zonedTimestamp = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:?\d{2})$/;
const timestampParts = new Intl.DateTimeFormat('en-GB', {
  timeZone: 'Asia/Kolkata', year: 'numeric', month: '2-digit', day: '2-digit',
  hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23',
});

function istParts(value: string) {
  if (!zonedTimestamp.test(value)) return null;
  const day = new Date(`${value.slice(0, 10)}T00:00:00Z`);
  const [hour, minute, second] = value.slice(11, 19).split(':').map(Number);
  if (!Number.isFinite(day.getTime()) || day.toISOString().slice(0, 10) !== value.slice(0, 10)
      || hour > 23 || minute > 59 || second > 59) return null;
  const date = new Date(value.replace(' ', 'T'));
  if (!Number.isFinite(date.getTime())) return null;
  return Object.fromEntries(timestampParts.formatToParts(date).map(part => [part.type, part.value]));
}

export function formatIstDateTime(value: string | null | undefined, fallback = '-'): string {
  const parts = value ? istParts(value) : null;
  return parts ? `${parts.day}/${parts.month}/${parts.year} | ${parts.hour}:${parts.minute}:${parts.second} IST` : fallback;
}

// Convert only explicit log timestamp prefixes, never dates embedded in payloads or paths.
const logPrefix = /^(\s*(?:\[(?:TRACE|DEBUG|INFO|WARN|ERROR)\]\s*)?)(?:\[(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:?\d{2}))\]|(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:?\d{2})))(?=\s|\[|$)/;

export function formatLogTimestampsIst(text: string): string {
  return text.replace(/[^\r\n]+/g, line => {
    let remaining = line;
    let formatted = '';
    while (true) {
      const match = logPrefix.exec(remaining);
      if (!match) return formatted + remaining;
      const [, prefix, bracketed, plain] = match;
      const original = bracketed || plain;
      const parts = istParts(original);
      if (!parts) return formatted + remaining;
      const fraction = original.match(/\.\d+/)?.[0] || '';
      const timestamp = `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second}${fraction} IST`;
      formatted += `${prefix}${bracketed ? `[${timestamp}]` : timestamp}`;
      remaining = remaining.slice(match[0].length);
    }
  });
}
