export function formatConsumerAddress(host?: string | null): string {
  const address = host?.trim().split('/').pop();
  return address && address !== '-' && address !== 'none' ? address : 'Unavailable';
}
