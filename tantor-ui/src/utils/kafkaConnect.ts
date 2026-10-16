export interface ConnectorTask { id: number; state: string; worker_id?: string; trace?: string }
export interface ConnectorRow {
  name: string; class: string; state: string; type?: string; worker?: string;
  tasks: number; runningTasks: number; failedTasks?: number;
  topics?: string[]; topicsAvailable?: boolean; topicsError?: string;
  consumerGroup?: string; consumerGroupSource?: string;
  config: Record<string, string>;
  status: { type?: string; connector?: { state?: string; worker_id?: string; trace?: string }; tasks?: ConnectorTask[] };
}
export const connectorType = (row: ConnectorRow): string => (row.type || row.status?.type || 'UNKNOWN').toUpperCase();
export const connectorStatusClass = (state: string): string => state === 'RUNNING' ? 'ds-status'
  : state === 'PAUSED' || state === 'STOPPED' ? 'ds-status warn' : 'ds-status error';
export function downloadConnectFile(name: string, text: string, type: string) {
  const url = URL.createObjectURL(new Blob([text], { type }));
  const link = document.createElement('a'); link.href = url; link.download = name; link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
export function connectorCsv(rows: ConnectorRow[], instance: string): string {
  const cell = (value: unknown) => {
    let text = String(value ?? '');
    if (/^[\s]*[=+@-]/.test(text) || /^[\t\r\n]/.test(text)) text = "'" + text;
    return '"' + text.replaceAll('"', '""') + '"';
  };
  return [['Name', 'Connect', 'Type', 'Plugin', 'Topics', 'Status', 'Consumer group', 'Running tasks', 'Total tasks'],
    ...rows.map(row => [row.name, instance, connectorType(row), row.class, row.topicsAvailable ? (row.topics || []).join(', ') : 'Unavailable',
      row.state, row.consumerGroup || '', row.runningTasks, row.tasks])].map(row => row.map(cell).join(',')).join('\r\n');
}
