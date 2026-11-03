import type { ConnectorRow } from './kafkaConnect';

type Snapshot = {
  connectors: ConnectorRow[]; connectorCount: number; taskCount: number;
  runningTasks: number; runningConnectors: number; pausedConnectors: number; failedConnectors: number;
};

export class ConnectorSummaryReadError extends Error {
  status: number;
  constructor(message: string, status: number) { super(message); this.status = status; }
}

export function acceptedConnector(body: Record<string, unknown>, response: Record<string, unknown>): ConnectorRow {
  const name = typeof response.name === 'string' ? response.name : typeof body.name === 'string' ? body.name : '';
  if (!name) throw new Error('The connector was accepted but its name was not returned.');
  const rawConfig = response.config && typeof response.config === 'object' ? response.config
    : body.config && typeof body.config === 'object' ? body.config : {};
  const config: Record<string, string> = {};
  for (const [key, value] of Object.entries(rawConfig)) if (typeof value === 'string') config[key] = value;
  const type = typeof response.type === 'string' ? response.type.toUpperCase() : 'UNKNOWN';
  return { name, config, class: config['connector.class'] || '', type,
    state: 'INITIALIZING', initializing: true, tasks: 0, runningTasks: 0, failedTasks: 0,
    status: { connector: { state: 'INITIALIZING' }, tasks: [] }, topics: [], topicsAvailable: false,
    topicsError: 'Connector is initializing; tracked topics are not available yet.' };
}

export function mergeCreatedConnectors<T extends Snapshot>(snapshot: T, created: ConnectorRow[]): T {
  const rows = new Map(snapshot.connectors.map(row => [row.name, row]));
  for (const pending of created) {
    const current = rows.get(pending.name);
    if (!current) rows.set(pending.name, pending);
    else if (current.initializing) rows.set(pending.name, { ...current,
      config: Object.keys(current.config || {}).length ? current.config : pending.config,
      class: current.class || pending.class,
      type: current.type && current.type !== 'UNKNOWN' ? current.type : pending.type });
  }
  const connectors = [...rows.values()];
  return { ...snapshot, connectors, connectorCount: connectors.length,
    taskCount: connectors.reduce((sum, row) => sum + row.tasks, 0),
    runningTasks: connectors.reduce((sum, row) => sum + row.runningTasks, 0),
    runningConnectors: connectors.filter(row => row.state === 'RUNNING').length,
    pausedConnectors: connectors.filter(row => row.state === 'PAUSED').length,
    failedConnectors: connectors.filter(row => row.state === 'FAILED').length };
}

function waitForRetry(signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const cancel = () => { clearTimeout(timer); signal.removeEventListener('abort', cancel); reject(signal.reason); };
    const timer = setTimeout(() => { signal.removeEventListener('abort', cancel); resolve(); }, 1000);
    signal.addEventListener('abort', cancel, { once: true });
    if (signal.aborted) cancel();
  });
}

/** Reconcile accepted creations using GET only, independently of the Live preference. */
export async function refreshCreatedConnectors<T extends Snapshot>({ created, read, publish, signal, attempts = 12, wait = waitForRetry }: {
  created: ConnectorRow[]; read: (signal: AbortSignal) => Promise<T>; publish: (snapshot: T) => void;
  signal: AbortSignal; attempts?: number; wait?: (signal: AbortSignal) => Promise<void>;
}): Promise<boolean> {
  for (let attempt = 0; attempt < attempts; attempt++) {
    signal.throwIfAborted();
    try {
      const snapshot = await read(signal);
      signal.throwIfAborted();
      publish(mergeCreatedConnectors(snapshot, created));
      const ready = created.every(pending => {
        const current = snapshot.connectors.find(row => row.name === pending.name);
        return current && !current.initializing && current.state !== 'INITIALIZING' && current.state !== 'UNKNOWN'
          && (current.tasks > 0 || ['FAILED', 'PAUSED', 'STOPPED'].includes(current.state));
      });
      if (ready) return true;
    } catch (failure) {
      signal.throwIfAborted();
      if (!(failure instanceof ConnectorSummaryReadError) || failure.status !== 404) throw failure;
    }
    if (attempt + 1 < attempts) await wait(signal);
  }
  return false;
}
