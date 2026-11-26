import { useEffect, useRef, useState, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { FileText, X } from 'lucide-react';

export type DeploymentSettings = {
  installPath: string; configDirectory: string; javaHome: string; runtimeUser: string;
  brokerDataPaths: string[]; brokerMetadataPath: string; brokerLogPath: string; brokerHeapXmx: string;
  controllerDataPaths: string[]; controllerMetadataPath: string; controllerLogPath: string; controllerHeapXmx: string;
};

type Props = {
  value: DeploymentSettings; onChange: (value: DeploymentSettings) => void; disabled: boolean;
  brokerOnly?: boolean; role?: 'broker' | 'controller' | 'separate'; children?: ReactNode;
};

function SettingsFields({ value, onChange, role = 'separate', children }: Omit<Props, 'disabled'>) {
  const field = (key: keyof DeploymentSettings, label: string, help?: string) => <label key={key}>{label}
    <input aria-label={label} required={key !== 'brokerMetadataPath'} value={Array.isArray(value[key]) ? (value[key] as string[]).join(',') : value[key]}
      onChange={event => {
        const text = event.target.value;
        onChange({ ...value, [key]: key.endsWith('DataPaths') ? text.split(',').map(path => path.trim()) : text,
          ...(key === 'installPath' ? { configDirectory: `${text.replace(/\/+$/, '')}/config` } : {}) });
      }} />
    {help && <small>{help}</small>}
  </label>;
  return <>
    <div className="add-node-fields">
      {field('installPath', 'Install directory', 'Kafka installation path. Configuration is derived as <install directory>/config.')}
      {children}
    </div>
    {(['broker', 'controller'] as const).filter(item => role === 'separate' || role === item).map(item => {
      const broker = item === 'broker';
      const label = broker ? 'Broker' : 'Controller';
      return <section className="add-node-process-settings" key={item} aria-label={`${label} deployment settings`}>
        <h3>{label} configuration</h3><div className="add-node-fields">
          {field(broker ? 'brokerDataPaths' : 'controllerDataPaths', `${label} data directory`, 'Separate multiple paths with commas. Broker and controller storage must remain isolated.')}
          {field(broker ? 'brokerLogPath' : 'controllerLogPath', `${label} log directory`)}
          {field(broker ? 'brokerMetadataPath' : 'controllerMetadataPath', `${label} metadata directory`, broker ? 'Optional; defaults to the first data directory.' : 'Persistent controller metadata directory.')}
          {field(broker ? 'brokerHeapXmx' : 'controllerHeapXmx', `${label} max heap`, 'For example 1G or 512M. Readiness checks validate available host memory.')}
        </div></section>;
    })}
  </>;
}

function ConfigDialog({ value, onSave, onClose, role }: {
  value: DeploymentSettings; onSave: (settings: DeploymentSettings) => void; onClose: () => void; role: Props['role'];
}) {
  const [draft, setDraft] = useState(value);
  const dialog = useRef<HTMLDialogElement>(null);
  useEffect(() => { dialog.current?.showModal(); }, []);
  return createPortal(<dialog ref={dialog} className="add-node-config-dialog" aria-labelledby="add-node-config-heading" onCancel={onClose}>
    <form onSubmit={event => { event.preventDefault(); onSave(draft); onClose(); }}>
      <header><div><h2 id="add-node-config-heading">Deployment configuration</h2><p>Review and edit the candidate deployment settings.</p></div>
        <button type="button" className="add-node-button" aria-label="Close configuration" onClick={onClose}><X size={18} /></button></header>
      <div className="add-node-config-content"><SettingsFields value={draft} onChange={setDraft} role={role} />
        <p className="add-node-config-derived">Configuration directory: <code>{draft.configDirectory}</code></p>
        <p className="add-node-config-help">Kafka identity and generated properties remain controlled by the existing readiness and deployment flow. Saving settings requires fresh checks.</p>
      </div>
      <footer><button type="button" className="add-node-button" onClick={onClose}>Cancel</button><button className="add-node-button primary" type="submit">Save settings</button></footer>
    </form>
  </dialog>, document.body);
}

export function AddNodeDeploymentSettings({ value, onChange, disabled, brokerOnly = false, role, children }: Props) {
  const [configOpen, setConfigOpen] = useState(false);
  const visibleRole = role ?? (brokerOnly ? 'broker' : 'separate');
  return <fieldset disabled={disabled} className="add-node-deployment-settings">
    <legend>Deployment paths &amp; configuration</legend>
    <div className="add-node-settings-heading"><p>These settings apply to the new host. Use approved absolute paths.</p>
      <button type="button" className="add-node-button" disabled={disabled} onClick={() => setConfigOpen(true)}><FileText size={16} />Config</button></div>
    <SettingsFields value={value} onChange={onChange} role={visibleRole}>{children}</SettingsFields>
    <p className="add-node-config-derived">Configuration directory: <code>{value.configDirectory}</code></p>
    <details className="add-node-identity"><summary>Runtime profile</summary><p>Java path and runtime user are checked during readiness; they are not entered manually here.</p>
      <dl className="add-node-review"><dt>Profile Java path</dt><dd>{value.javaHome || 'Not reported'}</dd><dt>Profile runtime user</dt><dd>{value.runtimeUser || 'Not reported'}</dd></dl>
    </details>
    {configOpen && !disabled && <ConfigDialog value={value} role={visibleRole} onSave={onChange} onClose={() => setConfigOpen(false)} />}
  </fieldset>;
}
