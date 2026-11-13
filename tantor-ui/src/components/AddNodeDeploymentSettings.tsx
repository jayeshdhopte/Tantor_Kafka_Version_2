export type DeploymentSettings = {
  installPath: string; configDirectory: string; javaHome: string; runtimeUser: string;
  brokerDataPaths: string[]; brokerMetadataPath: string; brokerLogPath: string; brokerHeapXmx: string;
  controllerDataPaths: string[]; controllerMetadataPath: string; controllerLogPath: string; controllerHeapXmx: string;
};

export function AddNodeDeploymentSettings({ value, onChange, disabled }: {
  value: DeploymentSettings; onChange: (value: DeploymentSettings) => void; disabled: boolean;
}) {
  const field = (key: keyof DeploymentSettings, label: string, help?: string) => <label key={key}>{label}
    <input aria-label={label} value={Array.isArray(value[key]) ? (value[key] as string[]).join(',') : value[key]}
      disabled={disabled} onChange={event => onChange({ ...value, [key]: key.endsWith('DataPaths') ? event.target.value.split(',') : event.target.value })} />
    {help && <small>{help}</small>}
  </label>;
  return <fieldset disabled={disabled} className="add-node-deployment-settings">
    <legend>Candidate deployment settings</legend>
    <p>New processes use broker.service and controller.service. These settings apply only to the selected new host. Changing a value requires fresh checks.</p>
    <p>Writable paths must be below /opt, /var, /data, /srv or /app. JAVA_HOME also accepts a JDK directory below /usr/lib/jvm. Paths must be absolute, with no spaces, traversal or symlinks.</p>
    <div className="add-node-fields">
      {field('installPath', 'Kafka installation directory')}
      {field('configDirectory', 'Configuration directory', 'broker.properties and controller.properties share this directory; their storage and logs remain separate.')}
      {field('javaHome', 'JAVA_HOME')}{field('runtimeUser', 'Runtime user', 'An existing service account on the candidate.')}
      {field('brokerDataPaths', 'Broker data paths', 'Separate multiple paths with commas.')}
      {field('brokerMetadataPath', 'Broker metadata path', 'Leave empty to use the first broker data directory.')}
      {field('brokerLogPath', 'Broker application log path')}{field('brokerHeapXmx', 'Broker maximum heap', 'For example 1G or 512M. Host memory admission still applies.')}
      {field('controllerDataPaths', 'Controller data paths', 'Used for Controller and Broker + Controller additions.')}
      {field('controllerMetadataPath', 'Controller metadata path')}
      {field('controllerLogPath', 'Controller application log path')}{field('controllerHeapXmx', 'Controller maximum heap')}
    </div>
    <p>Kafka version, cluster ID, sequential node IDs, listener ports and resource admission thresholds remain inherited or server-controlled.</p>
  </fieldset>;
}
