import type { SchemaRegistryConfig, KafkaConnectConfig } from './serviceConfiguration';
export interface ServiceArtifact { id: string; version?: string; serviceType?: string; status?: string; fileName?: string; filename?: string; sha256?: string; checksum?: string }

export function SchemaRegistryFields({ cfg, artifacts, onChange, unifiedDefault = true }: { cfg: SchemaRegistryConfig; artifacts: ServiceArtifact[]; onChange: (patch: Partial<SchemaRegistryConfig>) => void; unifiedDefault?: boolean }) {
  return <div className="cd-node-config-editor">
                  <div className="cd-node-config-top"><div><h3>Schema Registry</h3><p>{unifiedDefault ? "Leave install/config directories blank to use Schema Registry in this host's unified Kafka package." : "Configure this host's Schema Registry deployment."}</p></div></div>
                  <div className="cd-grid-2">
                    <label className="cd-field"><span>Artifact</span><select value={cfg.artifactId} onChange={e => onChange({ artifactId: e.target.value })}><option value="">Select uploaded artifact</option>{artifacts.map(a => <option key={a.id} value={a.id}>{a.version || a.id}</option>)}</select></label>
                    <label className="cd-field"><span>REST API port</span><input type="number" min={1} max={65535} value={cfg.port} onChange={e => onChange({ port: Number(e.target.value) })} /></label>
                    <label className="cd-field"><span>Min Heap</span><input value={cfg.heapMin} onChange={e => onChange({ heapMin: e.target.value })} placeholder="1G" /></label>
                    <label className="cd-field"><span>Max Heap</span><input value={cfg.heapSize} onChange={e => onChange({ heapSize: e.target.value })} placeholder="1G" /></label>
                    <label className="cd-field"><span>Compatibility level</span><select value={cfg.compatibility} onChange={e => onChange({ compatibility: e.target.value })}>{['BACKWARD', 'BACKWARD_TRANSITIVE', 'FORWARD', 'FORWARD_TRANSITIVE', 'FULL', 'FULL_TRANSITIVE', 'NONE'].map(level => <option value={level} key={level}>{level}</option>)}</select></label>
                    <label className="cd-field"><span>Install directory</span><input placeholder="Kafka installation" value={cfg.installDir} onChange={e => onChange({ installDir: e.target.value })} /></label>
                    <label className="cd-field"><span>Config directory</span><input placeholder="Kafka installation/etc/schema-registry" value={cfg.configDir} onChange={e => onChange({ configDir: e.target.value })} /></label>
                    <label className="cd-field"><span>Log directory</span><input value={cfg.logDir} onChange={e => onChange({ logDir: e.target.value })} /></label>
                    <label className="cd-field"><span>Working directory</span><input value={cfg.workingDir} onChange={e => onChange({ workingDir: e.target.value })} /></label>
                  </div>
                </div>;
}

export function KafkaConnectFields({ cfg, artifacts, onChange, unifiedDefault = true }: { cfg: KafkaConnectConfig; artifacts: ServiceArtifact[]; onChange: (patch: Partial<KafkaConnectConfig>) => void; unifiedDefault?: boolean }) {
  return <div className="cd-node-config-editor">
                  <div className="cd-node-config-top"><div><h3>Kafka Connect</h3><p>{unifiedDefault ? "Leave directories blank to use this host's existing unified Kafka installation and default Connect log/work paths. Enter a path to override it." : "Configure this host's distributed Connect worker and service directories."}</p></div></div>
                  <div className="cd-grid-2">
                    <label className="cd-field"><span>Artifact</span><select value={cfg.artifactId} onChange={e => onChange({ artifactId: e.target.value })}><option value="">Select uploaded artifact</option>{artifacts.map(a => <option key={a.id} value={a.id}>{a.version || a.id}</option>)}</select></label>
                    <label className="cd-field"><span>REST port</span><input type="number" min={1} max={65535} value={cfg.port} onChange={e => onChange({ port: Number(e.target.value) })} /></label>
                    <label className="cd-field"><span>Min Heap</span><input value={cfg.heapMin} onChange={e => onChange({ heapMin: e.target.value })} placeholder="1G" /></label>
                    <label className="cd-field"><span>Max Heap</span><input value={cfg.heapSize} onChange={e => onChange({ heapSize: e.target.value })} placeholder="1G" /></label>
                    <label className="cd-field"><span>Worker group ID</span><input value={cfg.groupId} onChange={e => onChange({ groupId: e.target.value })} /></label>
                    <label className="cd-field"><span>Install directory</span><input placeholder="Existing Kafka installation" value={cfg.installDir} onChange={e => onChange({ installDir: e.target.value })} /></label>
                    <label className="cd-field"><span>Config directory</span><input placeholder="Kafka installation/config" value={cfg.configDir} onChange={e => onChange({ configDir: e.target.value })} /></label>
                    <label className="cd-field"><span>Log directory</span><input placeholder="/var/log/tantor/kafka-connect" value={cfg.logDir} onChange={e => onChange({ logDir: e.target.value })} /></label>
                    <label className="cd-field"><span>Working directory</span><input placeholder="/var/lib/tantor/kafka-connect" value={cfg.workingDir} onChange={e => onChange({ workingDir: e.target.value })} /></label>
                    <label className="cd-field"><span>Plugin directory</span><input placeholder="Kafka installation/connect-plugins" value={cfg.pluginDir} onChange={e => onChange({ pluginDir: e.target.value })} /></label>
                  </div>
                  <div className="cd-template-summary"><strong>Air-gapped plugin requirement:</strong><span>Existing connector plugins are preserved. The default plugin directory must exist and be accessible. The working directory needs at least 5 GB free.</span></div>
                </div>;
}
