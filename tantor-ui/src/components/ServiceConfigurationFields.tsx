import type { SchemaRegistryConfig, KafkaConnectConfig } from './serviceConfiguration';
export interface ServiceArtifact { id: string; version?: string; serviceType?: string; status?: string; fileName?: string; filename?: string; sha256?: string; checksum?: string }

export function SchemaRegistryFields({ cfg, artifacts, onChange }: { cfg: SchemaRegistryConfig; artifacts: ServiceArtifact[]; onChange: (patch: Partial<SchemaRegistryConfig>) => void }) {
  return <div className="cd-node-config-editor">
                  <div className="cd-node-config-top"><div><h3>Schema Registry</h3><p>Configure this host's Schema Registry deployment.</p></div></div>
                  <div className="cd-grid-2">
                    <label className="cd-field"><span>Artifact</span><select value={cfg.artifactId} onChange={e => onChange({ artifactId: e.target.value })}><option value="">Select uploaded artifact</option>{artifacts.map(a => <option key={a.id} value={a.id}>{a.version || a.id}</option>)}</select></label>
                    <label className="cd-field"><span>REST API port</span><input type="number" min={1} max={65535} value={cfg.port} onChange={e => onChange({ port: Number(e.target.value) })} /></label>
                    <label className="cd-field"><span>Heap size</span><input value={cfg.heapSize} onChange={e => onChange({ heapSize: e.target.value })} /></label>
                    <label className="cd-field"><span>Compatibility level</span><select value={cfg.compatibility} onChange={e => onChange({ compatibility: e.target.value })}>{['BACKWARD', 'BACKWARD_TRANSITIVE', 'FORWARD', 'FORWARD_TRANSITIVE', 'FULL', 'FULL_TRANSITIVE', 'NONE'].map(level => <option value={level} key={level}>{level}</option>)}</select></label>
                    <label className="cd-field"><span>Install directory</span><input value={cfg.installDir} onChange={e => onChange({ installDir: e.target.value })} /></label>
                    <label className="cd-field"><span>Config directory</span><input value={cfg.configDir} onChange={e => onChange({ configDir: e.target.value })} /></label>
                    <label className="cd-field"><span>Log directory</span><input value={cfg.logDir} onChange={e => onChange({ logDir: e.target.value })} /></label>
                    <label className="cd-field"><span>Working directory</span><input value={cfg.workingDir} onChange={e => onChange({ workingDir: e.target.value })} /></label>
                  </div>
                </div>;
}

export function KafkaConnectFields({ cfg, artifacts, onChange }: { cfg: KafkaConnectConfig; artifacts: ServiceArtifact[]; onChange: (patch: Partial<KafkaConnectConfig>) => void }) {
  return <div className="cd-node-config-editor">
                  <div className="cd-node-config-top"><div><h3>Kafka Connect</h3><p>Configure this host's distributed Connect worker.</p></div></div>
                  <div className="cd-grid-2">
                    <label className="cd-field"><span>Artifact</span><select value={cfg.artifactId} onChange={e => onChange({ artifactId: e.target.value })}><option value="">Select uploaded artifact</option>{artifacts.map(a => <option key={a.id} value={a.id}>{a.version || a.id}</option>)}</select></label>
                    <label className="cd-field"><span>REST port</span><input type="number" min={1} max={65535} value={cfg.port} onChange={e => onChange({ port: Number(e.target.value) })} /></label>
                    <label className="cd-field"><span>Heap size</span><input value={cfg.heapSize} onChange={e => onChange({ heapSize: e.target.value })} /></label>
                    <label className="cd-field"><span>Worker group ID</span><input value={cfg.groupId} onChange={e => onChange({ groupId: e.target.value })} /></label>
                    <label className="cd-field"><span>Install directory</span><input value={cfg.installDir} onChange={e => onChange({ installDir: e.target.value })} /></label>
                    <label className="cd-field"><span>Config directory</span><input value={cfg.configDir} onChange={e => onChange({ configDir: e.target.value })} /></label>
                    <label className="cd-field"><span>Log directory</span><input value={cfg.logDir} onChange={e => onChange({ logDir: e.target.value })} /></label>
                    <label className="cd-field"><span>Working directory</span><input value={cfg.workingDir} onChange={e => onChange({ workingDir: e.target.value })} /></label>
                    <label className="cd-field"><span>Plugin directory</span><input value={cfg.pluginDir} onChange={e => onChange({ pluginDir: e.target.value })} /></label>
                  </div>
                  <div className="cd-template-summary"><strong>Air-gapped plugin requirement:</strong><span>Pre-stage at least one connector JAR in the plugin directory. The working directory needs at least 5 GB free.</span></div>
                </div>;
}
