package monitoring

const PrometheusSystemdTemplate = `[Unit]
Description=Prometheus
Wants=network-online.target
After=network-online.target

[Service]
User={{.User}}
Group={{.Group}}
Type=simple
ExecStart={{.InstallDir}}/prometheus \
    --config.file {{.InstallDir}}/prometheus.yml \
    --storage.tsdb.path {{.DataDir}} \
    --web.console.templates={{.InstallDir}}/consoles \
    --web.console.libraries={{.InstallDir}}/console_libraries

[Install]
WantedBy=multi-user.target
`

const PrometheusConfigTemplate = `
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'kafka'
    static_configs:
      - targets: ['localhost:7071'] # JMX Exporter
  - job_name: 'node'
    static_configs:
      - targets: ['localhost:9100']
`

const GrafanaSystemdTemplate = `[Unit]
Description=Grafana
Wants=network-online.target
After=network-online.target

[Service]
User={{.User}}
Group={{.Group}}
Type=simple
WorkingDirectory={{.InstallDir}}
ExecStart={{.InstallDir}}/bin/grafana-server \
    --config={{.InstallDir}}/conf/defaults.ini \
    --homepath={{.InstallDir}}

[Install]
WantedBy=multi-user.target
`

const JmxExporterConfigTemplate = `
startDelaySeconds: 0
hostPort: 127.0.0.1:7071
jmxUrl: service:jmx:rmi:///jndi/rmi://127.0.0.1:7071/jmxrmi
ssl: false
lowercaseOutputName: true
lowercaseOutputLabelNames: true
`
