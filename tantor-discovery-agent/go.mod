module io.translab/tantor-discovery-agent

go 1.22.0

require (
    github.com/shirou/gopsutil/v3 v3.24.5
    gopkg.in/yaml.v3 v3.0.1
    io.translab/tantor-agent v0.0.0
)

replace io.translab/tantor-agent => ../internal-agent/source
