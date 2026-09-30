package collect

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"io.translab/tantor-agent/internal/executor"
	"io.translab/tantor-agent/pkg/api"
)

// Collector gathers Linux host metrics without third-party dependencies.
type Collector struct {
	hostID       string
	agentName    string
	agentVersion string
	serverURL    string
	exec         executor.Executor
}

func NewCollector(hostID, agentName, agentVersion, serverURL string) *Collector {
	if strings.TrimSpace(agentVersion) == "" {
		agentVersion = "dev"
	}
	return &Collector{
		hostID:       hostID,
		agentName:    agentName,
		agentVersion: strings.TrimSpace(agentVersion),
		serverURL:    strings.TrimSpace(serverURL),
		exec:         executor.New(executor.Options{PrivilegeMode: "direct"}),
	}
}

func (c *Collector) GetHeartbeat() *api.HostHeartbeat {
	memTotal, memUsed := readMemoryMB()
	diskTotal, diskUsed := readDiskGB("/")
	return &api.HostHeartbeat{
		HostID:      c.hostID,
		CPUUsagePct: readCPUUsage(250 * time.Millisecond),
		MemTotalMB:  memTotal,
		MemUsedMB:   memUsed,
		DiskTotalGB: diskTotal,
		DiskUsedGB:  diskUsed,
		JavaVersion: c.getJavaVersion(),
	}
}

func (c *Collector) GetRegistration() *api.HostRegistration {
	hostname, _ := os.Hostname()
	return &api.HostRegistration{
		AgentName:   c.agentName,
		HostID:      c.hostID,
		Hostname:    hostname,
		IPAddresses: c.getLocalIPs(),
		OSDetails:   runtime.GOOS + "_" + runtime.GOARCH,
		AgentVer:    c.agentVersion,
		AgentPath:   executablePath(),
	}
}

func executablePath() string {
	path, err := os.Executable()
	if err != nil {
		return "unknown"
	}
	return path
}

func (c *Collector) getLocalIPs() []string {
	preferred := managementRouteSourceIP(c.serverURL)
	interfaces, err := net.Interfaces()
	if err != nil {
		if preferred != "" {
			return []string{preferred}
		}
		return []string{"127.0.0.1"}
	}

	seen := make(map[string]struct{})
	var regular []string
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 || isVirtualInterfaceName(iface.Name) {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ip, _, err := net.ParseCIDR(addr.String())
			if err != nil {
				ip = net.ParseIP(strings.Split(addr.String(), "%")[0])
			}
			if ip == nil || ip.IsLoopback() || ip.To4() == nil {
				continue
			}
			value := ip.String()
			if _, ok := seen[value]; ok {
				continue
			}
			seen[value] = struct{}{}
			regular = append(regular, value)
		}
	}
	sort.Strings(regular)

	// The backend historically treats the first address as the host's primary
	// address. Prefer the source address that Linux actually routes toward the
	// configured management server, rather than a virbr/docker bridge address.
	if preferred != "" {
		ordered := []string{preferred}
		for _, ip := range regular {
			if ip != preferred {
				ordered = append(ordered, ip)
			}
		}
		return ordered
	}
	if len(regular) == 0 {
		return []string{"127.0.0.1"}
	}
	return regular
}

func managementRouteSourceIP(rawServerURL string) string {
	u, err := url.Parse(strings.TrimSpace(rawServerURL))
	if err != nil || u.Hostname() == "" {
		return ""
	}
	port := u.Port()
	if port == "" {
		if strings.EqualFold(u.Scheme, "https") {
			port = "443"
		} else {
			port = "80"
		}
	}
	conn, err := net.DialTimeout("udp", net.JoinHostPort(u.Hostname(), port), 2*time.Second)
	if err != nil {
		return ""
	}
	defer conn.Close()
	udpAddr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok || udpAddr.IP == nil || udpAddr.IP.IsLoopback() || udpAddr.IP.To4() == nil {
		return ""
	}
	return udpAddr.IP.String()
}

func isVirtualInterfaceName(name string) bool {
	n := strings.ToLower(strings.TrimSpace(name))
	prefixes := []string{"virbr", "docker", "veth", "cni", "flannel", "podman", "br-"}
	for _, prefix := range prefixes {
		if strings.HasPrefix(n, prefix) {
			return true
		}
	}
	return false
}

func (c *Collector) getJavaVersion() string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stdout, stderr, err := c.exec.Run(ctx, "java", "-version")
	if err != nil {
		return "Unknown/Not Installed"
	}
	output := strings.TrimSpace(stderr)
	if output == "" {
		output = strings.TrimSpace(stdout)
	}
	if output == "" {
		return "Unknown"
	}
	return strings.TrimSpace(strings.Split(output, "\n")[0])
}

func readMemoryMB() (totalMB, usedMB int64) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0
	}
	defer f.Close()
	values := make(map[string]int64)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			continue
		}
		values[strings.TrimSuffix(fields[0], ":")] = value // values are kB
	}
	totalKB := values["MemTotal"]
	availableKB := values["MemAvailable"]
	if availableKB == 0 {
		availableKB = values["MemFree"] + values["Buffers"] + values["Cached"]
	}
	usedKB := totalKB - availableKB
	if usedKB < 0 {
		usedKB = 0
	}
	return totalKB / 1024, usedKB / 1024
}

func readDiskGB(path string) (totalGB, usedGB int64) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, 0
	}
	blockSize := uint64(stat.Bsize)
	total := stat.Blocks * blockSize
	available := stat.Bavail * blockSize
	used := total - available
	return int64(total / 1024 / 1024 / 1024), int64(used / 1024 / 1024 / 1024)
}

type cpuSample struct {
	total uint64
	idle  uint64
}

func readCPUUsage(sampleWindow time.Duration) float64 {
	first, err := readCPUSample()
	if err != nil {
		return 0
	}
	timer := time.NewTimer(sampleWindow)
	defer timer.Stop()
	<-timer.C
	second, err := readCPUSample()
	if err != nil || second.total <= first.total {
		return 0
	}
	totalDelta := second.total - first.total
	idleDelta := second.idle - first.idle
	if idleDelta > totalDelta {
		idleDelta = totalDelta
	}
	usage := (float64(totalDelta-idleDelta) / float64(totalDelta)) * 100
	if usage < 0 {
		return 0
	}
	if usage > 100 {
		return 100
	}
	return usage
}

func readCPUSample() (cpuSample, error) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return cpuSample{}, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return cpuSample{}, err
		}
		return cpuSample{}, fmt.Errorf("/proc/stat is empty")
	}
	fields := strings.Fields(scanner.Text())
	if len(fields) < 5 || fields[0] != "cpu" {
		return cpuSample{}, fmt.Errorf("unexpected /proc/stat cpu line")
	}
	var values []uint64
	for _, raw := range fields[1:] {
		v, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return cpuSample{}, err
		}
		values = append(values, v)
	}
	var total uint64
	for _, v := range values {
		total += v
	}
	idle := values[3]
	if len(values) > 4 {
		idle += values[4] // iowait is idle time for utilization reporting
	}
	return cpuSample{total: total, idle: idle}, nil
}
