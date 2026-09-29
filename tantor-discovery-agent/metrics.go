package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	maxMetricsResponseBytes = 16 << 20
	maxSamplesPerScrape     = 10000
	maxSamplesPerFamily     = 1000
)

type MetricSampleDTO struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	Value  float64           `json:"value"`
	Type   string            `json:"type,omitempty"`
}

type CollectorStatusDTO struct {
	Configured       bool   `json:"configured"`
	Available        bool   `json:"available"`
	Endpoint         string `json:"endpoint,omitempty"`
	LastAttemptAt    string `json:"lastAttemptAt"`
	LastSuccessAt    string `json:"lastSuccessAt,omitempty"`
	ScrapeDurationMs int64  `json:"scrapeDurationMs"`
	SamplesCollected int    `json:"samplesCollected"`
	Error            string `json:"error,omitempty"`
	Truncated        bool   `json:"truncated"`
}

type ExternalBrokerMetricsDTO struct {
	Hostname             string             `json:"hostname"`
	Bootstrap            string             `json:"bootstrap"`
	NodeID               int                `json:"nodeId"`
	CollectedAt          string             `json:"collectedAt"`
	CPUUsagePct          float64            `json:"cpuUsagePct"`
	MemoryUsedMB         int64              `json:"memoryUsedMb"`
	MemoryTotalMB        int64              `json:"memoryTotalMb"`
	DiskUsedGB           int64              `json:"diskUsedGb"`
	DiskTotalGB          int64              `json:"diskTotalGb"`
	DiskUsedBytes        int64              `json:"diskUsedBytes"`
	DiskTotalBytes       int64              `json:"diskTotalBytes"`
	MessagesInPerSec     float64            `json:"messagesInPerSec"`
	BytesInPerSec        float64            `json:"bytesInPerSec"`
	JMXCollector         CollectorStatusDTO `json:"jmxCollector"`
	KafkaExporter        CollectorStatusDTO `json:"kafkaExporter"`
	JMXMetrics           []MetricSampleDTO  `json:"jmxMetrics,omitempty"`
	KafkaExporterMetrics []MetricSampleDTO  `json:"kafkaExporterMetrics,omitempty"`
}

type jmxCounterState struct {
	messages float64
	bytes    float64
	at       time.Time
	ready    bool
}

type prometheusScrape struct {
	samples   []MetricSampleDTO
	types     map[string]string
	truncated bool
}

// Only operational Kafka/JVM families from the monitoring catalogue are
// forwarded. This prevents accidental ingestion of arbitrary exporter/process
// metrics while still covering broker, request, replication, controller/KRaft,
// log-cleaner, JVM and security telemetry.
var jmxMetricPrefixes = []string{
	"kafka_server_brokertopicmetrics_",
	"kafka_server_replicamanager_",
	"kafka_server_kafkarequesthandlerpool_",
	"kafka_server_socket_server_metrics_",
	"kafka_server_delayedoperationpurgatory_",
	"kafka_server_kafkaserver_",
	"kafka_server_kafkacontroller_",
	"kafka_controller_",
	"kafka_network_requestmetrics_",
	"kafka_network_socketserver_",
	"kafka_log_log_",
	"kafka_log_logcleaner_",
	"kafka_log_logflushstats_",
	"kafka_cluster_partition_",
	"kafka_cluster_replica_",
	"kafka_raft_",
	"kafka_metadata_",
	"jvm_memory_",
	"jvm_gc_",
	"jvm_threads_",
	"jvm_classes_",
	"jvm_buffer_pool_",
	"process_cpu_",
	"process_resident_memory_",
	"process_virtual_memory_",
	"process_open_fds",
	"process_max_fds",
	"process_start_time_seconds",
}

// Kafka Exporter naming differs slightly between releases, so controlled
// prefixes are used. Topic/partition/group labels are retained for drill-down.
var kafkaExporterMetricPrefixes = []string{
	"kafka_brokers",
	"kafka_topic_",
	"kafka_consumergroup_",
	"kafka_consumer_group_",
	"kafka_exporter_",
	"process_",
	"go_",
}

var allowedMetricLabels = map[string]bool{
	"broker": true, "broker_id": true, "node": true, "node_id": true,
	"topic": true, "partition": true, "group": true, "consumergroup": true,
	"client_id": true, "listener": true, "networkprocessor": true,
	"request": true, "api": true, "error": true, "quantile": true,
	"pool": true, "area": true, "id": true, "state": true,
	"role": true, "directory": true, "logdir": true,
}

func resolveMetricsURL(template string, cluster DiscoveredCluster) string {
	if template == "" {
		return ""
	}
	resolved := strings.ReplaceAll(template, "{host}", "127.0.0.1")
	return resolved
}

func metricAllowed(name string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func splitMetricToken(token string) (string, map[string]string, error) {
	open := strings.IndexByte(token, '{')
	if open < 0 {
		return token, nil, nil
	}
	if !strings.HasSuffix(token, "}") {
		return "", nil, fmt.Errorf("malformed label set")
	}
	name := token[:open]
	raw := token[open+1 : len(token)-1]
	labels := make(map[string]string)
	for len(strings.TrimSpace(raw)) > 0 {
		eq := strings.IndexByte(raw, '=')
		if eq <= 0 {
			return "", nil, fmt.Errorf("malformed label")
		}
		key := strings.TrimSpace(raw[:eq])
		raw = strings.TrimSpace(raw[eq+1:])
		if !strings.HasPrefix(raw, `"`) {
			return "", nil, fmt.Errorf("unquoted label")
		}
		var value strings.Builder
		escaped := false
		end := -1
		for i := 1; i < len(raw); i++ {
			ch := raw[i]
			if escaped {
				switch ch {
				case 'n':
					value.WriteByte('\n')
				case '\\', '"':
					value.WriteByte(ch)
				default:
					value.WriteByte(ch)
				}
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				end = i
				break
			}
			value.WriteByte(ch)
		}
		if end < 0 {
			return "", nil, fmt.Errorf("unterminated label")
		}
		if allowedMetricLabels[key] {
			labels[key] = value.String()
		}
		raw = strings.TrimSpace(raw[end+1:])
		if raw == "" {
			break
		}
		if raw[0] != ',' {
			return "", nil, fmt.Errorf("malformed label separator")
		}
		raw = raw[1:]
	}
	return name, labels, nil
}

func splitSampleLine(line string) (string, string, bool) {
	inLabels, inQuotes, escaped := false, false, false
	for i, ch := range line {
		switch {
		case escaped:
			escaped = false
		case inQuotes && ch == '\\':
			escaped = true
		case ch == '"':
			inQuotes = !inQuotes
		case !inQuotes && ch == '{':
			inLabels = true
		case !inQuotes && ch == '}':
			inLabels = false
		case !inLabels && (ch == ' ' || ch == '\t'):
			rest := strings.TrimSpace(line[i:])
			if rest == "" {
				return "", "", false
			}
			valueFields := strings.Fields(rest)
			if len(valueFields) == 0 {
				return "", "", false
			}
			return line[:i], valueFields[0], true
		}
	}
	return "", "", false
}

func parsePrometheus(reader io.Reader, prefixes []string) (prometheusScrape, error) {
	result := prometheusScrape{types: make(map[string]string)}
	familyCounts := make(map[string]int)
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 2<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "# HELP ") {
			continue
		}
		if strings.HasPrefix(line, "# TYPE ") {
			fields := strings.Fields(line)
			if len(fields) == 4 && metricAllowed(fields[2], prefixes) {
				result.types[fields[2]] = fields[3]
			}
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		token, rawValue, ok := splitSampleLine(line)
		if !ok {
			continue
		}
		name, labels, err := splitMetricToken(token)
		if err != nil || !metricAllowed(name, prefixes) {
			continue
		}
		value, err := strconv.ParseFloat(rawValue, 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			continue
		}
		family := name
		if strings.HasSuffix(family, "_created") {
			continue
		}
		if familyCounts[family] >= maxSamplesPerFamily {
			result.truncated = true
			continue
		}
		if len(result.samples) >= maxSamplesPerScrape {
			result.truncated = true
			continue
		}
		familyCounts[family]++
		result.samples = append(result.samples, MetricSampleDTO{
			Name: name, Labels: labels, Value: value, Type: result.types[name],
		})
	}
	if err := scanner.Err(); err != nil {
		return result, err
	}
	return result, nil
}

func scrapePrometheus(ctx context.Context, client *http.Client, endpoint string, prefixes []string) ([]MetricSampleDTO, CollectorStatusDTO) {
	status := CollectorStatusDTO{
		Configured: endpoint != "", Endpoint: endpoint,
		LastAttemptAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if endpoint == "" {
		return nil, status
	}
	started := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		status.Error = err.Error()
		return nil, status
	}
	req.Header.Set("Accept", "text/plain; version=0.0.4")
	resp, err := client.Do(req)
	status.ScrapeDurationMs = time.Since(started).Milliseconds()
	if err != nil {
		status.Error = err.Error()
		return nil, status
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		status.Error = fmt.Sprintf("HTTP %d", resp.StatusCode)
		return nil, status
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxMetricsResponseBytes+1))
	if err != nil {
		status.Error = err.Error()
		return nil, status
	}
	if len(body) > maxMetricsResponseBytes {
		status.Error = fmt.Sprintf("metrics response exceeds %d bytes", maxMetricsResponseBytes)
		return nil, status
	}
	parsed, err := parsePrometheus(bytes.NewReader(body), prefixes)
	if err != nil {
		status.Error = err.Error()
		return nil, status
	}
	status.Available = true
	status.LastSuccessAt = time.Now().UTC().Format(time.RFC3339Nano)
	status.SamplesCollected = len(parsed.samples)
	status.Truncated = parsed.truncated
	return parsed.samples, status
}

func aggregateBrokerCounters(samples []MetricSampleDTO) (messages, bytesIn float64, ok bool) {
	for _, sample := range samples {
		// Use only broker aggregate series. Per-topic series must not be added to
		// an aggregate series or traffic will be double-counted.
		if sample.Labels["topic"] != "" {
			continue
		}
		switch {
		case strings.HasPrefix(sample.Name, "kafka_server_brokertopicmetrics_messagesinpersec_count"),
			strings.HasPrefix(sample.Name, "kafka_server_brokertopicmetrics_messagesinpersec_total"),
			strings.HasPrefix(sample.Name, "kafka_server_brokertopicmetrics_messagesin_total"):
			messages += sample.Value
			ok = true
		case strings.HasPrefix(sample.Name, "kafka_server_brokertopicmetrics_bytesinpersec_count"),
			strings.HasPrefix(sample.Name, "kafka_server_brokertopicmetrics_bytesinpersec_total"),
			strings.HasPrefix(sample.Name, "kafka_server_brokertopicmetrics_bytesin_total"):
			bytesIn += sample.Value
			ok = true
		}
	}
	return messages, bytesIn, ok
}

func counterRates(state *jmxCounterState, now time.Time, messages, bytesIn float64) (float64, float64) {
	if !state.ready {
		state.messages, state.bytes, state.at, state.ready = messages, bytesIn, now, true
		return 0, 0
	}
	seconds := now.Sub(state.at).Seconds()
	if seconds <= 0 {
		return 0, 0
	}
	var messageRate, byteRate float64
	if messages >= state.messages {
		messageRate = (messages - state.messages) / seconds
	}
	if bytesIn >= state.bytes {
		byteRate = (bytesIn - state.bytes) / seconds
	}
	state.messages, state.bytes, state.at = messages, bytesIn, now
	return messageRate, byteRate
}

func publishMetrics(ctx context.Context, client *APIClient, cfg RuntimeConfig, store *ClusterStore, states map[string]*jmxCounterState, logger *slog.Logger) {
	clusters := store.Get()
	if len(clusters) == 0 {
		return
	}
	httpClient := &http.Client{Timeout: 8 * time.Second}
	seen := map[string]bool{}
	for _, cluster := range clusters {
		key := cluster.Name + "|" + cluster.BootstrapServers + "|" + cfg.HostID + "|" + strconv.Itoa(cluster.NodeID)
		if seen[key] {
			continue
		}
		seen[key] = true
		host := collectClusterMetrics(ctx, cluster)
		now := time.Now().UTC()
		metrics := ExternalBrokerMetricsDTO{
			Hostname: cfg.NodeName, Bootstrap: cluster.BootstrapServers,
			NodeID: cluster.NodeID, CollectedAt: now.Format(time.RFC3339Nano),
			CPUUsagePct: host.CPUUsagePct, MemoryUsedMB: host.MemoryUsedMB,
			MemoryTotalMB: host.MemoryTotalMB, DiskUsedGB: host.DiskUsedGB,
			DiskTotalGB: host.DiskTotalGB, DiskUsedBytes: host.DiskUsedBytes,
			DiskTotalBytes: host.DiskTotalBytes,
		}

		jmxEndpoint := resolveMetricsURL(jmxMetricsURLForCluster(cfg, cluster), cluster)
		metrics.JMXMetrics, metrics.JMXCollector = scrapePrometheus(ctx, httpClient, jmxEndpoint, jmxMetricPrefixes)
		if messages, bytesIn, ok := aggregateBrokerCounters(metrics.JMXMetrics); ok {
			stateKey := cfg.HostID + "|" + cluster.KafkaClusterID + "|" + strconv.Itoa(cluster.NodeID) + "|" + jmxEndpoint
			state := states[stateKey]
			if state == nil {
				state = &jmxCounterState{}
				states[stateKey] = state
			}
			metrics.MessagesInPerSec, metrics.BytesInPerSec = counterRates(state, now, messages, bytesIn)
		}

		exporterEndpoint := resolveMetricsURL(cfg.KafkaExporterMetricsURL, cluster)
		metrics.KafkaExporterMetrics, metrics.KafkaExporter = scrapePrometheus(ctx, httpClient, exporterEndpoint, kafkaExporterMetricPrefixes)

		if metrics.JMXCollector.Configured && !metrics.JMXCollector.Available {
			logger.Warn("JMX metrics scrape failed", "cluster", cluster.Name, "node_id", cluster.NodeID, "error", metrics.JMXCollector.Error)
		}
		if metrics.KafkaExporter.Configured && !metrics.KafkaExporter.Available {
			logger.Warn("Kafka Exporter scrape failed", "cluster", cluster.Name, "node_id", cluster.NodeID, "error", metrics.KafkaExporter.Error)
		}
		_, err := client.DoJSON(ctx, http.MethodPost, client.endpoint(externalAgentPath(cluster.Name, "/metrics")), nil, metrics, nil)
		if err != nil {
			logger.Warn("metrics publish failed", "cluster", cluster.Name, "node_id", cluster.NodeID, "error", err)
		}
	}
}

func runMetricsLoop(ctx context.Context, client *APIClient, cfg RuntimeConfig, store *ClusterStore, logger *slog.Logger) {
	ticker := time.NewTicker(cfg.MetricsInterval)
	defer ticker.Stop()
	states := make(map[string]*jmxCounterState)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			publishMetrics(ctx, client, cfg, store, states, logger)
		}
	}
}
