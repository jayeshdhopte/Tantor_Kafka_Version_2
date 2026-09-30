package main

import (
	"strings"
	"testing"
	"time"
)

func TestParsePrometheusFiltersAndPreservesControlledLabels(t *testing.T) {
	input := `
# TYPE kafka_server_brokertopicmetrics_messagesinpersec_total counter
kafka_server_brokertopicmetrics_messagesinpersec_total 100
kafka_server_brokertopicmetrics_messagesinpersec_total{topic="orders",broker="1",arbitrary="drop-me"} 40
unrelated_secret_metric{password="do-not-forward"} 1
`
	got, err := parsePrometheus(strings.NewReader(input), jmxMetricPrefixes)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.samples) != 2 {
		t.Fatalf("expected 2 allowed samples, got %d", len(got.samples))
	}
	if got.samples[1].Labels["topic"] != "orders" || got.samples[1].Labels["broker"] != "1" {
		t.Fatalf("expected controlled labels, got %#v", got.samples[1].Labels)
	}
	if _, exists := got.samples[1].Labels["arbitrary"]; exists {
		t.Fatal("arbitrary label must not be forwarded")
	}
}

func TestAggregateBrokerCountersDoesNotDoubleCountTopics(t *testing.T) {
	samples := []MetricSampleDTO{
		{Name: "kafka_server_brokertopicmetrics_messagesinpersec_total", Value: 100},
		{Name: "kafka_server_brokertopicmetrics_messagesinpersec_total", Labels: map[string]string{"topic": "orders"}, Value: 40},
		{Name: "kafka_server_brokertopicmetrics_bytesinpersec_total", Value: 1000},
	}
	messages, bytesIn, ok := aggregateBrokerCounters(samples)
	if !ok || messages != 100 || bytesIn != 1000 {
		t.Fatalf("unexpected aggregate messages=%v bytes=%v ok=%v", messages, bytesIn, ok)
	}
}

func TestAggregateBrokerCountersSupportsCurrentJMXExporterNames(t *testing.T) {
	samples := []MetricSampleDTO{
		{Name: "kafka_server_brokertopicmetrics_messagesin_total", Value: 240},
		{Name: "kafka_server_brokertopicmetrics_bytesin_total", Value: 4096},
	}
	messages, bytesIn, ok := aggregateBrokerCounters(samples)
	if !ok || messages != 240 || bytesIn != 4096 {
		t.Fatalf("unexpected aggregate messages=%v bytes=%v ok=%v", messages, bytesIn, ok)
	}
}

func TestCounterRatesHandlesReset(t *testing.T) {
	state := &jmxCounterState{}
	now := time.Unix(100, 0)
	counterRates(state, now, 100, 1000)
	messages, bytesIn := counterRates(state, now.Add(10*time.Second), 120, 1200)
	if messages != 2 || bytesIn != 20 {
		t.Fatalf("unexpected rates messages=%v bytes=%v", messages, bytesIn)
	}
	messages, bytesIn = counterRates(state, now.Add(20*time.Second), 5, 10)
	if messages != 0 || bytesIn != 0 {
		t.Fatalf("counter reset must produce zero rates, got %v %v", messages, bytesIn)
	}
}

func TestKafkaExporterFamilies(t *testing.T) {
	input := `
kafka_brokers 3
kafka_topic_partitions{topic="orders"} 12
kafka_consumergroup_lag{consumergroup="billing",topic="orders",partition="0"} 42
`
	got, err := parsePrometheus(strings.NewReader(input), kafkaExporterMetricPrefixes)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.samples) != 3 {
		t.Fatalf("expected 3 Kafka Exporter samples, got %d", len(got.samples))
	}
}

func TestResolveMetricsURLNeverUsesKafkaListenerPort(t *testing.T) {
	cluster := DiscoveredCluster{BootstrapServers: "broker1.example:9092"}
	got := resolveMetricsURL("http://{host}:9404/metrics", cluster)
	if got != "http://127.0.0.1:9404/metrics" {
		t.Fatalf("unexpected resolved endpoint %q", got)
	}
}
