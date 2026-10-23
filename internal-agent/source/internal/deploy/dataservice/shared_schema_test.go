package dataservice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"io.translab/tantor-agent/internal/config"
)

func TestSharedSchemaDefaultsAndProtection(t *testing.T) {
	task := taskFor()
	delete(task.Parameters, "install_dir")
	task.Parameters["other_install_dir"] = "/opt/kafka"
	task.Parameters["other_config_dir"] = "/opt/kafka/config"
	task.Parameters["other_plugin_dir"] = "/opt/kafka/connect-plugins"
	s, err := Parse(Schema, task)
	if err != nil {
		t.Fatal(err)
	}
	if !s.SharedSchema || s.Config != "/opt/kafka/etc/schema-registry" {
		t.Fatalf("unexpected defaults: %+v", s)
	}
	if err := CheckPaths(context.Background(), &sharedExecutor{}, s); err != nil {
		t.Fatal(err)
	}
	if err := CheckPaths(context.Background(), &sharedExecutor{redirected: true}, s); err == nil {
		t.Fatal("accepted config redirected into Kafka storage")
	}
	for _, key := range []string{"config_dir", "log_dir", "working_dir"} {
		task.Parameters[key] = "/opt/kafka/libs"
		if _, err := Parse(Schema, task); err == nil {
			t.Fatalf("accepted protected override %s", key)
		}
		delete(task.Parameters, key)
	}
	files := ConfigFiles(s, "/usr")
	if !strings.Contains(files[2].Content, "ExecStart=/opt/kafka/bin/schema-registry-start /opt/kafka/etc/schema-registry/schema-registry.properties") {
		t.Fatal(files[2].Content)
	}
	if files[1].Path != "/opt/kafka/etc/schema-registry/tantor-schema-registry-log4j2.xml" {
		t.Fatal(files[1].Path)
	}
}

func TestSharedSchemaInstallAndRollbackPreserveKafkaAndConnect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`[]`)) }))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	for _, fail := range []bool{false, true} {
		task := taskFor()
		delete(task.Parameters, "install_dir")
		task.Parameters["allow_deferred_kafka"] = "false"
		task.Parameters["rest_port"] = u.Port()
		e := &sharedExecutor{failStart: fail, current: "# preserve\ncustom.setting=keep\n"}
		logs, err := Deploy(context.Background(), &config.Config{}, nil, e, Schema, task)
		if fail && (err == nil || !strings.Contains(err.Error(), "rollback")) {
			t.Fatal(logs, err)
		}
		if !fail && err != nil {
			t.Fatal(logs, err)
		}
		commands := strings.Join(e.commands, "\n")
		for _, bad := range []string{"tar -x", "chown -R", "rm -rf", "systemctl start kafka-connect.service", "systemctl stop kafka-connect.service", "systemctl disable kafka-connect.service"} {
			if strings.Contains(commands, bad) {
				t.Fatalf("unexpected mutation %s: %s", bad, commands)
			}
		}
		if !strings.Contains(commands, "schema-registry.properties.tantor-backup.") {
			t.Fatal("missing config backup", commands)
		}
		if fail && !strings.Contains(commands, "systemctl stop schema-registry.service") {
			t.Fatal("missing schema rollback", commands)
		}
		if !fail && !strings.Contains(commands, "-mtime +6 -delete") {
			t.Fatal("missing retention", commands)
		}
	}
}

func TestSharedSchemaMissingRuntimeAndCleanup(t *testing.T) {
	task := taskFor()
	delete(task.Parameters, "install_dir")
	task.Parameters["allow_deferred_kafka"] = "false"
	e := &sharedExecutor{missingRuntime: true}
	if _, err := Deploy(context.Background(), &config.Config{}, nil, e, Schema, task); err == nil {
		t.Fatal("accepted missing runtime")
	}
	for _, cmd := range e.commands {
		if strings.HasPrefix(cmd, "mkdir ") || strings.HasPrefix(cmd, "cp ") || strings.HasPrefix(cmd, "install ") {
			t.Fatal("mutated before validation", cmd)
		}
	}
	s, err := Parse(Schema, task)
	if err != nil {
		t.Fatal(err)
	}
	cleanup := &cleanupExecutor{unit: ConfigFiles(s, "/usr")[2].Content}
	if _, err := Clean(context.Background(), cleanup, Schema, task); err != nil {
		t.Fatal(err)
	}
	for _, cmd := range cleanup.commands {
		if strings.HasPrefix(cmd, "rm -rf") && cmd != "rm -rf -- "+s.Log && cmd != "rm -rf -- "+s.Working && !strings.HasSuffix(cmd, ".service.d") {
			t.Fatal("shared directory removed", cmd)
		}
	}
}
