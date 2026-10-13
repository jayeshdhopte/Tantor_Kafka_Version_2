package dataservice

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"io.translab/tantor-agent/internal/config"
)

func TestSharedConnectDefaultsAndProtectedOverrides(t *testing.T) {
	task := taskFor()
	delete(task.Parameters, "install_dir")
	task.Parameters["kafka_install_dir"] = "/custom/kafka"
	s, err := Parse(Connect, task)
	if err != nil {
		t.Fatal(err)
	}
	if !s.SharedConnect || s.Install != "/custom/kafka" || s.Config != "/custom/kafka/config" || s.Plugin != "/custom/kafka/connect-plugins" {
		t.Fatalf("unexpected defaults: %+v", s)
	}
	for _, key := range []string{"log_dir", "working_dir", "config_dir", "plugin_dir"} {
		task.Parameters[key] = "/custom/kafka/libs"
		if _, err := Parse(Connect, task); err == nil {
			t.Fatalf("accepted runtime override for %s", key)
		}
		delete(task.Parameters, key)
	}
	task.Parameters["working_dir"] = "/data/kafka"
	if _, err := Parse(Connect, task); err == nil {
		t.Fatal("accepted Kafka storage")
	}
}

func TestSharedConnectPreservesPropertiesAndReplacesDuplicateManagedKeys(t *testing.T) {
	current := "# custom settings\nbootstrap.servers: old:9092\nbootstrap.servers=duplicate:9092\ncustom.setting=keep\nplugin.path=/old/plugins\n"
	got, err := mergeConnectProperties(current, "bootstrap.servers=new:9092\nplugin.path=/opt/kafka/connect-plugins\nlisteners=http://0.0.0.0:8083\n")
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"# custom settings", "custom.setting=keep", "plugin.path=/opt/kafka/connect-plugins", "listeners=http://0.0.0.0:8083"} {
		if !strings.Contains(got, v) {
			t.Fatal(got)
		}
	}
	if strings.Count(got, "bootstrap.servers=") != 1 || strings.Contains(got, "old:9092") {
		t.Fatal(got)
	}
	if _, err := mergeConnectProperties("bootstrap.servers=old,\\\nnext:9092\n", "bootstrap.servers=new:9092\n"); err == nil {
		t.Fatal("accepted ambiguous managed continuation")
	}
}

func TestSharedConnectCleanupPreservesEveryDirectory(t *testing.T) {
	task := taskFor()
	delete(task.Parameters, "install_dir")
	s, err := Parse(Connect, task)
	if err != nil {
		t.Fatal(err)
	}
	exec := &cleanupExecutor{unit: ConfigFiles(s, "/usr")[2].Content}
	if _, err := Clean(context.Background(), exec, Connect, task); err != nil {
		t.Fatal(err)
	}
	for _, cmd := range exec.commands {
		if strings.Contains(cmd, "rm -rf") {
			t.Fatalf("shared directory removed: %s", cmd)
		}
	}
}

type sharedExecutor struct {
	commands       []string
	redirected     bool
	failStart      bool
	current        string
	logToRoot      bool
	rootToData     bool
	missingRuntime bool
}

func (e *sharedExecutor) Run(_ context.Context, cmd string, args ...string) (string, string, error) {
	e.commands = append(e.commands, cmd+" "+strings.Join(args, " "))
	if cmd == "readlink" {
		p := args[len(args)-1]
		if e.logToRoot && p == "/var/log/tantor/kafka-connect" {
			return "/opt/kafka_2.13-4.3.0", "", nil
		}
		if e.rootToData && strings.HasPrefix(p, "/opt/kafka") {
			return strings.Replace(p, "/opt/kafka", "/data/kafka", 1), "", nil
		}
		if e.redirected && (p == "/opt/kafka/config" || p == "/opt/kafka/etc/schema-registry") {
			return "/data/kafka", "", nil
		}
		return strings.Replace(p, "/opt/kafka", "/opt/kafka_2.13-4.3.0", 1), "", nil
	}
	if cmd == "systemctl" || cmd == "pgrep" {
		return "", "", errors.New("not active")
	}
	if cmd == "bash" && len(args) > 1 && strings.Contains(args[1], "command -v java") {
		return "/usr/bin/java", "", nil
	}
	return "[PASS] host checks", "", nil
}
func (e *sharedExecutor) RunSudo(_ context.Context, cmd string, args ...string) (string, string, error) {
	e.commands = append(e.commands, cmd+" "+strings.Join(args, " "))
	if e.missingRuntime && cmd == "bash" && (strings.Contains(args[1], "connect-distributed.sh") || strings.Contains(args[1], "schema-registry-start")) {
		return "", "missing runtime", errors.New("missing runtime")
	}
	if cmd == "test" && args[0] == "-e" {
		return "", "", errors.New("not installed")
	}
	if cmd == "cat" {
		return e.current, "", nil
	}
	if cmd == "mktemp" {
		return strings.Replace(args[0], "XXXXXX", "ABCDEF", 1), "", nil
	}
	if e.failStart && cmd == "systemctl" && args[0] == "start" {
		return "", "start failed", errors.New("start failed")
	}
	return "", "", nil
}

func TestSharedConnectRejectsConfigSymlinkIntoStorage(t *testing.T) {
	task := taskFor()
	delete(task.Parameters, "install_dir")
	s, err := Parse(Connect, task)
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckPaths(context.Background(), &sharedExecutor{redirected: true}, s); err == nil {
		t.Fatal("accepted redirected config")
	}
	if err := CheckPaths(context.Background(), &sharedExecutor{}, s); err != nil {
		t.Fatal("rejected versioned Kafka symlink", err)
	}
	for _, e := range []*sharedExecutor{{logToRoot: true}, {rootToData: true}} {
		if err := CheckPaths(context.Background(), e, s); err == nil {
			t.Fatal("accepted redirected log or runtime into Kafka storage")
		}
	}
}

func TestSharedConnectMissingRuntimeFailsBeforeMutation(t *testing.T) {
	task := taskFor()
	delete(task.Parameters, "install_dir")
	task.Parameters["allow_deferred_kafka"] = "false"
	e := &sharedExecutor{missingRuntime: true}
	if _, err := Deploy(context.Background(), &config.Config{}, nil, e, Connect, task); err == nil {
		t.Fatal("missing installation accepted")
	}
	for _, cmd := range e.commands {
		if strings.HasPrefix(cmd, "mkdir ") || strings.HasPrefix(cmd, "cp ") || strings.HasPrefix(cmd, "install ") {
			t.Fatal("mutated before validation", cmd)
		}
	}
}

func TestSharedConnectInstallPreservesRuntimeAndRollsBackFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Write([]byte(`{"version":"4.3.0"}`))
		} else {
			w.Write([]byte(`[]`))
		}
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	for _, fail := range []bool{false, true} {
		task := taskFor()
		delete(task.Parameters, "install_dir")
		task.Parameters["allow_deferred_kafka"] = "false"
		task.Parameters["rest_port"] = u.Port()
		e := &sharedExecutor{failStart: fail, current: "# existing\ncustom.setting=keep\nbootstrap.servers=old:9092\n"}
		logs, err := Deploy(context.Background(), &config.Config{}, nil, e, Connect, task)
		if fail && (err == nil || !strings.Contains(err.Error(), "rollback")) {
			t.Fatal(logs, err)
		}
		if !fail && err != nil {
			t.Fatal(logs, err)
		}
		cmds := strings.Join(e.commands, "\n")
		for _, bad := range []string{"tar -x", "cp -a", "chown -R", "rm -rf", "kafka-server-stop"} {
			if strings.Contains(cmds, bad) {
				t.Fatal(cmds)
			}
		}
		if fail && !strings.Contains(cmds, "systemctl stop kafka-connect.service") {
			t.Fatal("missing rollback", cmds)
		}
	}
}
