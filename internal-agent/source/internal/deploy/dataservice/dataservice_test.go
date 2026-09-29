package dataservice

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/pkg/api"
)

func taskFor() *api.Task {
	return &api.Task{ArtifactURL: "https://artifacts.invalid/bundle.tgz", Checksum: strings.Repeat("a", 64), Parameters: map[string]string{"bootstrap_servers": "broker:9092", "allow_deferred_kafka": "true"}}
}

func TestSafePathAcceptsVersionedJavaHome(t *testing.T) {
	if err := SafePath("/opt/java/jdk-17.0.20.1+1"); err != nil {
		t.Fatal(err)
	}
	if err := SafePath("/opt/java/jdk-17;touch /tmp/unsafe"); err == nil {
		t.Fatal("accepted shell punctuation in path")
	}
}

func TestRejectsPathsThatCouldChangeKafka(t *testing.T) {
	for _, dir := range []string{"/opt", "/data/kafka", "/var/log/kafka/subdir", "/etc", "/tmp/../opt", "/opt/evil\nExecStart=bad", "/opt/tantor/kafka-connect/share/data"} {
		task := taskFor()
		task.Parameters["working_dir"] = dir
		if _, err := Parse(Connect, task); err == nil {
			t.Errorf("accepted protected/invalid path %s", dir)
		}
	}
	task := taskFor()
	task.Parameters["install_dir"] = "/custom/kafka"
	task.Parameters["kafka_install_dir"] = "/custom/kafka/4.3.0"
	if _, err := Parse(Connect, task); err == nil {
		t.Fatal("accepted custom Kafka parent")
	}
}

func TestCustomConfigurationsAreRenderedWithoutKafkaServiceActions(t *testing.T) {
	for _, kind := range []string{Schema, Connect} {
		task := taskFor()
		task.Parameters["rest_port"] = "18083"
		task.Parameters["heap_size"] = "2G"
		task.Parameters["replication_factor"] = "2"
		task.Parameters["plugin_dir"] = "/custom/plugins"
		task.Parameters["working_dir"] = "/custom/working"
		task.Parameters["config_dir"] = "/custom/config"
		task.Parameters["compatibility_level"] = "FULL"
		task.Parameters["group_id"] = "workers-custom"
		s, err := Parse(kind, task)
		if err != nil {
			t.Fatal(err)
		}
		files := ConfigFiles(s, "/usr/lib/jvm/java-17")
		all := files[0].Content + files[2].Content
		for _, expected := range []string{"18083", "2G", "/custom/working", "/custom/config", "LimitNOFILE=1024000", "workers-custom"} {
			if !strings.Contains(all, expected) {
				t.Errorf("%s missing %s", kind, expected)
			}
		}
		for _, forbidden := range []string{"ExecStop=", "kafka-server-stop", "Requires=kafka", "Restart=always"} {
			if strings.Contains(all, forbidden) {
				t.Errorf("unsafe service action: %s", forbidden)
			}
		}
		if kind == Connect && (!strings.Contains(all, "plugin.path=/custom/plugins") || !strings.Contains(all, "config.storage.replication.factor=2")) {
			t.Fatal(all)
		}
		if kind == Schema && !strings.Contains(all, "schema.compatibility.level=FULL") {
			t.Fatal(all)
		}
	}
}

type fakeExec struct {
	fail     bool
	resolved string
	args     []string
}

func (f *fakeExec) Run(_ context.Context, command string, args ...string) (string, string, error) {
	if command == "readlink" {
		if f.resolved != "" && strings.Contains(args[len(args)-1], "kafka-connect") {
			return f.resolved, "", nil
		}
		return args[len(args)-1], "", nil
	}
	f.args = args
	if f.fail {
		return "[PASS] Java 17\n[FAIL] Plugin JARs missing", "", errors.New("exit 1")
	}
	return "[PASS] Host checks\n[DEFERRED] Kafka reachability", "", nil
}
func (f *fakeExec) RunSudo(context.Context, string, ...string) (string, string, error) {
	return "", "", nil
}

func TestPrecheckKeepsDetailedFailuresAndUsesArguments(t *testing.T) {
	task := taskFor()
	task.Parameters["working_dir"] = "/custom/work"
	task.Parameters["plugin_dir"] = "/custom/plugins"
	executor := &fakeExec{fail: true}
	logs, err := Precheck(context.Background(), executor, Connect, task)
	if err == nil || !strings.Contains(logs, "Plugin JARs missing") {
		t.Fatal(logs, err)
	}
	joined := strings.Join(executor.args[2:], " ")
	if !strings.Contains(joined, "/custom/work") || !strings.Contains(joined, "/custom/plugins") {
		t.Fatal(executor.args)
	}
	if strings.Contains(executor.args[1], "/custom/work") {
		t.Fatal("task data interpolated into shell program")
	}
}

func TestSymlinkIntoKafkaIsRejected(t *testing.T) {
	s, err := Parse(Connect, taskFor())
	if err != nil {
		t.Fatal(err)
	}
	if err = CheckPaths(context.Background(), &fakeExec{resolved: "/data/kafka"}, s); err == nil {
		t.Fatal("accepted symlink into Kafka data")
	}
}

func TestInstallRefusesToOverwriteActiveService(t *testing.T) {
	task := taskFor()
	task.Parameters["allow_deferred_kafka"] = "false"
	_, err := Deploy(context.Background(), &config.Config{}, nil, &fakeExec{}, Connect, task)
	if err == nil || !strings.Contains(err.Error(), "already active") {
		t.Fatal(err)
	}
}

func TestInstallationRejectsPreviewDeferral(t *testing.T) {
	_, err := Deploy(context.Background(), &config.Config{}, nil, nil, Connect, taskFor())
	if err == nil || !strings.Contains(err.Error(), "cannot defer") {
		t.Fatal(err)
	}
}

func makeArchive(t *testing.T, extra []*tar.Header) string {
	t.Helper()
	name := filepath.Join(t.TempDir(), "bundle.tgz")
	f, _ := os.Create(name)
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	headers := []*tar.Header{{Name: "bundle", Typeflag: tar.TypeDir, Mode: 0755}, {Name: "bundle/bin/connect-distributed.sh", Typeflag: tar.TypeReg, Mode: 0755}, {Name: "bundle/libs/connect-runtime-4.3.0.jar", Typeflag: tar.TypeReg, Mode: 0644}}
	for _, h := range append(headers, extra...) {
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
	}
	tw.Close()
	gz.Close()
	f.Close()
	return name
}

func TestArchiveRejectsEscapesAndMissingLibraries(t *testing.T) {
	for _, h := range []*tar.Header{
		{Name: "bundle/../../kafka", Typeflag: tar.TypeReg},
		{Name: "bundle/escape", Typeflag: tar.TypeSymlink, Linkname: "/data/kafka"},
		{Name: "bundle/link", Typeflag: tar.TypeSymlink, Linkname: "missing.jar"},
	} {
		if _, err := InspectArchive(makeArchive(t, []*tar.Header{h}), Connect); err == nil {
			t.Errorf("accepted %s", h.Name)
		}
	}
	if _, err := InspectArchive(makeArchive(t, nil), Schema); err == nil {
		t.Fatal("accepted Kafka-only artifact for Schema Registry")
	}
}

func TestArchiveAcceptsInternalLibraryLinks(t *testing.T) {
	name := makeArchive(t, []*tar.Header{{Name: "bundle/libs/alias.jar", Typeflag: tar.TypeSymlink, Linkname: "connect-runtime-4.3.0.jar"}})
	if root, err := InspectArchive(name, Connect); err != nil || root != "bundle" {
		t.Fatal(root, err)
	}
}

func TestSuppliedArtifact(t *testing.T) {
	name := os.Getenv("TANTOR_TEST_ARTIFACT")
	if name == "" {
		t.Skip("set TANTOR_TEST_ARTIFACT for actual bundle validation")
	}
	for _, kind := range []string{Schema, Connect} {
		if _, err := InspectArchive(name, kind); err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
	}
}

func TestRESTVerificationRequiresActualServiceJSON(t *testing.T) {
	for _, kind := range []string{Schema, Connect} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/" {
				fmt.Fprint(w, `{"version":"4.3.0"}`)
			} else {
				fmt.Fprint(w, `[]`)
			}
		}))
		if _, err := Verify(context.Background(), kind, server.URL); err != nil {
			t.Fatal(err)
		}
		server.Close()
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "<html>proxy login</html>") }))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := Verify(ctx, Connect, server.URL); err == nil {
		t.Fatal("accepted HTML as Connect API")
	}
}

func TestEmbeddedShellChecks(t *testing.T) {
	bash := os.Getenv("TANTOR_TEST_BASH")
	if bash == "" {
		var err error
		bash, err = osexec.LookPath("bash")
		if err != nil {
			t.Skip("bash unavailable")
		}
	}
	fixture := `ulimit(){ echo 1024000; }
java(){ echo 'openjdk version "17.0.9"' >&2; }
systemctl(){ return 0; }
ss(){ return 0; }
getent(){ echo 127.0.0.1; }
find(){ echo /plugin/example.jar; }
df(){ printf 'Filesystem blocks used free capacity mount\n/dev/test 10000 1000 9000 10%% /\n'; }
timeout(){ echo probe-executed; return 0; }
`
	for _, tc := range []struct {
		name, override string
		deferred, fail bool
	}{
		{"preview", "", true, false}, {"live", "", false, false},
		{"java", `java(){ echo 'openjdk version "21.0.1"' >&2; }`, true, true},
		{"limits", `ulimit(){ echo 1024; }`, true, true},
		{"ntp", `systemctl(){ return 1; }`, true, true},
		{"plugins", `find(){ return 0; }`, true, true},
		{"port", `ss(){ echo 'LISTEN 0 128 0.0.0.0:8083 0.0.0.0:*'; }`, true, true},
		{"disk", `df(){ printf 'header\n/dev/test 100 20 80 20%% /\n'; }`, true, true},
		{"ssmissing", `ss(){ return 127; }`, true, true},
		{"ip", `getent(){ return 1; }`, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bootstrap := "broker:9092"
			if tc.name == "ip" {
				bootstrap = "192.0.2.1:9092"
			}
			cmd := osexec.Command(bash, "-c", fixture+"\n"+tc.override+"\n"+precheckShell(), "--", Connect, "8083", bootstrap, "/missing", "/", "/", "5120", fmt.Sprint(tc.deferred), "", "connect-configs")
			out, err := cmd.CombinedOutput()
			if (err != nil) != tc.fail {
				t.Fatalf("err=%v output=%s", err, out)
			}
			if tc.fail && !strings.Contains(string(out), "[FAIL]") {
				t.Fatalf("Bash did not reach the expected failing check: %s", out)
			}
			if tc.deferred && strings.Contains(string(out), "probe-executed") {
				t.Fatal("Preview probed brokers")
			}
			if !tc.deferred && !strings.Contains(string(out), "Reachability: broker:9092") {
				t.Fatal(string(out))
			}
		})
	}
}
