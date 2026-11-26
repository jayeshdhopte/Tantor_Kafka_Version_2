package javaupgrade

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io.translab/tantor-agent/pkg/api"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// No systemctl, Java executable, or Java symlink is touched by this executor.
// The only real files are a tiny archive in a newly-created test directory.
type separateExecutor struct {
	work, current, pending string
	archive                []byte
	active                 map[string]bool
	calls                  []string
	failStart, unrelated   bool
}

func (f *separateExecutor) Run(_ context.Context, cmd string, args ...string) (string, string, error) {
	if cmd == "/usr/bin/java" {
		return "openjdk 17.0.21", "", nil
	}
	if strings.HasSuffix(cmd, "/bin/java") {
		return "openjdk 21.0.13", "", nil
	}
	if strings.HasSuffix(cmd, "kafka-topics.sh") {
		return "", "", nil
	}
	if strings.HasSuffix(cmd, "kafka-metadata-quorum.sh") {
		return "LeaderId: 1\nCurrentVoters: [1]", "", nil
	}
	return "", "", fmt.Errorf("unexpected command %s", cmd)
}
func (f *separateExecutor) RunSudo(_ context.Context, cmd string, args ...string) (string, string, error) {
	switch cmd {
	case "systemctl":
		unit := args[len(args)-1]
		if args[0] == "show" {
			switch args[1] {
			case "--property=LoadState":
				return "loaded", "", nil
			case "--property=ActiveState":
				if f.active[unit] {
					return "active", "", nil
				}
				return "inactive", "", nil
			case "--property=MainPID":
				if !f.active[unit] {
					return "0", "", nil
				}
				if unit == "broker.service" {
					return "101", "", nil
				}
				return "102", "", nil
			}
		}
		f.calls = append(f.calls, args[0]+" "+unit)
		if args[0] == "start" && unit == "broker.service" && f.failStart && f.current != "/old/bin/java" {
			f.failStart = false
			return "", "", fmt.Errorf("injected target start failure")
		}
		f.active[unit] = args[0] == "start"
		return "", "", nil
	case "ps":
		text := ""
		if f.active["broker.service"] {
			text += "101 java\n"
		}
		if f.active["controller.service"] {
			text += "102 java\n"
		}
		if f.active["kafka.service"] {
			text += "102 java\n"
		}
		if f.unrelated {
			text += "1734 java\n"
		}
		return text, "", nil
	case "readlink":
		return f.current, "", nil
	case "mktemp":
		if strings.HasPrefix(args[len(args)-1], "/usr/bin/") {
			return "/usr/bin/.tantor-java-TEST", "", nil
		}
		return f.work, "", nil
	case "install":
		return "", "", os.WriteFile(args[len(args)-1], f.archive, 0600)
	case "sha256sum":
		return fmt.Sprintf("%x  jdk.tar.gz", sha256.Sum256(f.archive)), "", nil
	case "ln":
		f.pending = args[len(args)-2]
		return "", "", nil
	case "mv":
		f.current = f.pending
		return "", "", nil
	case "chmod", "mkdir", "tar", "rm":
		return "", "", nil
	}
	return "", "", fmt.Errorf("unexpected privileged command %s %v", cmd, args)
}

func TestControllerPrecheckFailureDoesNotStopServicesOrSwitchJava(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux task")
	}
	f := &separateExecutor{current: "/old/bin/java", active: map[string]bool{"broker.service": true, "controller.service": true}}
	encoded, _ := json.Marshal(pair())
	task := &api.Task{Checksum: strings.Repeat("a", 64), Parameters: map[string]string{
		"artifact_id": "00000000-0000-4000-8000-000000000001", "target_version": "21.0.13", "parcel_dir": "/srv/tantor/parcels", "service_name": "broker", "separate_services": string(encoded),
	}}
	var logs strings.Builder
	err := Run(context.Background(), failingControllerProbe{f}, task, &logs)
	if err == nil || !strings.Contains(err.Error(), "pre-upgrade health for controller.service") || !strings.Contains(err.Error(), "Connection refused") {
		t.Fatalf("unexpected failure: %v", err)
	}
	if len(f.calls) != 0 || f.current != "/old/bin/java" || !f.active["broker.service"] || !f.active["controller.service"] {
		t.Fatal("precheck failure mutated services or Java")
	}
}

func TestSeparateUpgradeExecutionAndRecovery(t *testing.T) {
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		t.Skip("Linux isolated archive fixture requires root")
	}
	for _, tc := range []struct {
		name                        string
		controller, fail, unrelated bool
	}{
		{"both services", true, false, false}, {"controller originally stopped", false, false, false},
		{"target startup failure restores both", true, true, false}, {"Elasticsearch remains blocked", true, false, true},
		{"combined service regression", false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			work, err := os.MkdirTemp("/usr/lib/jvm", ".tantor-java-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(work) })
			var buffer bytes.Buffer
			gz := gzip.NewWriter(&buffer)
			tw := tar.NewWriter(gz)
			arch := map[string]string{"amd64": "x86_64", "arm64": "aarch64"}[runtime.GOARCH]
			for _, file := range []struct {
				name, body string
				mode       int64
			}{
				{"jdk/release", fmt.Sprintf("JAVA_VERSION=\"21.0.13\"\nOS_NAME=\"Linux\"\nOS_ARCH=\"%s\"\n", arch), 0644},
				{"jdk/bin/java", "fixture", 0755},
			} {
				if err := tw.WriteHeader(&tar.Header{Name: file.name, Mode: file.mode, Size: int64(len(file.body)), Typeflag: tar.TypeReg}); err != nil {
					t.Fatal(err)
				}
				if _, err := tw.Write([]byte(file.body)); err != nil {
					t.Fatal(err)
				}
			}
			_ = tw.Close()
			_ = gz.Close()
			f := &separateExecutor{work: work, current: "/old/bin/java", archive: buffer.Bytes(), active: map[string]bool{"broker.service": true, "controller.service": tc.controller}, failStart: tc.fail, unrelated: tc.unrelated}
			encoded, _ := json.Marshal(pair())
			task := &api.Task{Checksum: fmt.Sprintf("%x", sha256.Sum256(f.archive)), Parameters: map[string]string{
				"artifact_id": "00000000-0000-4000-8000-000000000001", "target_version": "21.0.13", "parcel_dir": "/srv/tantor/parcels", "service_name": "broker", "service_role": "broker", "kafka_home": "/opt/kafka", "health_endpoint": "127.0.0.1:9092", "separate_services": string(encoded),
			}}
			combined := tc.name == "combined service regression"
			if combined {
				f.active = map[string]bool{"kafka.service": true}
				delete(task.Parameters, "separate_services")
				task.Parameters["service_name"] = "kafka"
				task.Parameters["service_role"] = "broker_controller"
			}
			var log strings.Builder
			err = Run(context.Background(), f, task, &log)
			if tc.unrelated {
				if err == nil || !strings.Contains(err.Error(), "1734") || len(f.calls) != 0 {
					t.Fatalf("unrelated process bypassed: %v %v", err, f.calls)
				}
				return
			}
			if tc.fail {
				if err == nil || f.current != "/old/bin/java" || !strings.Contains(log.String(), "Restored previous") {
					t.Fatalf("failed recovery: %v %s %v", err, log.String(), f.calls)
				}
			} else if err != nil || f.current != filepath.Join(work, "jdk", "bin", "java") {
				t.Fatalf("upgrade failed: %v %s", err, log.String())
			}
			if combined && !f.active["kafka.service"] || !combined && (!f.active["broker.service"] || f.active["controller.service"] != tc.controller) {
				t.Fatal("original service states were not preserved")
			}
			if !tc.fail {
				want := []string{"stop broker.service", "start broker.service"}
				if combined {
					want = []string{"stop kafka.service", "start kafka.service"}
				}
				if tc.controller {
					want = []string{"stop broker.service", "stop controller.service", "start controller.service", "start broker.service"}
				}
				if !reflect.DeepEqual(f.calls, want) {
					t.Fatalf("wrong service order: %v", f.calls)
				}
			}
		})
	}
}
