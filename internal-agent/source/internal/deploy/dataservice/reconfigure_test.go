package dataservice

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

type portExec struct {
	fakeExec
	s           Settings
	current     string
	restarts    int
	restored    bool
	failRestart bool
	mutations   []string
}

func (f *portExec) Run(ctx context.Context, command string, args ...string) (string, string, error) {
	if command == "systemctl" && args[0] == "show" {
		launcher := "schema-registry-start"
		if f.s.Kind == Connect {
			launcher = "connect-distributed.sh"
		}
		return fmt.Sprintf("{ argv[]=%s/bin/%s %s ; }", f.s.Install, launcher, ConfigFiles(f.s, "/usr")[0].Path), "", nil
	}
	return f.fakeExec.Run(ctx, command, args...)
}

func (f *portExec) RunSudo(_ context.Context, command string, args ...string) (string, string, error) {
	if command == "cat" {
		return f.current, "", nil
	}
	if command == "mktemp" {
		return strings.TrimSuffix(args[0], "XXXXXX") + "123456", "", nil
	}
	f.mutations = append(f.mutations, command+" "+strings.Join(args, " "))
	if command == "cp" && strings.Contains(args[2], ".port-backup.") {
		f.restored = true
	}
	if command == "systemctl" && args[0] == "restart" {
		f.restarts++
		if f.failRestart && f.restarts == 1 {
			return "", "start failure", errors.New("restart failed")
		}
	}
	return "", "", nil
}

func TestExistingPortUpdateAndRollback(t *testing.T) {
	for _, kind := range []string{Schema, Connect} {
		for _, fail := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fail=%v", kind, fail), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/" {
						fmt.Fprint(w, `{"version":"test"}`)
					} else {
						fmt.Fprint(w, "[]")
					}
				}))
				defer server.Close()
				u, _ := url.Parse(server.URL)
				task := taskFor()
				task.Parameters["allow_deferred_kafka"] = "false"
				s, _ := Parse(kind, task)
				current := ConfigFiles(s, "/usr")[0].Content
				s.Port, _ = strconv.Atoi(u.Port())
				task.Parameters["rest_port"] = u.Port()
				exec := &portExec{s: s, current: current, failRestart: fail}
				logs, err := reconfigurePorts(context.Background(), exec, s, task)
				if fail {
					if err == nil || !exec.restored || exec.restarts != 2 {
						t.Fatalf("rollback missing: %v %+v", err, exec)
					}
				} else if err != nil || exec.restored || exec.restarts != 1 || !strings.Contains(logs, "REST port updated") {
					t.Fatalf("update failed: %v %s", err, logs)
				}
				for _, action := range exec.mutations {
					if strings.Contains(action, "restart broker") || strings.Contains(action, "restart controller") || strings.Contains(action, "tar ") {
						t.Fatal("unrelated mutation", action)
					}
				}
			})
		}
	}
}

func TestPortUpdatePreservesIdentityAndComments(t *testing.T) {
	for _, kind := range []string{Schema, Connect} {
		t.Run(kind, func(t *testing.T) {
			task := taskFor()
			s, err := Parse(kind, task)
			if err != nil {
				t.Fatal(err)
			}
			current := "# Retain this operator comment\n" + ConfigFiles(s, "/usr")[0].Content
			s.Port = 18090
			want := ConfigFiles(s, "/usr")[0].Content
			got, err := portConfiguration(current, want)
			if err != nil {
				t.Fatal(err)
			}
			if got != "# Retain this operator comment\n"+want {
				t.Fatalf("unexpected update: %s", got)
			}
			if _, err = portConfiguration(got, want); err != nil {
				t.Fatal("retry rejected", err)
			}
		})
	}
}

func TestPortUpdateRejectsNonPortChanges(t *testing.T) {
	s, _ := Parse(Connect, taskFor())
	current := ConfigFiles(s, "/usr")[0].Content
	for _, changed := range []string{
		strings.Replace(current, "bootstrap.servers=broker:9092", "bootstrap.servers=other:9092", 1),
		strings.Replace(current, "group.id=tantor-kafka-connect", "group.id=other", 1),
		current + "\nsecurity.protocol=SASL_SSL\n",
		current + "\nlisteners=http://0.0.0.0:9999\n",
		strings.Replace(current, "http://0.0.0.0:", "https://0.0.0.0:", 1),
	} {
		if _, err := portConfiguration(changed, current); err == nil {
			t.Fatalf("accepted incompatible existing configuration: %s", changed)
		}
	}
}
