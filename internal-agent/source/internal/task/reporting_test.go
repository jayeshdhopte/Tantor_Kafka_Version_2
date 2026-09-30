package task

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"io.translab/tantor-agent/internal/client"
	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/internal/deploy"
	"io.translab/tantor-agent/pkg/api"
)

type checkExecutor struct {
	calls int
	fail  bool
}

func (x *checkExecutor) Run(_ context.Context, _ string, _ ...string) (string, string, error) {
	x.calls++
	if x.fail {
		return "unavailable", "", errors.New("check failed")
	}
	return "17.0.15", "", nil
}
func (x *checkExecutor) RunSudo(ctx context.Context, cmd string, args ...string) (string, string, error) {
	return x.Run(ctx, cmd, args...)
}

func TestCheckReportsRetainClaimAndRefreshItOnRedelivery(t *testing.T) {
	for _, tc := range []struct {
		command string
		fail    bool
	}{
		{"CHECK_PORTS", false}, {"CHECK_PORTS", true},
		{"CHECK_PREREQUISITES", false}, {"CHECK_PREREQUISITES", true},
	} {
		t.Run(tc.command+map[bool]string{true: "-failure", false: "-success"}[tc.fail], func(t *testing.T) {
			var reports []api.TaskResult
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v1/agents/tasks/result" {
					http.NotFound(w, r)
					return
				}
				var report api.TaskResult
				if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				reports = append(reports, report)
				w.WriteHeader(http.StatusNoContent)
			}))
			defer srv.Close()
			cfg := &config.Config{}
			cfg.Agent.ServerURL = srv.URL
			cfg.Agent.HostID = "host-test"
			cfg.Auth.Mode = "none"
			c, err := client.NewAPIClient(cfg)
			if err != nil {
				t.Fatal(err)
			}
			x := &checkExecutor{fail: tc.fail}
			engine := NewEngine(cfg, c, nil, deploy.NewEngine(cfg, c, x))
			listener, err := net.Listen("tcp", ":0")
			if err != nil {
				t.Fatal(err)
			}
			_, port, _ := net.SplitHostPort(listener.Addr().String())
			defer listener.Close()
			if !tc.fail {
				listener.Close()
			}
			payload, _ := json.Marshal(map[string]interface{}{"task_id": "task-test", "claim_token": "claim-first", "command": tc.command, "parameters": map[string]string{"required_ports": port}})
			var task api.Task
			if err := json.Unmarshal(payload, &task); err != nil {
				t.Fatal(err)
			}
			engine.executeTask(context.Background(), task)
			if len(reports) != 2 {
				t.Fatalf("reports=%d, want running and final", len(reports))
			}
			want := "SUCCESS"
			if tc.fail {
				want = "FAILED"
			}
			if reports[0].Status != "RUNNING" || reports[1].Status != want || strings.TrimSpace(reports[1].LogOutput) == "" {
				t.Fatalf("unexpected reports: %+v", reports)
			}
			for _, r := range reports {
				if r.ClaimToken != "claim-first" {
					t.Fatalf("missing claim: %+v", r)
				}
			}
			calls := x.calls
			task.ClaimToken = "claim-redelivered"
			engine.executeTask(context.Background(), task)
			if len(reports) != 3 || reports[2].ClaimToken != "claim-redelivered" || reports[2].Status != want || reports[2].LogOutput != reports[1].LogOutput {
				t.Fatalf("redelivery: %+v", reports)
			}
			if x.calls != calls {
				t.Fatal("cached task executed again")
			}
		})
	}
}
