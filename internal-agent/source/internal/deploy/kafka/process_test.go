package kafka

import (
	"context"
	"errors"
	"io.translab/tantor-agent/internal/executor"
	"os"
	"strings"
	"testing"
)

type pidExecutor struct {
	pid   string
	dead  bool
	calls []string
}

func (x *pidExecutor) Run(ctx context.Context, c string, a ...string) (string, string, error) {
	return x.RunSudo(ctx, c, a...)
}
func (x *pidExecutor) RunSudo(_ context.Context, c string, a ...string) (string, string, error) {
	x.calls = append(x.calls, c+" "+strings.Join(a, " "))
	if c == "systemctl" {
		return x.pid, "", nil
	}
	if c == "kill" && !x.dead {
		return "", "", nil
	}
	return "", "", errors.New("no process")
}
func TestServiceProcessPID(t *testing.T) {
	for _, tc := range []struct {
		pid      string
		dead, ok bool
	}{{"1234\n", false, true}, {"0", false, false}, {"1", false, false}, {"bad", false, false}, {"1234", true, false}} {
		x := &pidExecutor{pid: tc.pid, dead: tc.dead}
		d := &Deployer{exec: x}
		_, err := d.serviceProcessPID(context.Background(), "tantor-kafka-1")
		if (err == nil) != tc.ok {
			t.Fatalf("%+v: %v", tc, err)
		}
		if x.calls[0] != "systemctl show tantor-kafka-1 --property=MainPID --value" {
			t.Fatal(x.calls)
		}
	}
}
func TestLiveServiceProcessPID(t *testing.T) {
	unit := os.Getenv("TANTOR_TEST_LIVE_UNIT")
	if unit == "" {
		t.Skip("opt-in read-only live check")
	}
	d := &Deployer{exec: executor.New(executor.Options{PrivilegeMode: "direct"})}
	pid, err := d.serviceProcessPID(context.Background(), unit)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Detected %s PID %s", unit, pid)
}
