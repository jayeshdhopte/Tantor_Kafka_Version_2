package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"

	"io.translab/tantor-agent/pkg/api"
)

type serviceStateExecutor struct {
	state string
	err   error
}

func (e serviceStateExecutor) RunSudo(_ context.Context, command string, args ...string) (string, string, error) {
	if command != "systemctl" || strings.Join(args, " ") != "show -p ActiveState --value controller.service" {
		return "", "", errors.New("unexpected command")
	}
	return e.state, "", e.err
}

func (e serviceStateExecutor) Run(context.Context, string, ...string) (string, string, error) {
	return "", "", errors.New("unexpected unprivileged command")
}

func TestDeployRefusesActiveControllerService(t *testing.T) {
	task := &api.Task{Parameters: map[string]string{"service_role": "controller", "node_id": "101"}}
	for _, tc := range []struct {
		state   string
		blocked bool
	}{
		{"active", true},
		{"activating", true},
		{"inactive", false},
		{"failed", false},
		{"", true},
	} {
		t.Run(tc.state, func(t *testing.T) {
			d := &Deployer{exec: serviceStateExecutor{state: tc.state}}
			err := d.ensureKafkaServiceAvailable(context.Background(), task)
			if (err != nil) != tc.blocked {
				t.Fatalf("state %q: unexpected error %v", tc.state, err)
			}
		})
	}
}
