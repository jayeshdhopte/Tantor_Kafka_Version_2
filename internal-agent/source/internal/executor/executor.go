package executor

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Executor defines an interface for running commands.
type Executor interface {
	RunSudo(ctx context.Context, cmd string, args ...string) (string, string, error)
	Run(ctx context.Context, cmd string, args ...string) (string, string, error)
}

type Options struct {
	PrivilegeMode string
	SudoPath      string
}

type DefaultExecutor struct {
	privilegeMode string
	sudoPath      string
}

// New preserves the previous zero-argument constructor while allowing runtime
// privilege configuration for production installations.
func New(options ...Options) *DefaultExecutor {
	opts := Options{PrivilegeMode: "sudo", SudoPath: "/usr/bin/sudo"}
	if len(options) > 0 {
		if strings.TrimSpace(options[0].PrivilegeMode) != "" {
			opts.PrivilegeMode = strings.ToLower(strings.TrimSpace(options[0].PrivilegeMode))
		}
		if strings.TrimSpace(options[0].SudoPath) != "" {
			opts.SudoPath = options[0].SudoPath
		}
	}
	return &DefaultExecutor{privilegeMode: opts.PrivilegeMode, sudoPath: opts.SudoPath}
}

func (e *DefaultExecutor) Run(ctx context.Context, name string, args ...string) (string, string, error) {
	return e.execute(ctx, false, name, args...)
}

// RunSudo executes a privileged command without ever accepting or storing a
// sudo password. When the agent already runs as root, or privilege.mode=direct,
// the target command is invoked directly. Otherwise sudo is used non-interactively.
func (e *DefaultExecutor) RunSudo(ctx context.Context, name string, args ...string) (string, string, error) {
	return e.execute(ctx, true, name, args...)
}

func (e *DefaultExecutor) execute(ctx context.Context, privileged bool, name string, args ...string) (string, string, error) {
	if strings.TrimSpace(name) == "" {
		return "", "", fmt.Errorf("command name must not be empty")
	}

	commandName := name
	commandArgs := append([]string(nil), args...)

	if privileged && e.privilegeMode != "direct" && os.Geteuid() != 0 {
		target, err := resolveExecutable(name)
		if err != nil {
			return "", "", err
		}
		commandName = e.sudoPath
		commandArgs = append([]string{"-n", "--", target}, args...)
	}

	cmd := exec.CommandContext(ctx, commandName, commandArgs...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	outStr := strings.TrimSpace(stdout.String())
	errStr := strings.TrimSpace(stderr.String())
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return outStr, errStr, fmt.Errorf("command %q canceled: %w", name, ctxErr)
		}
		return outStr, errStr, fmt.Errorf("command %q failed: %w", name, err)
	}
	return outStr, errStr, nil
}

func resolveExecutable(name string) (string, error) {
	if filepath.IsAbs(name) {
		return name, nil
	}
	path, err := exec.LookPath(name)
	if err != nil {
		return "", fmt.Errorf("required command %q was not found in PATH: %w", name, err)
	}
	return path, nil
}

// RunWithTimeout executes a command with a specified timeout.
func RunWithTimeout(timeout time.Duration, useSudo bool, name string, args ...string) (string, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	e := New()
	if useSudo {
		return e.RunSudo(ctx, name, args...)
	}
	return e.Run(ctx, name, args...)
}
