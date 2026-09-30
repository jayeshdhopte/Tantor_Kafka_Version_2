package dataservice

import (
	"context"
	_ "embed"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"io.translab/tantor-agent/internal/executor"
	"io.translab/tantor-agent/pkg/api"
)

//go:embed precheck.sh
var precheckScript string

func precheckShell() string {
	// A Windows checkout can embed CRLF even in a Linux build. Bash treats
	// trailing CR as part of tokens such as `set -u` and `else`.
	return strings.ReplaceAll(precheckScript, "\r\n", "\n")
}

func CheckPaths(ctx context.Context, exec executor.Executor, s Settings) error {
	var protected []string
	for _, dir := range s.Protected {
		out, errOut, err := exec.Run(ctx, "readlink", "-m", "--", dir)
		if err != nil || SafePath(strings.TrimSpace(out)) != nil {
			return fmt.Errorf("cannot resolve protected path %s: %s %v", dir, errOut, err)
		}
		protected = append(protected, strings.TrimSpace(out))
	}
	for _, dir := range s.Directories() {
		out, errOut, err := exec.Run(ctx, "readlink", "-m", "--", dir)
		real := strings.TrimSpace(out)
		if err != nil || SafePath(real) != nil {
			return fmt.Errorf("cannot resolve service path %s: %s %v", dir, errOut, err)
		}
		for _, keep := range protected {
			if Overlaps(real, keep) {
				return fmt.Errorf("service path %s resolves into protected Kafka/other-service path %s", dir, keep)
			}
		}
		// Privileged read-only check; do not create anything during Preview.
		_, errOut, err = exec.RunSudo(ctx, "bash", "-c", `p=$1; while [ ! -e "$p" ] && [ "$p" != / ]; do p=$(dirname -- "$p"); done; test -d "$p" && test -w "$p"`, "--", dir)
		if err != nil {
			return fmt.Errorf("service path %s is not creatable: %s", dir, errOut)
		}
	}
	return nil
}

func Precheck(ctx context.Context, exec executor.Executor, kind string, task *api.Task) (string, error) {
	s, err := Parse(kind, task)
	if err != nil {
		return "[FAIL] Configuration: " + err.Error(), err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var logs strings.Builder
	// Preview does not download a potentially large artifact; install verifies its bytes.
	_, hashErr := hex.DecodeString(task.Checksum)
	if task.ArtifactURL == "" || len(task.Checksum) != 64 || hashErr != nil {
		return "[FAIL] Artifact URL and SHA-256 are required", fmt.Errorf("artifact URL and SHA-256 required")
	}
	if err = CheckPaths(ctx, exec, s); err != nil {
		return "[FAIL] Directory safety: " + err.Error(), err
	}
	logs.WriteString("[PASS] Configuration and protected Kafka paths\n")
	args := []string{"-c", precheckShell(), "--", kind, strconv.Itoa(s.Port), s.Bootstrap, s.KafkaBin, s.Plugin, s.Working, strconv.Itoa(s.MinDisk), strconv.FormatBool(s.Deferred), s.HAPeers}
	if kind == Schema {
		args = append(args, s.SchemaTopic)
	} else {
		args = append(args, s.ConfigTopic, s.OffsetTopic, s.StatusTopic)
	}
	out, errOut, err := exec.Run(ctx, "bash", args...)
	logs.WriteString(out)
	if errOut != "" {
		logs.WriteString("\n" + errOut)
	}
	if err != nil {
		return logs.String(), fmt.Errorf("%s prechecks failed: %w", kind, err)
	}
	return logs.String(), nil
}
