//go:build linux

package expansion

import (
	"context"
	"fmt"
	"io.translab/tantor-agent/internal/executor"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Type=simple can report active while its reviewed shell script still prepares
// the JVM. Wait for exec(java); the caller must still verify the full runtime.
func waitReviewedJava(ctx context.Context, e executor.Executor, unit, config string) error {
	bounded, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	return awaitJavaLaunch(bounded, func() (bool, error) {
		state, err := (&nativeParticipantEnvironment{exec: e}).show(bounded, unit)
		if err != nil {
			return false, err
		}
		if state["ActiveState"] == "failed" || state["ActiveState"] == "inactive" {
			return false, fmt.Errorf("reviewed service exited before Java startup")
		}
		pid, err := strconv.Atoi(state["MainPID"])
		if err != nil || pid < 1 {
			return false, nil
		}
		executable, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(pid), "exe"))
		if err != nil {
			return false, err
		}
		raw, err := readBounded(bounded, filepath.Join("/proc", strconv.Itoa(pid), "cmdline"), 262144)
		if err != nil {
			return false, err
		}
		return javaLaunchReady(executable, strings.Split(strings.TrimSuffix(raw, "\x00"), "\x00"), config)
	})
}
func awaitJavaLaunch(ctx context.Context, probe func() (bool, error)) error {
	for {
		ready, err := probe()
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("reviewed Java startup did not complete before the bounded deadline: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
func javaLaunchReady(executable string, args []string, config string) (bool, error) {
	switch filepath.Base(executable) {
	case "java":
		for i, arg := range args {
			if arg == "kafka.Kafka" && i+1 < len(args) && args[i+1] == config {
				return true, nil
			}
		}
		return false, fmt.Errorf("reviewed unit launched a different Java main/configuration")
	case "bash", "sh":
		for i, arg := range args {
			if strings.HasSuffix(arg, "/bin/kafka-server-start.sh") && i+1 < len(args) && args[i+1] == config {
				return false, nil
			}
			if strings.HasSuffix(arg, "/bin/kafka-run-class.sh") {
				for j, value := range args {
					if value == "kafka.Kafka" && j+1 < len(args) && args[j+1] == config {
						return false, nil
					}
				}
			}
		}
		return false, fmt.Errorf("reviewed startup shell target differs")
	default:
		return false, fmt.Errorf("unexpected executable while starting reviewed Kafka unit")
	}
}
