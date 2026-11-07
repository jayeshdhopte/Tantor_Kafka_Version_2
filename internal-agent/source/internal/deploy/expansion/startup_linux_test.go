//go:build linux

package expansion

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReviewedStartupWaitsForShellToExecJava(t *testing.T) {
	calls := 0
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := awaitJavaLaunch(ctx, func() (bool, error) {
		calls++
		if calls < 3 {
			return javaLaunchReady("/usr/bin/bash", []string{"bash", "/opt/kafka/bin/kafka-server-start.sh", "/opt/kafka/config/broker.properties"}, "/opt/kafka/config/broker.properties")
		}
		return javaLaunchReady("/opt/java/bin/java", []string{"java", "-cp", "libs/*", "kafka.Kafka", "/opt/kafka/config/broker.properties"}, "/opt/kafka/config/broker.properties")
	})
	if err != nil || calls != 3 {
		t.Fatal("did not wait for reviewed Java", calls, err)
	}
}
func TestReviewedStartupRejectsWrongTargetAndHasABoundedDeadline(t *testing.T) {
	for _, executable := range []string{"/bin/python3", "/opt/java/bin/java", "/bin/bash"} {
		if _, err := javaLaunchReady(executable, []string{"other", "/opt/foreign/config.properties"}, "/opt/kafka/config/broker.properties"); err == nil {
			t.Fatal("foreign startup accepted", executable)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := awaitJavaLaunch(ctx, func() (bool, error) { return false, nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("startup deadline not propagated", err)
	}
}
