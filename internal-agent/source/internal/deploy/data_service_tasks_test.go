package deploy

import (
	"context"
	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/pkg/api"
	"strings"
	"testing"
)

func TestDataServiceCommandsAreRoutedAndFailClosedWithoutConfiguration(t *testing.T) {
	engine := NewEngine(&config.Config{}, nil, nil)
	for _, command := range []string{"PRECHECK_SCHEMA", "PRECHECK_CONNECT", "INSTALL_SCHEMA", "INSTALL_CONNECT", "VERIFY_SCHEMA_REGISTRY", "VERIFY_CONNECT"} {
		result, err := engine.Execute(context.Background(), &api.Task{TaskID: "test", Command: command})
		if err != nil || result == nil || result.Status != "FAILED" || strings.Contains(result.ErrorMsg, "Unknown command") {
			t.Fatalf("%s: result=%+v err=%v", command, result, err)
		}
	}
}
