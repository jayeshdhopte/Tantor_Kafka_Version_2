// Package kafkaconfig shares file semantics; callers must authorize the discovered path first.
package kafkaconfig

import (
	"context"
	"io.translab/tantor-agent/internal/configbackup"
	"io.translab/tantor-agent/internal/deploy/kafka"
	"io.translab/tantor-agent/internal/executor"
	"time"
)

func Patch(content string, changes, expected map[string]*string) (string, bool, error) {
	return kafka.ApplyPropertyPatch(content, changes, expected)
}
func ValidateRecovery(parameters map[string]string, changes, expected map[string]*string) error {
	return kafka.ValidateConfigRecovery(parameters, changes, expected)
}
func Read(content string) (map[string]string, error) { return kafka.ReadableProperties(content) }
func Write(path string, content []byte) error        { return kafka.WritePropertyPatch(path, content) }
func Reserve(directory, path string, now time.Time) (string, error) {
	return configbackup.ReservePath(directory, path, now)
}
func Register(ctx context.Context, directory string) error {
	return configbackup.Register(ctx, executor.New(executor.Options{PrivilegeMode: "direct"}), directory)
}
func Sweep(now time.Time) error { return configbackup.Sweep(configbackup.Registry, now) }
