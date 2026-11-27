package javaupgrade

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

type runtimeOverride struct {
	path        string
	original    []byte
	existed     bool
	envOriginal []byte
	envExisted  bool
}

// Use a dedicated, late EnvironmentFile so the selected Kafka services do not
// remain pinned to their previous JAVA_HOME. Never edit the original unit.
func captureRuntimeOverrides(services []ServiceTarget) ([]runtimeOverride, error) {
	var saved []runtimeOverride
	for _, s := range services {
		p := filepath.Join("/etc/systemd/system", s.Name+".d", "zzzz-tantor-java-upgrade.conf")
		info, err := os.Lstat(p)
		entry := runtimeOverride{path: p}
		if err == nil {
			if !info.Mode().IsRegular() {
				return nil, fmt.Errorf("runtime override is not a regular file: %s", p)
			}
			entry.original, err = os.ReadFile(p)
			entry.existed = true
		}
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		envInfo, envErr := os.Lstat(p + ".env")
		if envErr == nil {
			if !envInfo.Mode().IsRegular() {
				return nil, fmt.Errorf("runtime environment is not a regular file: %s", p+".env")
			}
			entry.envOriginal, envErr = os.ReadFile(p + ".env")
			entry.envExisted = true
		}
		if envErr != nil && !os.IsNotExist(envErr) {
			return nil, envErr
		}
		saved = append(saved, entry)
	}
	return saved, nil
}

func writeRuntimeFile(ctx context.Context, run func(context.Context, string, ...string) (string, error), path string, data []byte) error {
	temp, err := os.CreateTemp("", "tantor-java-runtime-*")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	if _, err = temp.Write(data); err != nil {
		temp.Close()
		return err
	}
	if err = temp.Close(); err != nil {
		return err
	}
	if _, err = run(ctx, "mkdir", "-p", "--", filepath.Dir(path)); err != nil {
		return err
	}
	_, err = run(ctx, "install", "-m", "0644", "--", temp.Name(), path)
	return err
}

func applyRuntimeOverrides(ctx context.Context, run func(context.Context, string, ...string) (string, error), saved []runtimeOverride, jdk string) error {
	for _, entry := range saved {
		envPath := entry.path + ".env"
		if err := writeRuntimeFile(ctx, run, envPath, []byte("JAVA_HOME="+jdk+"\nJRE_HOME="+jdk+"\n")); err != nil {
			return err
		}
		data := []byte("[Service]\nEnvironmentFile=" + envPath + "\n")
		if err := writeRuntimeFile(ctx, run, entry.path, data); err != nil {
			return err
		}
	}
	_, err := run(ctx, "systemctl", "daemon-reload")
	return err
}

func restoreRuntimeOverrides(ctx context.Context, run func(context.Context, string, ...string) (string, error), saved []runtimeOverride) error {
	for _, entry := range saved {
		for _, file := range []struct {
			path    string
			data    []byte
			existed bool
		}{{entry.path, entry.original, entry.existed}, {entry.path + ".env", entry.envOriginal, entry.envExisted}} {
			if file.existed {
				if err := writeRuntimeFile(ctx, run, file.path, file.data); err != nil {
					return err
				}
			} else if _, err := run(ctx, "rm", "-f", "--", file.path); err != nil {
				return err
			}
		}
	}
	_, err := run(ctx, "systemctl", "daemon-reload")
	return err
}
