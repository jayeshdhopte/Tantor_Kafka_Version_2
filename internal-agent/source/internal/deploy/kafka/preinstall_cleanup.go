package kafka

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"io.translab/tantor-agent/internal/taskvalidate"
	"io.translab/tantor-agent/pkg/api"
)

// A prerequisite failure can leave empty directories before deployment state
// exists. Without that ownership record we never stop services, unlink an active
// installation, or recursively remove any directory.
func (d *Deployer) cleanUninstalledDirectories(ctx context.Context, task *api.Task) error {
	if task.Parameters["cleanup_artifact_downloads"] == "" ||
		firstNonEmpty(task.Parameters["kafka_install_base_dir"], task.Parameters["kafka_install_dir"]) == "" ||
		firstNonEmpty(task.Parameters["target_version"], task.Parameters["version"]) == "" {
		return fmt.Errorf("deployment state is missing; cannot establish the failed installation paths")
	}
	paths := resolveKafkaInstallPaths(task)
	if _, err := os.Lstat(paths.ActiveDir); !os.IsNotExist(err) {
		return fmt.Errorf("deployment state is missing; active installation %s must be inspected before cleanup", paths.ActiveDir)
	}
	services, err := cleanupServices([]string{"broker", "controller", "zookeeper", "broker-exporter", "controller-exporter"}, task.Parameters["cleanup_services"])
	if err != nil {
		return err
	}
	for _, service := range services {
		state, stderr, err := d.exec.RunSudo(ctx, "systemctl", "show", service+".service", "--property=LoadState", "--value")
		// systemctl may return a nonzero status for a missing unit. Its explicit
		// LoadState is required; command errors must not count as an absent unit.
		if strings.TrimSpace(state) != "not-found" {
			return fmt.Errorf("deployment state is missing; cannot clean while unit %s has LoadState %q: %v %s", service, strings.TrimSpace(state), err, stderr)
		}
	}
	dataDir := firstNonEmpty(task.Parameters["kafka_data_dir"], defaultKafkaDataDir(paths.BaseDir))
	return removeEmptyPrerequisiteDirectories(uniqueNonEmpty([]string{paths.VersionedDir, dataDir}))
}

func removeEmptyPrerequisiteDirectories(dirs []string) error {
	// Validate the whole plan before removing anything. os.Remove below is
	// intentionally non-recursive, so new files appearing after validation survive.
	for _, dir := range dirs {
		if err := validateKafkaStorageDir(dir); err != nil {
			return err
		}
		if _, err := taskvalidate.ApprovedPath(dir); err != nil {
			return err
		}
		info, err := os.Lstat(dir)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		real, resolveErr := filepath.EvalSymlinks(dir)
		if !info.IsDir() || resolveErr != nil || real != dir {
			return fmt.Errorf("missing deployment state: refusing redirected or non-directory path %s", dir)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		if len(entries) != 0 {
			return fmt.Errorf("missing deployment state: refusing nonempty directory %s", dir)
		}
	}
	for _, dir := range dirs {
		if err := os.Remove(dir); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove empty prerequisite directory %s: %w", dir, err)
		}
	}
	return nil
}
