// Package artifactwork manages only private deployment staging, never shared
// parcel archives, installed binaries, or arbitrary files in an artifact root.
package artifactwork

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const prefix = ".tantor-work-"
const ownerFile = ".tantor-owner.json"
const lockName = ".tantor-active.lock"

type Owner struct {
	ClusterID string `json:"clusterId"`
	TaskID    string `json:"taskId"`
}
type Workspace struct {
	dir, root string
	lock      *os.File
}

func canonicalRoot(root string) (string, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == string(filepath.Separator) {
		return "", fmt.Errorf("invalid artifact root")
	}
	real, err := filepath.EvalSymlinks(root)
	if err != nil || real != root {
		return "", fmt.Errorf("artifact root is missing or redirected: %s", root)
	}
	return root, nil
}
func New(root string, owner Owner) (*Workspace, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == string(filepath.Separator) {
		return nil, fmt.Errorf("invalid artifact root")
	}
	if err := os.MkdirAll(root, 0750); err != nil {
		return nil, err
	}
	if _, err := canonicalRoot(root); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp(root, prefix)
	if err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, lockName), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err == nil {
		err = lockFile(lock)
	}
	if err != nil {
		if lock != nil {
			lock.Close()
		}
		os.RemoveAll(dir)
		return nil, err
	}
	data, err := json.Marshal(owner)
	if err == nil {
		err = os.WriteFile(filepath.Join(dir, ownerFile), data, 0600)
	}
	if err != nil {
		unlockFile(lock)
		lock.Close()
		os.RemoveAll(dir)
		return nil, err
	}
	return &Workspace{dir: dir, root: root, lock: lock}, nil
}
func (w *Workspace) Dir() string { return w.dir }
func (w *Workspace) Close() error {
	defer w.release()
	if _, err := os.Lstat(w.dir); os.IsNotExist(err) {
		return nil
	}
	if _, err := canonicalRoot(w.root); err != nil {
		return err
	}
	if filepath.Dir(w.dir) != w.root || !strings.HasPrefix(filepath.Base(w.dir), prefix) {
		return fmt.Errorf("unowned artifact workspace")
	}
	info, err := os.Lstat(w.dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("redirected artifact workspace")
	}
	return os.RemoveAll(w.dir)
}

func (w *Workspace) release() {
	if w.lock != nil {
		unlockFile(w.lock)
		w.lock.Close()
		w.lock = nil
	}
}

// CleanupCluster reclaims abandoned workspaces only when their local ownership
// record matches. Different clusters, symlinks and legacy/unmarked files remain.
func CleanupCluster(root, cluster string) error {
	if cluster == "" {
		return fmt.Errorf("cluster identity required for artifact cleanup")
	}
	if _, err := os.Stat(root); os.IsNotExist(err) {
		return nil
	}
	if _, err := canonicalRoot(root); err != nil {
		return err
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		dir := filepath.Join(root, entry.Name())
		path := filepath.Join(dir, ownerFile)
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var owner Owner
		if json.Unmarshal(data, &owner) != nil || owner.ClusterID != cluster || owner.TaskID == "" {
			continue
		}
		lockPath := filepath.Join(dir, lockName)
		info, err = os.Lstat(lockPath)
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("artifact workspace lock is missing or redirected: %s", dir)
		}
		lock, err := os.OpenFile(lockPath, os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		if err = lockFile(lock); err != nil {
			lock.Close()
			return fmt.Errorf("artifact workspace is still in use by task %s: %w", owner.TaskID, err)
		}
		if err := (&Workspace{dir: dir, root: root, lock: lock}).Close(); err != nil {
			return err
		}
	}
	return nil
}
