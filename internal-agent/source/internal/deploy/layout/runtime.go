package layout

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// CheckDirectory rejects symlinks and special ancestors before any mutation.
func CheckDirectory(p string) error {
	for current := p; current != "/" && current != "."; current = filepath.Dir(current) {
		info, e := os.Lstat(current)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return e
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("candidate directory is not canonical: %s", current)
		}
	}
	return nil
}

// PrepareRuntime copies only verified bin/libs bytes into the selected active
// installation. Existing differing files are never overwritten. Repeating this
// after an interrupted copy is safe. Configurations and storage are not copied.
func PrepareRuntime(ctx context.Context, source, destination string) error {
	if !Approved(destination) || Overlap(source, destination) {
		return fmt.Errorf("candidate installation overlaps cached Kafka runtime")
	}
	for _, dir := range []string{"bin", "libs"} {
		existingRoot := filepath.Join(destination, dir)
		if _, err := os.Lstat(existingRoot); err == nil {
			if err := filepath.WalkDir(existingRoot, func(p string, entry os.DirEntry, e error) error {
				if e != nil {
					return e
				}
				rel, e := filepath.Rel(destination, p)
				if e != nil {
					return e
				}
				info, e := os.Lstat(filepath.Join(source, rel))
				if e != nil || entry.Type()&os.ModeSymlink != 0 || entry.IsDir() != info.IsDir() {
					return fmt.Errorf("unexpected candidate runtime entry: %s", p)
				}
				return nil
			}); err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}
		base := filepath.Join(source, dir)
		count := 0
		var total int64
		if err := filepath.WalkDir(base, func(p string, entry os.DirEntry, e error) error {
			if e != nil {
				return e
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			count++
			if count > 20000 {
				return fmt.Errorf("runtime copy entry limit exceeded")
			}
			relative, e := filepath.Rel(source, p)
			if e != nil {
				return e
			}
			target := filepath.Join(destination, relative)
			if err := CheckDirectory(filepath.Dir(target)); err != nil {
				return err
			}
			info, e := entry.Info()
			if e != nil {
				return e
			}
			if entry.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("cached Kafka runtime contains symlink")
			}
			if entry.IsDir() {
				if err := CheckDirectory(target); err != nil {
					return err
				}
				return os.MkdirAll(target, 0755)
			}
			if !info.Mode().IsRegular() || info.Size() > 256*1024*1024 || info.Size() < 0 {
				return fmt.Errorf("unbounded Kafka runtime file")
			}
			total += info.Size()
			if total > 2*1024*1024*1024 {
				return fmt.Errorf("runtime copy exceeds size bound")
			}
			data, e := os.ReadFile(p)
			if e != nil {
				return e
			}
			if existing, e := os.Lstat(target); e == nil {
				if !existing.Mode().IsRegular() || existing.Mode()&os.ModeSymlink != 0 {
					return fmt.Errorf("foreign Kafka runtime file")
				}
				old, e := os.ReadFile(target)
				if e != nil || !bytes.Equal(old, data) {
					return fmt.Errorf("candidate runtime differs from verified package: %s", target)
				}
				return nil
			} else if !os.IsNotExist(e) {
				return e
			}
			// Write through an exclusive temporary file and publish atomically; a crash
			// never leaves a truncated final executable accepted on resume.
			f, e := os.CreateTemp(filepath.Dir(target), ".tantor-runtime-")
			if e != nil {
				return e
			}
			temp := f.Name()
			defer os.Remove(temp)
			if e = f.Chmod(info.Mode().Perm()); e == nil {
				_, e = f.Write(data)
			}
			if e == nil {
				e = f.Sync()
			}
			closeErr := f.Close()
			if e != nil {
				return e
			}
			if closeErr != nil {
				return closeErr
			}
			if e = os.Link(temp, target); e != nil {
				return e
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}
