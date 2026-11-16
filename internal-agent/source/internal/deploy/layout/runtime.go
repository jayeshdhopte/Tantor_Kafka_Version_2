package layout

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// VerifyRuntime observes installed bin/libs without repairing or creating files.
func VerifyRuntime(ctx context.Context, source, destination string) error {
	if err := CheckDirectory(source); err != nil {
		return err
	}
	if err := CheckDirectory(destination); err != nil {
		return err
	}
	seen := map[string]bool{}
	count := 0
	var total int64
	for _, dir := range []string{"bin", "libs"} {
		if err := filepath.WalkDir(filepath.Join(source, dir), func(p string, e os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			count++
			if count > 100000 {
				return fmt.Errorf("runtime exceeds inspection bound")
			}
			rel, err := filepath.Rel(source, p)
			if err != nil {
				return err
			}
			target := filepath.Join(destination, rel)
			seen[target] = true
			a, err := os.Lstat(p)
			if err != nil {
				return err
			}
			b, err := os.Lstat(target)
			if err != nil {
				return err
			}
			if a.Mode()&os.ModeSymlink != 0 || b.Mode()&os.ModeSymlink != 0 || a.IsDir() != b.IsDir() {
				return fmt.Errorf("foreign runtime entry")
			}
			if a.IsDir() {
				return nil
			}
			if !a.Mode().IsRegular() || !b.Mode().IsRegular() || a.Size() != b.Size() || a.Size() > 256*1024*1024 {
				return fmt.Errorf("runtime file identity differs")
			}
			total += a.Size()
			if total > 2*1024*1024*1024 {
				return fmt.Errorf("runtime inspection exceeds byte bound")
			}
			hash := func(path string) ([32]byte, error) {
				var sum [32]byte
				f, err := os.Open(path)
				if err != nil {
					return sum, err
				}
				defer f.Close()
				h := sha256.New()
				_, err = io.Copy(h, io.LimitReader(f, 256*1024*1024+1))
				copy(sum[:], h.Sum(nil))
				return sum, err
			}
			ah, err := hash(p)
			if err != nil {
				return err
			}
			bh, err := hash(target)
			if err != nil {
				return err
			}
			if ah != bh || a.Mode().Perm()&0111 != b.Mode().Perm()&0111 {
				return fmt.Errorf("installed runtime differs from approved payload")
			}
			return nil
		}); err != nil {
			return err
		}
	}
	for _, dir := range []string{"bin", "libs"} {
		if err := filepath.WalkDir(filepath.Join(destination, dir), func(p string, e os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !seen[p] {
				return fmt.Errorf("unexpected installed runtime entry")
			}
			return ctx.Err()
		}); err != nil {
			return err
		}
	}
	return nil
}

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
