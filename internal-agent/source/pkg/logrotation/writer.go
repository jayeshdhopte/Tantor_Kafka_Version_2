// Package logrotation manages agent application logs only, never Kafka data files.
package logrotation

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

const Retention = 7 * 24 * time.Hour

type Writer struct {
	mu             sync.Mutex
	dir, name, day string
	file           *os.File
	now            func() time.Time
}

func validDirectory(dir string) error {
	if !filepath.IsAbs(dir) || filepath.Clean(dir) == string(filepath.Separator) {
		return fmt.Errorf("log directory must be an absolute non-root path")
	}
	return nil
}
func regular(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("refusing non-regular log file %s", path)
	}
	return nil
}
func New(dir, name string) (*Writer, error) {
	if err := validDirectory(dir); err != nil {
		return nil, err
	}
	if filepath.Base(name) != name || name == "." || !strings.HasSuffix(name, ".log") {
		return nil, fmt.Errorf("invalid managed log filename")
	}
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, err
	}
	archive := filepath.Join(dir, "archive")
	if info, err := os.Lstat(archive); err == nil && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
		return nil, fmt.Errorf("unsafe log archive directory")
	}
	if err := os.MkdirAll(archive, 0750); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, name)
	if err := regular(path); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0640)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	day := now.Format("2006-01-02")
	if info, err := f.Stat(); err == nil && info.Size() > 0 {
		day = info.ModTime().In(now.Location()).Format("2006-01-02")
	}
	w := &Writer{dir: dir, name: name, day: day, file: f, now: time.Now}
	if err = w.Maintain(); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}
func (w *Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return 0, os.ErrClosed
	}
	if err := w.rotate(w.now()); err != nil {
		return 0, err
	}
	return w.file.Write(p)
}
func (w *Writer) rotate(now time.Time) error {
	today := now.Format("2006-01-02")
	if today <= w.day {
		return nil
	} // A clock adjustment must not overwrite an archive.
	info, err := w.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() > 0 {
		if err = w.file.Sync(); err != nil {
			return err
		}
		archive, err := os.CreateTemp(filepath.Join(w.dir, "archive"), w.name+"."+w.day+".*.gz.tmp")
		if err != nil {
			return err
		}
		tmp := archive.Name()
		defer os.Remove(tmp)
		zip := gzip.NewWriter(archive)
		_, copyErr := io.Copy(zip, io.NewSectionReader(w.file, 0, info.Size()))
		zipErr := zip.Close()
		syncErr := archive.Sync()
		closeErr := archive.Close()
		for _, e := range []error{copyErr, zipErr, syncErr, closeErr} {
			if e != nil {
				return e
			}
		}
		if err = os.Chmod(tmp, 0640); err != nil {
			return err
		}
		if err = os.Rename(tmp, strings.TrimSuffix(tmp, ".tmp")); err != nil {
			return err
		}
		// All structured writes hold this mutex; keep the inode so systemd's inherited
		// stderr descriptor remains valid without restarting the agent or losing tasks.
		if err = w.file.Truncate(0); err != nil {
			return err
		}
	}
	w.day = today
	return Prune(w.dir, w.name, now)
}
func (w *Writer) Maintain() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return os.ErrClosed
	}
	if err := w.rotate(w.now()); err != nil {
		return err
	}
	return Prune(w.dir, w.name, w.now())
}
func (w *Writer) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := w.Maintain(); err != nil {
					fmt.Fprintln(os.Stderr, "agent log maintenance:", err)
				}
			}
		}
	}()
}
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

// Prune can also run from a persistent hourly timer while the main agent is stopped.
func Prune(dir, name string, now time.Time) error {
	if err := validDirectory(dir); err != nil {
		return err
	}
	archive := filepath.Join(dir, "archive")
	info, err := os.Lstat(archive)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("unsafe log archive directory")
	}
	entries, err := os.ReadDir(archive)
	if err != nil {
		return err
	}
	pattern := regexp.MustCompile("^" + regexp.QuoteMeta(name) + `\.[0-9]{4}-[0-9]{2}-[0-9]{2}\.[0-9]+\.gz$`)
	for _, entry := range entries {
		if !pattern.MatchString(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || now.Sub(info.ModTime()) < Retention {
			continue
		}
		if err = os.Remove(filepath.Join(archive, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}
