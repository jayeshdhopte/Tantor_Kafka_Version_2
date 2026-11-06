package expansion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

type diskRuntimeFiles struct{}

func (diskRuntimeFiles) SHA256(ctx context.Context, name string) (string, error) {
	return fileSHA256(ctx, name, 256*1024*1024)
}
func (diskRuntimeFiles) Libraries(ctx context.Context, dir string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := noLinks(dir); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) > 4096 {
		return nil, fmt.Errorf("bounded library directory required")
	}
	out := []string{}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".jar" {
			continue
		}
		if !entry.Type().IsRegular() {
			return nil, fmt.Errorf("regular libraries required")
		}
		out = append(out, filepath.Join(dir, entry.Name()))
	}
	sort.Strings(out)
	return out, nil
}
func noLinks(name string) error {
	if !filepath.IsAbs(name) || filepath.Clean(name) != name {
		return fmt.Errorf("canonical absolute path required")
	}
	for current := name; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink is outside the reviewed path adapter")
		}
		if current != name && !info.IsDir() {
			return fmt.Errorf("path ancestor is not a directory")
		}
		if current == filepath.Dir(current) {
			break
		}
	}
	return nil
}
func openRegular(name string, max int64) (*os.File, os.FileInfo, error) {
	if err := noLinks(name); err != nil {
		return nil, nil, err
	}
	before, err := os.Lstat(name)
	if err != nil {
		return nil, nil, err
	}
	if !before.Mode().IsRegular() || before.Size() > max {
		return nil, nil, fmt.Errorf("bounded regular file required")
	}
	f, err := os.Open(name)
	if err != nil {
		return nil, nil, err
	}
	actual, err := f.Stat()
	if err != nil || !actual.Mode().IsRegular() || !os.SameFile(before, actual) {
		f.Close()
		return nil, nil, fmt.Errorf("file identity changed while opening")
	}
	return f, actual, nil
}
func unchanged(name string, f *os.File, before os.FileInfo) error {
	after, err := f.Stat()
	if err != nil {
		return err
	}
	if err = noLinks(name); err != nil {
		return err
	}
	current, err := os.Lstat(name)
	if err != nil {
		return err
	}
	if !os.SameFile(before, after) || !os.SameFile(before, current) || before.Size() != after.Size() || before.ModTime() != after.ModTime() || before.Mode() != after.Mode() {
		return fmt.Errorf("file changed during observation")
	}
	return nil
}
func fileSHA256(ctx context.Context, name string, max int64) (string, error) {
	f, before, err := openRegular(name, max)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(&contextReader{ctx: ctx, reader: f}, max+1))
	if err != nil {
		return "", err
	}
	if n > max {
		return "", fmt.Errorf("file size limit exceeded")
	}
	if err = unchanged(name, f, before); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func readBounded(ctx context.Context, name string, max int64) (string, error) {
	f, before, err := openRegular(name, max)
	if err != nil {
		return "", err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(&contextReader{ctx: ctx, reader: f}, max+1))
	if err != nil {
		return "", err
	}
	if int64(len(raw)) > max {
		return "", fmt.Errorf("file size limit exceeded")
	}
	if err = unchanged(name, f, before); err != nil {
		return "", err
	}
	return string(raw), nil
}

func loadManifest(ctx context.Context, p Plan) (ArtifactManifest, error) {
	// Match the catalog-bound parcel cache only. Never search other installs for
	// a same-version archive with potentially different vendor modifications.
	archive := filepath.Join(p.ParcelDir, ".downloads", "parcel_kafka_"+p.KafkaVersion+".tgz")
	checksum, err := fileSHA256(ctx, archive, 1024*1024*1024)
	if err != nil || checksum != p.ArtifactSHA256 {
		return ArtifactManifest{}, fmt.Errorf("approved catalog archive is unavailable or changed")
	}
	f, before, err := openRegular(archive, 1024*1024*1024)
	if err != nil {
		return ArtifactManifest{}, err
	}
	defer f.Close()
	m, err := Manifest(ctx, f, checksum, p.KafkaVersion)
	if err != nil {
		return m, err
	}
	if err = unchanged(archive, f, before); err != nil {
		return m, err
	}
	return m, nil
}
