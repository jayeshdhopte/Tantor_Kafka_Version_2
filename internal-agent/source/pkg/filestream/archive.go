// Package filestream validates bundled connector consistency without extracting
// or modifying a parcel. Both internal and external installation paths use it.
package filestream

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

func ValidateArchive(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()
	reader := tar.NewReader(gz)
	var selected, digest string
	for {
		header, err := reader.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		name := path.Base(header.Name)
		if !strings.HasPrefix(name, "connect-file-") || !strings.HasSuffix(name, ".jar") || !header.FileInfo().Mode().IsRegular() {
			continue
		}
		hash := sha256.New()
		if _, err := io.Copy(hash, reader); err != nil {
			return err
		}
		current := fmt.Sprintf("%x", hash.Sum(nil))
		if selected != "" && digest != current {
			return fmt.Errorf("inconsistent FileStream parcel: %s differs from %s; rebuild the parcel using one runtime-matched FileStream JAR in libs, share and plugin directories", header.Name, selected)
		}
		selected, digest = header.Name, current
	}
}
