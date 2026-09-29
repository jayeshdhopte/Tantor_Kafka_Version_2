package dataservice

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
)

// InspectArchive verifies the entire archive before privileged extraction.
// Internal symlinks are permitted (the unified bundle uses them for Java libs).
func InspectArchive(filename, kind string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return "", err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	entries := map[string]*tar.Header{}
	root := ""
	for {
		h, e := tr.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return "", e
		}
		name := strings.TrimPrefix(strings.TrimSuffix(h.Name, "/"), "./")
		if name == "" || path.IsAbs(name) || path.Clean(name) != name || strings.HasPrefix(name, "../") || strings.Contains(name, "\\") {
			return "", fmt.Errorf("unsafe archive entry %q", h.Name)
		}
		top := strings.Split(name, "/")[0]
		if root == "" {
			root = top
		}
		if root != top {
			return "", fmt.Errorf("archive must contain a single distribution root")
		}
		if _, exists := entries[name]; exists {
			return "", fmt.Errorf("duplicate archive entry %s", name)
		}
		if h.Mode&06000 != 0 {
			return "", fmt.Errorf("setuid/setgid archive entry %s", name)
		}
		switch h.Typeflag {
		case tar.TypeReg, tar.TypeRegA, tar.TypeDir, tar.TypeSymlink, tar.TypeLink:
		default:
			return "", fmt.Errorf("unsupported archive type for %s", name)
		}
		copy := *h
		entries[name] = &copy
	}
	var resolve func(string, map[string]bool) (string, error)
	resolve = func(name string, seen map[string]bool) (string, error) {
		if seen[name] {
			return "", fmt.Errorf("archive link cycle at %s", name)
		}
		seen[name] = true
		h, ok := entries[name]
		if !ok {
			return "", fmt.Errorf("missing archive link target %s", name)
		}
		if h.Typeflag != tar.TypeSymlink && h.Typeflag != tar.TypeLink {
			return name, nil
		}
		target := h.Linkname
		if path.IsAbs(target) || strings.Contains(target, "\\") {
			return "", fmt.Errorf("unsafe link at %s", name)
		}
		if h.Typeflag == tar.TypeSymlink {
			target = path.Join(path.Dir(name), target)
		} else {
			target = path.Clean(target)
		}
		if !strings.HasPrefix(target, root+"/") {
			return "", fmt.Errorf("link escapes distribution: %s", name)
		}
		return resolve(target, seen)
	}
	for name, h := range entries {
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if p := entries[parent]; p != nil && p.Typeflag != tar.TypeDir {
				return "", fmt.Errorf("archive writes through non-directory %s", parent)
			}
		}
		if h.Typeflag == tar.TypeLink || h.Typeflag == tar.TypeSymlink {
			if _, err = resolve(name, map[string]bool{}); err != nil {
				return "", err
			}
		}
	}
	script := "bin/connect-distributed.sh"
	library := "libs/connect-runtime-"
	if kind == Schema {
		script = "bin/schema-registry-start"
		library = "share/java/schema-registry/kafka-schema-registry-"
	}
	h := entries[root+"/"+script]
	if h == nil || (h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeRegA) || h.Mode&0111 == 0 {
		return "", fmt.Errorf("distribution lacks executable %s", script)
	}
	hasLibrary := false
	for name := range entries {
		version := strings.TrimPrefix(name, root+"/"+library)
		if version != name && len(version) > 0 && version[0] >= '0' && version[0] <= '9' && strings.HasSuffix(name, ".jar") {
			hasLibrary = true
		}
	}
	if !hasLibrary {
		return "", fmt.Errorf("distribution lacks runtime libraries for %s", kind)
	}
	return root, nil
}
