// Package pathpolicy validates UI-selected filesystem paths against the
// administrator-approved roots stored in the agent configuration.
package pathpolicy

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Resolve returns an absolute, cleaned path only when it is inside one of the
// configured roots. Existing path components are evaluated for symlink escape.
func Resolve(value, approvedRoots, field string) (string, error) {
	candidate := filepath.Clean(strings.TrimSpace(value))
	if candidate == "." || !filepath.IsAbs(candidate) {
		return "", fmt.Errorf("%s must be an absolute path", field)
	}

	resolvedCandidate := resolveExisting(candidate)
	for _, configuredRoot := range strings.Split(approvedRoots, ":") {
		root := filepath.Clean(strings.TrimSpace(configuredRoot))
		if root == "." || !filepath.IsAbs(root) {
			continue
		}
		resolvedRoot := resolveExisting(root)
		rel, err := filepath.Rel(resolvedRoot, resolvedCandidate)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("%s %q is outside administrator-approved deployment roots", field, candidate)
}

func resolveExisting(path string) string {
	current := path
	for {
		if evaluated, err := filepath.EvalSymlinks(current); err == nil {
			if current == path {
				return evaluated
			}
			suffix, _ := filepath.Rel(current, path)
			return filepath.Join(evaluated, suffix)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return path
		}
		current = parent
	}
}
