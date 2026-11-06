//go:build linux

package expansion

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

func verifyResources(ctx context.Context, p Plan) error {
	raw, err := readBounded(ctx, "/proc/meminfo", 65536)
	if err != nil {
		return err
	}
	available := uint64(0)
	seen := false
	for _, line := range strings.Split(raw, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == "MemAvailable:" && fields[2] == "kB" {
			if seen {
				return fmt.Errorf("ambiguous available memory observation")
			}
			seen = true
			n, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil || n > math.MaxUint64/1024 {
				return fmt.Errorf("invalid available memory observation")
			}
			available = n * 1024
		}
	}
	if !seen {
		return fmt.Errorf("available memory observation is missing")
	}
	mounts := map[string]MountCapacity{}
	for _, process := range p.Processes {
		for _, path := range append(storagePaths(process), process.LogPath) {
			ancestor := path
			for {
				if err = noLinks(ancestor); err == nil {
					break
				} else if !os.IsNotExist(err) {
					return err
				}
				next := filepath.Dir(ancestor)
				if next == ancestor {
					return fmt.Errorf("filesystem ancestor unavailable")
				}
				ancestor = next
			}
			info, err := os.Stat(ancestor)
			if err != nil || !info.IsDir() {
				return fmt.Errorf("candidate filesystem ancestor is not a directory")
			}
			identity, ok := info.Sys().(*syscall.Stat_t)
			if !ok {
				return fmt.Errorf("candidate filesystem identity unavailable")
			}
			var stat syscall.Statfs_t
			if err = syscall.Statfs(ancestor, &stat); err != nil {
				return err
			}
			if stat.Bsize < 1 || stat.Bavail > math.MaxUint64/uint64(stat.Bsize) {
				return fmt.Errorf("candidate filesystem capacity overflow")
			}
			mounts[path] = MountCapacity{Device: uint64(identity.Dev), AvailableBytes: stat.Bavail * uint64(stat.Bsize), AvailableInodes: stat.Ffree}
		}
	}
	return ResourceBudget(p, available, mounts)
}
