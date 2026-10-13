package kafka

import (
	"os"
	"syscall"
)

func preservePatchOwner(file *os.File, original os.FileInfo) error {
	if owner, ok := original.Sys().(*syscall.Stat_t); ok {
		return file.Chown(int(owner.Uid), int(owner.Gid))
	}
	return nil
}
