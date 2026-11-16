package addnodebroker

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func observeFormatter(procRoot, configPath string) (bool, bool) {
	entries, err := os.ReadDir(procRoot)
	if err != nil || len(entries) > 65536 {
		return false, false
	}
	complete := true
	active := false
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		f, err := os.Open(filepath.Join(procRoot, entry.Name(), "cmdline"))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			complete = false
			continue
		}
		data, err := io.ReadAll(io.LimitReader(f, 65537))
		f.Close()
		if err != nil || len(data) > 65536 {
			complete = false
			continue
		}
		bound, formatter := false, false
		for _, arg := range strings.Split(string(data), "\x00") {
			if arg == configPath || arg == "--config="+configPath {
				bound = true
			}
			if arg == "kafka.tools.StorageTool" || arg == "org.apache.kafka.tools.StorageTool" || filepath.Base(arg) == "kafka-storage.sh" {
				formatter = true
			}
		}
		if bound && formatter {
			active = true
		}
	}
	return complete, active
}
