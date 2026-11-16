package addnodebroker

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// verifyBrokerProcess binds the unit's live JVM to the reviewed config, Java and
// installed libraries. It never repairs a unit, starts a process or writes /proc.
func verifyBrokerProcess(procRoot, pid, configPath, javaHome, installPath, heap string) error {
	n, err := strconv.Atoi(strings.TrimSpace(pid))
	if err != nil || n <= 0 {
		return fmt.Errorf("broker MainPID unavailable")
	}
	root := filepath.Join(procRoot, strconv.Itoa(n))
	before, err := os.ReadFile(filepath.Join(root, "stat"))
	if err != nil || len(before) > 65536 {
		return fmt.Errorf("broker process identity unavailable")
	}
	executable, err := filepath.EvalSymlinks(filepath.Join(root, "exe"))
	if err != nil {
		return err
	}
	expectedJava, err := filepath.EvalSymlinks(filepath.Join(javaHome, "bin", "java"))
	if err != nil || executable != expectedJava {
		return fmt.Errorf("broker Java executable changed")
	}
	f, err := os.Open(filepath.Join(root, "cmdline"))
	if err != nil {
		return err
	}
	raw, err := io.ReadAll(io.LimitReader(f, 65537))
	f.Close()
	if err != nil || len(raw) > 65536 {
		return fmt.Errorf("broker command line unavailable")
	}
	libraryRoot, err := filepath.EvalSymlinks(filepath.Join(installPath, "libs"))
	if err != nil {
		return err
	}
	args := strings.Split(string(raw), "\x00")
	main, config, heapOK, classpath := false, false, false, false
	for i, arg := range args {
		if arg == "kafka.Kafka" {
			main = true
			if i+1 < len(args) && args[i+1] == configPath {
				config = true
			}
		}
		if strings.HasPrefix(arg, "-Xmx") {
			heapOK = arg == "-Xmx"+heap
		}
		if (arg == "-cp" || arg == "-classpath") && i+1 < len(args) {
			classpath = true
			for _, entry := range strings.Split(args[i+1], ":") {
				// Kafka's launcher commonly uses bin/../libs; resolve the real parent.
				parent, err := filepath.EvalSymlinks(filepath.Dir(entry))
				if !filepath.IsAbs(entry) || err != nil || (parent != libraryRoot && !strings.HasPrefix(parent, libraryRoot+string(os.PathSeparator))) {
					return fmt.Errorf("broker loads libraries outside approved installation")
				}
			}
		}
	}
	after, err := os.ReadFile(filepath.Join(root, "stat"))
	// Only the stable PID/start-time portion matters; CPU counters change normally.
	start := func(b []byte) string {
		s := string(b)
		end := strings.LastIndex(s, ")")
		if end < 0 {
			return ""
		}
		fields := strings.Fields(s[end+1:])
		if len(fields) < 20 {
			return ""
		}
		return fields[19]
	}
	if err != nil || start(before) == "" || start(before) != start(after) || !main || !config || !heapOK || !classpath {
		return fmt.Errorf("broker process does not match frozen runtime")
	}
	return nil
}
