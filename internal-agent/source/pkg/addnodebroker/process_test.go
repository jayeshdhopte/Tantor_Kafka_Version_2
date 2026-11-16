package addnodebroker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunningBrokerMustMatchConfigJavaHeapAndLibraryRoot(t *testing.T) {
	root := t.TempDir()
	proc := filepath.Join(root, "proc")
	java := filepath.Join(root, "java")
	install := filepath.Join(root, "kafka")
	config := filepath.Join(install, "config", "broker.properties")
	os.MkdirAll(filepath.Join(proc, "12"), 0755)
	os.MkdirAll(filepath.Join(java, "bin"), 0755)
	os.MkdirAll(filepath.Join(install, "bin"), 0755)
	os.MkdirAll(filepath.Join(install, "libs"), 0755)
	exe := filepath.Join(java, "bin", "java")
	os.WriteFile(exe, []byte("java"), 0755)
	if err := os.Symlink(exe, filepath.Join(proc, "12", "exe")); err != nil {
		t.Skip("process fixture requires symlinks")
	}
	fields := append([]string{"S"}, strings.Fields(strings.Repeat("0 ", 18))...)
	fields = append(fields, "12345")
	os.WriteFile(filepath.Join(proc, "12", "stat"), []byte("12 (java) "+strings.Join(fields, " ")), 0644)
	valid := []string{exe, "-Xmx1G", "-cp", install + "/bin/../libs/*", "kafka.Kafka", config}
	write := func(args []string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(proc, "12", "cmdline"), []byte(strings.Join(args, "\x00")+"\x00"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	write(valid)
	if err := verifyBrokerProcess(proc, "12", config, java, install, "1G"); err != nil {
		t.Fatal(err)
	}
	for index, value := range map[int]string{1: "-Xmx2G", 3: "/foreign/libs/*", 4: "org.apache.kafka.tools.StorageTool", 5: config + ".other"} {
		changed := append([]string{}, valid...)
		changed[index] = value
		write(changed)
		if verifyBrokerProcess(proc, "12", config, java, install, "1G") == nil {
			t.Fatalf("changed argument %d accepted", index)
		}
	}
	if verifyBrokerProcess(proc, "0", config, java, install, "1G") == nil {
		t.Fatal("missing MainPID accepted")
	}
}
