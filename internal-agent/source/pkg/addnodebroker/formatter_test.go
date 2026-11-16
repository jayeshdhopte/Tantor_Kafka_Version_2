package addnodebroker

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFormatterObservationRequiresExactConfigAndCompleteRead(t *testing.T) {
	root := t.TempDir()
	config := "/opt/tantor/kafka/config/broker.properties"
	write := func(pid, cmd string) {
		t.Helper()
		os.MkdirAll(filepath.Join(root, pid), 0755)
		if err := os.WriteFile(filepath.Join(root, pid, "cmdline"), []byte(cmd), 0644); err != nil {
			t.Fatal(err)
		}
	}
	write("1", "java\x00kafka.Kafka\x00"+config+"\x00")
	write("2", "java\x00org.apache.kafka.tools.StorageTool\x00--config\x00"+config+".other\x00")
	if complete, active := observeFormatter(root, config); !complete || active {
		t.Fatal("unrelated process detected")
	}
	write("3", "java\x00org.apache.kafka.tools.StorageTool\x00--config="+config+"\x00")
	if complete, active := observeFormatter(root, config); !complete || !active {
		t.Fatal("owned formatter missed")
	}
	os.RemoveAll(filepath.Join(root, "3"))
	os.MkdirAll(filepath.Join(root, "4", "cmdline"), 0755)
	if complete, _ := observeFormatter(root, config); complete {
		t.Fatal("unreadable process accepted")
	}
	if complete, _ := observeFormatter(filepath.Join(root, "missing"), config); complete {
		t.Fatal("missing proc accepted")
	}
}
