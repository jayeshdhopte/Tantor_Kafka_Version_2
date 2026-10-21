package dataservice

import (
	"strings"
	"testing"
)

func TestNamedInstancesUseDistinctSystemdUnits(t *testing.T) {
	task := taskFor()
	task.Parameters["service_name"] = "kafka-connect-worker-a"
	a, err := Parse(Connect, task)
	if err != nil {
		t.Fatal(err)
	}
	task.Parameters["service_name"] = "kafka-connect-worker-b"
	b, err := Parse(Connect, task)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []Settings{a, b} {
		files := ConfigFiles(s, "/usr/lib/jvm/java-17")
		found := false
		for _, file := range files {
			if strings.HasPrefix(file.Path, "/etc/systemd/system/") {
				if file.Path != "/etc/systemd/system/"+s.ServiceName+".service" {
					t.Fatal(file.Path)
				}
				found = true
			}
		}
		if !found {
			t.Fatal("missing unit")
		}
	}
	for _, name := range []string{"broker", "../../evil", "kafka-connect-x.service", "kafka-connect-a;bad"} {
		task.Parameters["service_name"] = name
		if _, err = Parse(Connect, task); err == nil {
			t.Fatalf("accepted service %q", name)
		}
	}
}
