package logrotation

import (
	"encoding/xml"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestKafkaPolicyRoutesRootEventsToJournalAndRetainedFile(t *testing.T) {
	var config struct {
		Appenders struct {
			Console []struct {
				Name   string `xml:"name,attr"`
				Target string `xml:"target,attr"`
			} `xml:"Console"`
			Files []struct {
				Name string `xml:"name,attr"`
			} `xml:"RollingFile"`
		} `xml:"Appenders"`
		Root struct {
			Refs []struct {
				Name string `xml:"ref,attr"`
			} `xml:"AppenderRef"`
		} `xml:"Loggers>Root"`
	}
	body := KafkaLog4j2("/client/logs/kafka", "broker.log")
	if err := xml.Unmarshal([]byte(body), &config); err != nil {
		t.Fatal(err)
	}
	if len(config.Appenders.Console) != 1 || config.Appenders.Console[0].Name != "Journal" || config.Appenders.Console[0].Target != "SYSTEM_OUT" || len(config.Appenders.Files) != 1 || len(config.Root.Refs) != 2 {
		t.Fatal(body)
	}
	refs := map[string]bool{}
	for _, ref := range config.Root.Refs {
		refs[ref.Name] = true
	}
	if !refs["Application"] || !refs["Journal"] || !strings.Contains(body, `age="7d"`) || !strings.Contains(body, `schedule="0 0 0 * * ?"`) {
		t.Fatal(body)
	}
}

// Set this to the selected Kafka distribution's Log4j API/core jars to exercise
// actual Log4j2 output without starting Kafka or touching a systemd service.
func TestKafkaLog4jRuntimeWritesConsoleAndFile(t *testing.T) {
	classpath := os.Getenv("TANTOR_TEST_LOG4J_CLASSPATH")
	if classpath == "" {
		t.Skip("Log4j runtime jars not configured")
	}
	dir := t.TempDir()
	config := filepath.Join(dir, "log4j2.xml")
	if err := os.WriteFile(config, []byte(KafkaLog4j2(dir, "broker.log")), 0600); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(dir, "JournalProbe.java")
	if err := os.WriteFile(source, []byte(`import org.apache.logging.log4j.LogManager;
class JournalProbe { public static void main(String[] args) {
 var log=LogManager.getLogger("kafka.tantor.JournalProbe");
 log.info("tantor-journal-info");log.warn("tantor-journal-warn");log.error("tantor-journal-error");log.debug("tantor-journal-debug");LogManager.shutdown();
}}`), 0600); err != nil {
		t.Fatal(err)
	}
	stdout, err := exec.Command("java", "-Dlog4j2.configurationFile="+config, "--class-path", classpath, source).CombinedOutput()
	if err != nil {
		t.Fatalf("Log4j probe: %v\n%s", err, stdout)
	}
	file, err := os.ReadFile(filepath.Join(dir, "broker.log"))
	if err != nil {
		t.Fatal(err)
	}
	for _, level := range []string{"info", "warn", "error"} {
		marker := "tantor-journal-" + level
		if strings.Count(string(stdout), marker) != 1 || strings.Count(string(file), marker) != 1 {
			t.Fatalf("expected one %s event in each destination; stdout=%s file=%s", level, stdout, file)
		}
	}
	if strings.Contains(string(stdout), "tantor-journal-debug") || strings.Contains(string(file), "tantor-journal-debug") {
		t.Fatal("INFO root threshold changed")
	}
}
