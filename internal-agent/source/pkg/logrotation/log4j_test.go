package logrotation

import (
	"encoding/xml"
	"strings"
	"testing"
)

func TestLog4jPolicyHasDailyCompressedArchivesAndScopedSevenDayRetention(t *testing.T) {
	body := Log4j2("/client/logs/test&application", "broker.log")
	var parsed struct {
		XMLName   xml.Name
		Appenders struct {
			File struct {
				Path     string `xml:"fileName,attr"`
				Pattern  string `xml:"filePattern,attr"`
				Policies struct {
					Cron struct {
						Schedule string `xml:"schedule,attr"`
					} `xml:"CronTriggeringPolicy"`
				} `xml:"Policies"`
				Strategy struct {
					Delete struct {
						Path     string `xml:"basePath,attr"`
						Filename struct {
							Glob string `xml:"glob,attr"`
						} `xml:"IfFileName"`
						Age struct {
							Age string `xml:"age,attr"`
						} `xml:"IfLastModified"`
					} `xml:"Delete"`
				} `xml:"DefaultRolloverStrategy"`
			} `xml:"RollingFile"`
		} `xml:"Appenders"`
	}
	if err := xml.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatal(err)
	}
	f := parsed.Appenders.File
	if f.Path != "/client/logs/test&application/broker.log" || !strings.HasSuffix(f.Pattern, "broker.log.%d{yyyy-MM-dd}.%i.gz") || f.Policies.Cron.Schedule != "0 0 0 * * ?" {
		t.Fatal(body)
	}
	if f.Strategy.Delete.Path != "/client/logs/test&application/archive" || f.Strategy.Delete.Filename.Glob != "broker.log.*.gz" || f.Strategy.Delete.Age.Age != "7d" {
		t.Fatal(body)
	}
	if strings.Contains(body, "Console") {
		t.Fatal("duplicate console appender")
	}
}
