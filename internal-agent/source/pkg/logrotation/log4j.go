package logrotation

import (
	"encoding/xml"
	"fmt"
	"strings"
)

// Log4j2 writes one application log, rolls at midnight into gzip archives and
// removes managed archives older than seven days. Kafka partition logs are unrelated.
func Log4j2(dir, name string) string {
	return log4j2(dir, name, false)
}

// KafkaLog4j2 also sends application events to stdout, which the foreground
// Kafka systemd service captures for journalctl and systemctl status.
func KafkaLog4j2(dir, name string) string {
	return log4j2(dir, name, true)
}

func log4j2(dir, name string, console bool) string {
	escape := func(s string) string { var b strings.Builder; xml.EscapeText(&b, []byte(s)); return b.String() }
	dir = escape(dir)
	name = escape(name)
	consoleAppender, consoleRef := "", ""
	if console {
		consoleAppender = `<Console name="Journal" target="SYSTEM_OUT"><PatternLayout pattern="%d{ISO8601} %-5p %c - %m%n"/></Console>`
		consoleRef = `<AppenderRef ref="Journal"/>`
	}
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<Configuration status="WARN">
 <Appenders><RollingFile name="Application" fileName="%s/%s" filePattern="%s/archive/%s.%%d{yyyy-MM-dd}.%%i.gz" immediateFlush="true">
  <PatternLayout pattern="%%d{ISO8601} %%-5p %%c - %%m%%n"/>
  <Policies><CronTriggeringPolicy schedule="0 0 0 * * ?" evaluateOnStartup="true"/></Policies>
  <DefaultRolloverStrategy><Delete basePath="%s/archive" maxDepth="1"><IfFileName glob="%s.*.gz"/><IfLastModified age="7d"/></Delete></DefaultRolloverStrategy>
 </RollingFile>%s</Appenders>
 <Loggers><Root level="info"><AppenderRef ref="Application"/>%s</Root></Loggers>
</Configuration>
`, dir, name, dir, name, dir, name, consoleAppender, consoleRef)
}
