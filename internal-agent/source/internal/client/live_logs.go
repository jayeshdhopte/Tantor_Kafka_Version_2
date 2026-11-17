package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os/exec"
	"regexp"
	"time"
)

var logTokenPattern = regexp.MustCompile("^[a-f0-9]{64}$")
var logUnitPattern = regexp.MustCompile(`^(broker|controller|kafka|tantor-(broker|controller|kafka(-broker|-controller)?)(-[0-9]+)?|kafka-[0-9]+)\.service$`)
var logHostPattern = regexp.MustCompile("^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
var logIDPattern = regexp.MustCompile("^[a-f0-9-]{36}$")

const maxJournalBytes = 512 * 1024

type journalQuery struct {
	ID   string    `json:"id"`
	Unit string    `json:"unit"`
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

func (c *APIClient) RunLiveLogs(ctx context.Context, token string) {
	if !logTokenPattern.MatchString(token) || !logHostPattern.MatchString(c.cfg.Agent.HostID) ||
		c.baseURL.Scheme != "https" || c.cfg.Agent.InsecureSkipVerify {
		return
	}
	endpoint, err := c.endpoint("/api/v1/agents/" + c.cfg.Agent.HostID + "/live-logs")
	if err != nil {
		return
	}
	// Keep the per-agent secret on the configured origin, including on redirects.
	client := *c.httpClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.pollJournal(ctx, &client, endpoint, token)
		}
	}
}

func (c *APIClient) pollJournal(ctx context.Context, client *http.Client, endpoint, token string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return
	}
	req.Header.Set("X-Tantor-Log-Token", token)
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return
	}
	var query journalQuery
	if json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&query) != nil || !logIDPattern.MatchString(query.ID) {
		return
	}
	logs, readErr := readJournal(ctx, query)
	report, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/"+query.ID, bytes.NewReader(logs))
	if err != nil {
		return
	}
	report.Header.Set("X-Tantor-Log-Token", token)
	report.Header.Set("Content-Type", "application/octet-stream")
	if readErr != nil {
		report.Header.Set("X-Tantor-Log-Error", "true")
	}
	if reply, err := client.Do(report); err == nil {
		reply.Body.Close()
	}
}

func journalArgs(query journalQuery) ([]string, error) {
	if !logUnitPattern.MatchString(query.Unit) || query.From.IsZero() || query.To.IsZero() ||
		query.To.Before(query.From) || query.To.Sub(query.From) > 7*24*time.Hour {
		return nil, errors.New("invalid service journal query")
	}
	args := []string{"--no-pager", "--quiet", "--output=json", "--output-fields=MESSAGE,PRIORITY",
		"--lines=200", "--unit=" + query.Unit, "--since=" + query.From.UTC().Format("2006-01-02 15:04:05.000000 UTC")}
	return append(args, "--until="+query.To.UTC().Format("2006-01-02 15:04:05.000000 UTC")), nil
}

func readJournal(ctx context.Context, query journalQuery) ([]byte, error) {
	args, err := journalArgs(query)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "journalctl", args...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		return nil, err
	}
	logs, readErr := io.ReadAll(io.LimitReader(out, maxJournalBytes+1))
	if readErr != nil || len(logs) > maxJournalBytes {
		cancel()
		_ = cmd.Wait()
		return nil, errors.New("journal output exceeds the read limit")
	}
	return logs, cmd.Wait()
}
