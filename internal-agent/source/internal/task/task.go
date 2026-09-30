package task

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"io.translab/tantor-agent/internal/client"
	"io.translab/tantor-agent/internal/collect"
	"io.translab/tantor-agent/internal/config"
	"io.translab/tantor-agent/internal/deploy"
	"io.translab/tantor-agent/pkg/api"
)

const maxCachedResults = 1024

// Engine handles registration, heartbeat, task polling, and serialized task execution.
type Engine struct {
	cfg          *config.Config
	client       *client.APIClient
	collector    *collect.Collector
	deployEngine *deploy.Engine

	mu             sync.Mutex
	busy           bool
	completed      map[string]*api.TaskResult
	completedOrder []string
}

func NewEngine(cfg *config.Config, c *client.APIClient, col *collect.Collector, deployEngine *deploy.Engine) *Engine {
	return &Engine{
		cfg:          cfg,
		client:       c,
		collector:    col,
		deployEngine: deployEngine,
		completed:    make(map[string]*api.TaskResult),
	}
}

func (e *Engine) Start(ctx context.Context) {
	registered := e.register()
	pollTicker := time.NewTicker(time.Duration(e.cfg.Agent.PollInterval) * time.Second)
	heartbeatTicker := time.NewTicker(time.Duration(e.cfg.Agent.HeartbeatInterval) * time.Second)
	defer pollTicker.Stop()
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Task engine shutting down")
			return
		case <-heartbeatTicker.C:
			if !registered {
				registered = e.register()
				continue
			}
			if err := e.sendHeartbeat(); err != nil && strings.Contains(err.Error(), "404") {
				registered = false
			}
		case <-pollTicker.C:
			if !registered {
				registered = e.register()
				if !registered {
					continue
				}
			}
			e.pollTasks(ctx)
		}
	}
}

func (e *Engine) register() bool {
	if err := e.client.RegisterHost(e.collector.GetRegistration()); err != nil {
		slog.Warn("Failed to register host; retrying on the next cycle", "err", err)
		return false
	}
	slog.Info("Host successfully registered with management server")
	return true
}

func (e *Engine) sendHeartbeat() error {
	err := e.client.SendHeartbeat(e.collector.GetHeartbeat())
	if err != nil {
		slog.Warn("Failed to send heartbeat", "err", err)
		if strings.Contains(err.Error(), "404") {
			slog.Info("Management server no longer recognizes this host; registration will be retried")
		}
	}
	return err
}

func (e *Engine) pollTasks(ctx context.Context) {
	e.mu.Lock()
	if e.busy {
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()

	tasks, err := e.client.PollTasks()
	if err != nil {
		slog.Error("Failed to poll tasks", "err", err)
		return
	}
	if len(tasks) == 0 {
		return
	}

	e.mu.Lock()
	if e.busy {
		e.mu.Unlock()
		return
	}
	e.busy = true
	e.mu.Unlock()

	for _, t := range tasks {
		slog.Info("Received task", "taskId", t.TaskID, "command", t.Command)
	}
	go e.executeTasks(ctx, tasks)
}

func (e *Engine) executeTasks(ctx context.Context, tasks []api.Task) {
	defer func() {
		e.mu.Lock()
		e.busy = false
		e.mu.Unlock()
	}()

	for _, t := range tasks {
		select {
		case <-ctx.Done():
			return
		default:
		}
		e.executeTask(ctx, t)
	}
}

func (e *Engine) executeTask(ctx context.Context, t api.Task) {
	if cached := e.cachedResult(t.TaskID); cached != nil {
		// A redelivery may carry a new claim after the previous lease expired.
		cached.ClaimToken = t.ClaimToken
		slog.Warn("Task ID was already completed in this agent process; re-reporting cached result instead of executing twice", "taskId", t.TaskID)
		if err := e.client.ReportTaskResult(cached); err != nil {
			slog.Error("Failed to re-report cached task result", "taskId", t.TaskID, "err", err)
		}
		return
	}

	if err := e.client.ReportTaskResult(&api.TaskResult{
		TaskID:     t.TaskID,
		HostID:     e.cfg.Agent.HostID,
		Status:     "RUNNING",
		ClaimToken: t.ClaimToken,
	}); err != nil {
		slog.Error("Failed to report RUNNING status", "taskId", t.TaskID, "err", err)
	}

	result, err := e.deployEngine.Execute(ctx, &t)
	if err != nil {
		slog.Error("Task execution failed with system error", "taskId", t.TaskID, "err", err)
	}
	if result == nil {
		result = &api.TaskResult{
			TaskID:   t.TaskID,
			HostID:   e.cfg.Agent.HostID,
			Status:   "FAILED",
			ErrorMsg: "task execution returned no result",
		}
		if err != nil {
			result.ErrorMsg = err.Error()
		}
	}

	result.ClaimToken = t.ClaimToken
	e.cacheResult(result)
	slog.Info("Task executed", "taskId", t.TaskID, "status", result.Status)
	if err := e.client.ReportTaskResult(result); err != nil {
		slog.Error("Failed to report task result; the result is cached for duplicate task re-delivery", "taskId", t.TaskID, "err", err)
	} else {
		slog.Info("Task result report accepted by server", "taskId", t.TaskID, "status", result.Status)
	}
}

func (e *Engine) cachedResult(taskID string) *api.TaskResult {
	if strings.TrimSpace(taskID) == "" {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	result := e.completed[taskID]
	if result == nil {
		return nil
	}
	copy := *result
	return &copy
}

func (e *Engine) cacheResult(result *api.TaskResult) {
	if result == nil || strings.TrimSpace(result.TaskID) == "" {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	copy := *result
	if _, exists := e.completed[result.TaskID]; !exists {
		e.completedOrder = append(e.completedOrder, result.TaskID)
	}
	e.completed[result.TaskID] = &copy
	for len(e.completedOrder) > maxCachedResults {
		oldest := e.completedOrder[0]
		e.completedOrder = e.completedOrder[1:]
		delete(e.completed, oldest)
	}
}
