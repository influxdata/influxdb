package influxdb

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/ast/edit"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/flux/values"
	"github.com/influxdata/influxdb/task/options"
)

const (
	TaskDefaultPageSize = 100
	TaskMaxPageSize     = 500
)

// Task is a task. 🎊
type Task struct {
	ID              ID     `json:"id,omitempty"`
	OrganizationID  ID     `json:"orgID"`
	Organization    string `json:"org"`
	Name            string `json:"name"`
	Status          string `json:"status"`
	Owner           User   `json:"owner"`
	Flux            string `json:"flux"`
	Every           string `json:"every,omitempty"`
	Cron            string `json:"cron,omitempty"`
	Offset          string `json:"offset,omitempty"`
	LatestCompleted string `json:"latest_completed,omitempty"`
}

// Run is a record created when a run of a task is scheduled.
type Run struct {
	ID           ID     `json:"id,omitempty"`
	TaskID       ID     `json:"taskID"`
	Status       string `json:"status"`
	ScheduledFor string `json:"scheduledFor"`
	StartedAt    string `json:"startedAt,omitempty"`
	FinishedAt   string `json:"finishedAt,omitempty"`
	RequestedAt  string `json:"requestedAt,omitempty"`
	Log          Log    `json:"log"`
}

// Log represents a link to a log resource
type Log string

// TaskService represents a service for managing one-off and recurring tasks.
type TaskService interface {
	// FindTaskByID returns a single task
	FindTaskByID(ctx context.Context, id ID) (*Task, error)

	// FindTasks returns a list of tasks that match a filter (limit 100) and the total count
	// of matching tasks.
	FindTasks(ctx context.Context, filter TaskFilter) ([]*Task, int, error)

	// CreateTask creates a new task.
	CreateTask(ctx context.Context, t *Task) error

	// UpdateTask updates a single task with changeset.
	UpdateTask(ctx context.Context, id ID, upd TaskUpdate) (*Task, error)

	// DeleteTask removes a task by ID and purges all associated data and scheduled runs.
	DeleteTask(ctx context.Context, id ID) error

	// FindLogs returns logs for a run.
	FindLogs(ctx context.Context, filter LogFilter) ([]*Log, int, error)

	// FindRuns returns a list of runs that match a filter and the total count of returned runs.
	FindRuns(ctx context.Context, filter RunFilter) ([]*Run, int, error)

	// FindRunByID returns a single run.
	FindRunByID(ctx context.Context, taskID, runID ID) (*Run, error)

	// CancelRun cancels a currently running run.
	CancelRun(ctx context.Context, taskID, runID ID) error

	// RetryRun creates and returns a new run (which is a retry of another run).
	RetryRun(ctx context.Context, taskID, runID ID) (*Run, error)

	// ForceRun forces a run to occur with unix timestamp scheduledFor, to be executed as soon as possible.
	// The value of scheduledFor may or may not align with the task's schedule.
	ForceRun(ctx context.Context, taskID ID, scheduledFor int64) (*Run, error)
}

// TaskUpdate represents updates to a task. Options updates override any options set in the Flux field.
type TaskUpdate struct {
	Flux   *string `json:"flux,omitempty"`
	Status *string `json:"status,omitempty"`
	// Options gets unmarshalled from json as if it was flat, with the same level as Flux and Status.
	Options options.Options // when we unmarshal this gets unmarshalled from flat key-values
}

func (t *TaskUpdate) UnmarshalJSON(data []byte) error {
	// this is a type so we can marshal string into durations nicely
	jo := struct {
		Flux   *string `json:"flux,omitempty"`
		Status *string `json:"status,omitempty"`
		Name   string  `json:"options,omitempty"`

		// Cron is a cron style time schedule that can be used in place of Every.
		Cron string `json:"cron,omitempty"`

		// Every represents a fixed period to repeat execution.
		// It gets marshalled from a string duration, i.e.: "10s" is 10 seconds
		Every flux.Duration `json:"every,omitempty"`

		// Offset represents a delay before execution.
		// It gets marshalled from a string duration, i.e.: "10s" is 10 seconds
		Offset flux.Duration `json:"offset,omitempty"`

		Concurrency int64 `json:"concurrency,omitempty"`

		Retry int64 `json:"retry,omitempty"`
	}{}

	if err := json.Unmarshal(data, &jo); err != nil {
		return err
	}
	t.Options.Name = jo.Name
	t.Options.Cron = jo.Cron
	t.Options.Every = time.Duration(jo.Every)
	t.Options.Offset = time.Duration(jo.Offset)
	t.Options.Concurrency = jo.Concurrency
	t.Options.Retry = jo.Retry
	t.Flux = jo.Flux
	t.Status = jo.Status

	return nil
}

func (t TaskUpdate) MarshalJSON() ([]byte, error) {
	jo := struct {
		Flux   *string `json:"flux,omitempty"`
		Status *string `json:"status,omitempty"`
		Name   string  `json:"options,omitempty"`

		// Cron is a cron style time schedule that can be used in place of Every.
		Cron string `json:"cron,omitempty"`

		// Every represents a fixed period to repeat execution.
		Every flux.Duration `json:"every,omitempty"`

		// Offset represents a delay before execution.
		Offset flux.Duration `json:"offset,omitempty"`

		Concurrency int64 `json:"concurrency,omitempty"`

		Retry int64 `json:"retry,omitempty"`
	}{}
	jo.Name = t.Options.Name
	jo.Cron = t.Options.Cron
	jo.Every = flux.Duration(t.Options.Every)
	jo.Offset = flux.Duration(t.Options.Offset)
	jo.Concurrency = t.Options.Concurrency
	jo.Retry = t.Options.Retry
	jo.Flux = t.Flux
	jo.Status = t.Status
	return json.Marshal(jo)
}

func (t TaskUpdate) Validate() error {
	switch {
	case t.Options.Every != 0 && t.Options.Cron != "":
		return errors.New("cannot specify both every and cron")
	case t.Flux == nil && t.Status == nil && t.Options.IsZero():
		return errors.New("cannot update task without content")
	}
	return nil
}

// UpdateFlux updates the TaskUpdate to go from updating options to updating a flux string, that now has those updated options in it
// It zeros the options in the TaskUpdate.
func (t *TaskUpdate) UpdateFlux(oldFlux string) error {
	if t.Flux != nil {
		return nil
	}
	parsedPKG := parser.ParseSource(oldFlux)
	if ast.Check(parsedPKG) > 0 {
		return ast.GetError(parsedPKG)
	}
	parsed := parsedPKG.Files[0] //TODO: remove this line when flux 0.14 is upgraded into platform
	if t.Options.Every != 0 && t.Options.Cron != "" {
		return errors.New("cannot specify both every and cron")
	}
	// so we don't allocate if we are just changing the status
	if t.Options.Name != "" || t.Options.Every != 0 || t.Options.Cron != "" || t.Options.Offset != 0 {
		op := make(map[string]values.Value, 4)

		if t.Options.Name != "" {
			op["name"] = values.NewString(t.Options.Name)
		}
		if t.Options.Every != 0 {
			op["every"] = values.NewDuration(values.Duration(t.Options.Every))
		}
		if t.Options.Cron != "" {
			op["cron"] = values.NewString(t.Options.Cron)
		}
		if t.Options.Offset != 0 {
			op["offset"] = values.NewDuration(values.Duration(t.Options.Offset))
		}
		ok, err := edit.Option(parsed, "task", edit.OptionObjectFn(op))
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("unable to edit option")
		}
		t.Options.Clear()
		s := ast.Format(parsed)
		t.Flux = &s
		return nil
	}
	return nil
}

// TaskFilter represents a set of filters that restrict the returned results
type TaskFilter struct {
	After        *ID
	Organization *ID
	User         *ID
	Limit        int
}

// RunFilter represents a set of filters that restrict the returned results
type RunFilter struct {
	Org        *ID
	Task       *ID
	After      *ID
	Limit      int
	AfterTime  string
	BeforeTime string
}

// LogFilter represents a set of filters that restrict the returned results
type LogFilter struct {
	Org  *ID
	Task *ID
	Run  *ID
}
