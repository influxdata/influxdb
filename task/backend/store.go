package backend

// The tooling needed to correctly run go generate is managed by the Makefile.
// Run `make` from the project root to ensure these generate commands execute correctly.
//go:generate protoc -I ../../vendor -I . --plugin ../../bin/${GOOS}/protoc-gen-gogofaster --gogofaster_out=plugins=grpc:. ./meta.proto

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/platform"
	"github.com/influxdata/platform/task/options"
)

// ErrUserNotFound is an error for when we can't find a user
var ErrUserNotFound = errors.New("user not found")

// ErrOrgNotFound is an error for when we can't find an org
var ErrOrgNotFound = errors.New("org not found")

// ErrTaskNameTaken is an error for when a task name is already taken
var ErrTaskNameTaken = errors.New("task name already in use by current user or target organization")

type TaskStatus string

const (
	TaskEnabled  TaskStatus = "enabled"
	TaskDisabled TaskStatus = "disabled"
)

type RunStatus int

const (
	RunScheduled RunStatus = iota
	RunStarted
	RunSuccess
	RunFail
	RunCanceled
)

func (r RunStatus) String() string {
	switch r {
	case RunScheduled:
		return "scheduled"
	case RunStarted:
		return "started"
	case RunSuccess:
		return "success"
	case RunFail:
		return "failed"
	case RunCanceled:
		return "canceled"
	}
	panic(fmt.Sprintf("unknown RunStatus: %d", r))
}

// Store is the interface around persisted tasks.
type Store interface {
	// CreateTask creates a task with the given script, belonging to the given org and user.
	// The scheduleAfter parameter is a Unix timestamp (seconds elapsed since January 1, 1970 UTC),
	// and the first run of the task will be run according to the earliest time after scheduleAfter,
	// matching the task's schedule via its cron or every option.
	CreateTask(ctx context.Context, org, user platform.ID, script string, scheduleAfter int64) (platform.ID, error)

	// ModifyTask updates the script of an existing task.
	// It returns an error if there was no task matching the given ID.
	ModifyTask(ctx context.Context, id platform.ID, newScript string) error

	// ListTasks lists the tasks in the store that match the search params.
	ListTasks(ctx context.Context, params TaskSearchParams) ([]StoreTask, error)

	// FindTaskByID returns the task with the given ID.
	// If no task matches the ID, the returned task is nil.
	FindTaskByID(ctx context.Context, id platform.ID) (*StoreTask, error)

	// EnableTask updates task status to enabled.
	EnableTask(ctx context.Context, id platform.ID) error

	// disableTask updates task status to disabled.
	DisableTask(ctx context.Context, id platform.ID) error

	// FindTaskMetaByID returns the metadata about a task.
	FindTaskMetaByID(ctx context.Context, id platform.ID) (*StoreTaskMeta, error)

	// DeleteTask returns whether an entry matching the given ID was deleted.
	// If err is non-nil, deleted is false.
	// If err is nil, deleted is false if no entry matched the ID,
	// or deleted is true if there was a matching entry and it was deleted.
	DeleteTask(ctx context.Context, id platform.ID) (deleted bool, err error)

	// CreateRun adds `now` to the task's metaData if we have not exceeded 'max_concurrency'.
	CreateRun(ctx context.Context, taskID platform.ID, now int64) (QueuedRun, error)

	// FinishRun removes runID from the list of running tasks and if its `now` is later then last completed update it.
	FinishRun(ctx context.Context, taskID, runID platform.ID) error

	// DeleteOrg deletes the org.
	DeleteOrg(ctx context.Context, orgID platform.ID) error

	// DeleteUser deletes a user with userID.
	DeleteUser(ctx context.Context, userID platform.ID) error

	// Close closes the store for usage and cleans up running processes.
	Close() error
}

// LogWriter writes task logs and task state changes to a store.
type LogWriter interface {
	// UpdateRunState sets the run state and the respective time.
	UpdateRunState(ctx context.Context, task *StoreTask, runID platform.ID, when time.Time, state RunStatus) error

	// AddRunLog adds a log line to the run.
	AddRunLog(ctx context.Context, task *StoreTask, runID platform.ID, when time.Time, log string) error
}

// NopLogWriter is a LogWriter that doesn't do anything when its methods are called.
// This is useful for test, but not much else.
type NopLogWriter struct{}

func (NopLogWriter) UpdateRunState(context.Context, *StoreTask, platform.ID, time.Time, RunStatus) error {
	return nil
}

func (NopLogWriter) AddRunLog(context.Context, *StoreTask, platform.ID, time.Time, string) error {
	return nil
}

// LogReader reads log information and log data from a store.
type LogReader interface {
	// ListRuns returns a list of runs belonging to a task.
	ListRuns(ctx context.Context, runFilter platform.RunFilter) ([]*platform.Run, error)

	// FindRunByID finds a run given a taskID and runID.
	FindRunByID(ctx context.Context, taskID, runID platform.ID) (*platform.Run, error)

	// ListLogs lists logs for a task or a specified run of a task.
	ListLogs(ctx context.Context, logFilter platform.LogFilter) ([]platform.Log, error)
}

// NopLogWriter is a LogWriter that doesn't do anything when its methods are called.
// This is useful for test, but not much else.
type NopLogReader struct{}

func (NopLogReader) ListRuns(ctx context.Context, runFilter platform.RunFilter) ([]*platform.Run, error) {
	return nil, nil
}

func (NopLogReader) FindRunByID(ctx context.Context, taskID, runID platform.ID) (*platform.Run, error) {
	return nil, nil
}

func (NopLogReader) ListLogs(ctx context.Context, logFilter platform.LogFilter) ([]platform.Log, error) {
	return nil, nil
}

// TaskSearchParams is used when searching or listing tasks.
type TaskSearchParams struct {
	// Return tasks belonging to this exact organization ID. May be nil.
	Org platform.ID

	// Return tasks belonging to this exact user ID. May be nil.
	User platform.ID

	// Return tasks starting after this ID.
	After platform.ID

	// Size of each page. Must be non-negative.
	// If zero, the implementation picks an appropriate default page size.
	// Valid page sizes are implementation-dependent.
	PageSize int
}

// StoreTask is a stored representation of a Task.
type StoreTask struct {
	ID platform.ID

	// IDs for the owning organization and user.
	Org, User platform.ID

	// The user-supplied name of the Task.
	Name string

	// The script content of the task.
	Script string
}

// StoreValidator is a package-level StoreValidation, so that you can write
//    backend.StoreValidator.CreateArgs(...)
var StoreValidator StoreValidation

// StoreValidation is used for namespacing the store validation methods.
type StoreValidation struct{}

// CreateArgs returns the script's parsed options,
// and an error if any of the provided fields are invalid for creating a task.
func (StoreValidation) CreateArgs(org, user platform.ID, script string) (options.Options, error) {
	var missing []string
	var o options.Options

	if script == "" {
		missing = append(missing, "script")
	} else {
		var err error
		o, err = options.FromScript(script)
		if err != nil {
			return o, err
		}
	}

	if len(org) == 0 {
		missing = append(missing, "organization ID")
	}
	if len(user) == 0 {
		missing = append(missing, "user ID")
	}

	if len(missing) > 0 {
		return o, fmt.Errorf("missing required fields to create task: %s", strings.Join(missing, ", "))
	}

	return o, nil
}

// ModifyArgs returns the script's parsed options,
// and an error if any of the provided fields are invalid for modifying a task.
func (StoreValidation) ModifyArgs(taskID platform.ID, script string) (options.Options, error) {
	var missing []string
	var o options.Options

	if script == "" {
		missing = append(missing, "script")
	} else {
		var err error
		o, err = options.FromScript(script)
		if err != nil {
			return o, err
		}
	}

	if len(taskID) == 0 {
		missing = append(missing, "task ID")
	}

	if len(missing) > 0 {
		return o, fmt.Errorf("missing required fields to modify task: %s", strings.Join(missing, ", "))
	}

	return o, nil
}
