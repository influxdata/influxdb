package backend

// The tooling needed to correctly run go generate is managed by the Makefile.
// Run `make` from the project root to ensure these generate commands execute correctly.
//go:generate protoc -I ../../internal -I . --plugin ../../scripts/protoc-gen-gogofaster --gogofaster_out=plugins=grpc:. ./meta.proto

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/ast/edit"
	"github.com/influxdata/flux/parser"
	"github.com/influxdata/flux/values"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/task/options"
)

var (
	// ErrTaskNotFound indicates no task could be found for given parameters.
	ErrTaskNotFound = errors.New("task not found")

	// ErrUserNotFound is an error for when we can't find a user
	ErrUserNotFound = errors.New("user not found")

	// ErrOrgNotFound is an error for when we can't find an org
	ErrOrgNotFound = errors.New("org not found")

	// ErrManualQueueFull is returned when a manual run request cannot be completed.
	ErrManualQueueFull = errors.New("manual queue at capacity")

	// ErrRunNotFound is returned when searching for a run that doesn't exist.
	ErrRunNotFound = errors.New("run not found")

	// ErrRunNotFinished is returned when a retry is invalid due to the run not being finished yet.
	ErrRunNotFinished = errors.New("run is still in progress")
)

type TaskStatus string

const (
	TaskActive   TaskStatus = "active"
	TaskInactive TaskStatus = "inactive"

	DefaultTaskStatus TaskStatus = TaskActive
)

// validate returns an error if s is not a known task status.
func (s TaskStatus) validate(allowEmpty bool) error {
	if allowEmpty && s == "" {
		return nil
	}

	if s == TaskActive || s == TaskInactive {
		return nil
	}

	return fmt.Errorf("invalid task status: %q", s)
}

type RunStatus int

const (
	RunStarted RunStatus = iota
	RunSuccess
	RunFail
	RunCanceled
	RunScheduled
)

func (r RunStatus) String() string {
	switch r {
	case RunStarted:
		return "started"
	case RunSuccess:
		return "success"
	case RunFail:
		return "failed"
	case RunCanceled:
		return "canceled"
	case RunScheduled:
		return "scheduled"
	}
	panic(fmt.Sprintf("unknown RunStatus: %d", r))
}

// RunNotYetDueError is returned from CreateNextRun if a run is not yet due.
type RunNotYetDueError struct {
	// DueAt is the unix timestamp of when the next run is due.
	DueAt int64
}

func (e RunNotYetDueError) Error() string {
	return "run not due until " + time.Unix(e.DueAt, 0).UTC().Format(time.RFC3339)
}

// RequestStillQueuedError is returned when attempting to retry a run which has not yet completed.
type RequestStillQueuedError struct {
	// Unix timestamps matching existing request's start and end.
	Start, End int64
}

const fmtRequestStillQueued = "previous retry for start=%s end=%s has not yet finished"

func (e RequestStillQueuedError) Error() string {
	return fmt.Sprintf(fmtRequestStillQueued,
		time.Unix(e.Start, 0).UTC().Format(time.RFC3339),
		time.Unix(e.End, 0).UTC().Format(time.RFC3339),
	)
}

// ParseRequestStillQueuedError attempts to parse a RequestStillQueuedError from msg.
// If msg is formatted correctly, the resultant error is returned; otherwise it returns nil.
func ParseRequestStillQueuedError(msg string) *RequestStillQueuedError {
	var s, e string
	n, err := fmt.Sscanf(msg, fmtRequestStillQueued, &s, &e)
	if err != nil || n != 2 {
		return nil
	}

	start, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil
	}

	end, err := time.Parse(time.RFC3339, e)
	if err != nil {
		return nil
	}

	return &RequestStillQueuedError{Start: start.Unix(), End: end.Unix()}
}

// RunCreation is returned by CreateNextRun.
type RunCreation struct {
	Created QueuedRun

	// Unix timestamp for when the next run is due.
	NextDue int64

	// Whether there are any manual runs queued for this task.
	// If so, the scheduler should begin executing them after handling real-time tasks.
	HasQueue bool
}

// CreateTaskRequest encapsulates state of a new task to be created.
type CreateTaskRequest struct {
	// Owners.
	Org, User platform.ID

	// Script content of the task.
	Script string

	// Unix timestamp (seconds elapsed since January 1, 1970 UTC).
	// The first run of the task will be run according to the earliest time after ScheduleAfter,
	// matching the task's schedul via its cron or every option.
	ScheduleAfter int64

	// The initial task status.
	// If empty, will be treated as DefaultTaskStatus.
	Status TaskStatus
}

// UpdateTaskRequest encapsulates requested changes to a task.
type UpdateTaskRequest struct {
	// ID of the task.
	ID platform.ID

	// New script content of the task.
	// If empty, do not modify the existing script.
	Script string

	// The new desired task status.
	// If empty, do not modify the existing status.
	Status TaskStatus

	// These options are for editing options via request.  Zeroed options will be ignored.
	options.Options
}

// UpdateFlux updates the taskupdate to go from updating options to updating a flux string, that now has those updated options in it
// It zeros the options in the TaskUpdate.
func (t *UpdateTaskRequest) UpdateFlux(oldFlux string) error {
	if t.Script != "" {
		oldFlux = t.Script
	}
	parsedPKG := parser.ParseSource(oldFlux)
	if ast.Check(parsedPKG) > 0 {
		return ast.GetError(parsedPKG)
	}
	parsed := parsedPKG.Files[0] //TODO: remove this line when flux 0.14 is upgraded into platform
	// so we don't allocate if we are just changing the status
	if t.Every != 0 && t.Cron != "" {
		return errors.New("cannot specify both every and cron")
	}
	if t.Name != "" || !t.IsZero() {
		op := make(map[string]values.Value, 5)
		if t.Name != "" {
			op["name"] = values.NewString(t.Name)
		}
		if t.Every != 0 {
			op["every"] = values.NewDuration(values.Duration(t.Every))
		}
		if t.Cron != "" {
			op["cron"] = values.NewString(t.Cron)
		}
		if t.Offset != 0 {
			op["offset"] = values.NewDuration(values.Duration(t.Offset))
		}
		if t.Concurrency != 0 {
			op["concurrency"] = values.NewInt(t.Concurrency)
		}
		if t.Retry != 0 {
			op["retry"] = values.NewInt(t.Retry)
		}
		ok, err := edit.Option(parsed, "task", edit.OptionObjectFn(op))
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("unable to edit option")
		}
		t.Options.Clear()
		t.Script = ast.Format(parsed)
		return nil
	}
	return nil
}

// UpdateTaskResult describes the result of modifying a single task.
// Having the content returned from ModifyTask makes it much simpler for callers
// to decide how to notify on status changes, etc.
type UpdateTaskResult struct {
	OldScript string
	OldStatus TaskStatus

	NewTask StoreTask
	NewMeta StoreTaskMeta
}

// Store is the interface around persisted tasks.
type Store interface {
	// CreateTask creates a task with from the given CreateTaskRequest.
	// If the task is created successfully, the ID of the new task is returned.
	CreateTask(ctx context.Context, req CreateTaskRequest) (platform.ID, error)

	// UpdateTask updates an existing task.
	// It returns an error if there was no task matching the given ID.
	// If the returned error is not nil, the returned result should not be inspected.
	UpdateTask(ctx context.Context, req UpdateTaskRequest) (UpdateTaskResult, error)

	// ListTasks lists the tasks in the store that match the search params.
	ListTasks(ctx context.Context, params TaskSearchParams) ([]StoreTaskWithMeta, error)

	// FindTaskByID returns the task with the given ID.
	// If no task matches the ID, the returned task is nil and ErrTaskNotFound is returned.
	FindTaskByID(ctx context.Context, id platform.ID) (*StoreTask, error)

	// FindTaskMetaByID returns the metadata about a task.
	// If no task meta matches the ID, the returned meta is nil and ErrTaskNotFound is returned.
	FindTaskMetaByID(ctx context.Context, id platform.ID) (*StoreTaskMeta, error)

	// FindTaskByIDWithMeta combines finding the task and the meta into a single call.
	FindTaskByIDWithMeta(ctx context.Context, id platform.ID) (*StoreTask, *StoreTaskMeta, error)

	// DeleteTask returns whether an entry matching the given ID was deleted.
	// If err is non-nil, deleted is false.
	// If err is nil, deleted is false if no entry matched the ID,
	// or deleted is true if there was a matching entry and it was deleted.
	DeleteTask(ctx context.Context, id platform.ID) (deleted bool, err error)

	// CreateNextRun creates the earliest needed run scheduled no later than the given Unix timestamp now.
	// Internally, the Store should rely on the underlying task's StoreTaskMeta to create the next run.
	CreateNextRun(ctx context.Context, taskID platform.ID, now int64) (RunCreation, error)

	// FinishRun removes runID from the list of running tasks and if its `now` is later then last completed update it.
	FinishRun(ctx context.Context, taskID, runID platform.ID) error

	// ManuallyRunTimeRange enqueues a request to run the task with the given ID for all schedules no earlier than start and no later than end (Unix timestamps).
	// requestedAt is the Unix timestamp when the request was initiated.
	// ManuallyRunTimeRange must delegate to an underlying StoreTaskMeta's ManuallyRunTimeRange method.
	ManuallyRunTimeRange(ctx context.Context, taskID platform.ID, start, end, requestedAt int64) (*StoreTaskMetaManualRun, error)

	// DeleteOrg deletes the org.
	DeleteOrg(ctx context.Context, orgID platform.ID) error

	// DeleteUser deletes a user with userID.
	DeleteUser(ctx context.Context, userID platform.ID) error

	// Close closes the store for usage and cleans up running processes.
	Close() error
}

// RunLogBase is the base information for a logs about an individual run.
type RunLogBase struct {
	// The parent task that owns the run.
	Task *StoreTask

	// The ID of the run.
	RunID platform.ID

	// The Unix timestamp indicating the run's scheduled time.
	RunScheduledFor int64

	// When the log is requested, should be ignored when it is zero.
	RequestedAt int64
}

// LogWriter writes task logs and task state changes to a store.
type LogWriter interface {
	// UpdateRunState sets the run state and the respective time.
	UpdateRunState(ctx context.Context, base RunLogBase, when time.Time, state RunStatus) error

	// AddRunLog adds a log line to the run.
	AddRunLog(ctx context.Context, base RunLogBase, when time.Time, log string) error
}

// NopLogWriter is a LogWriter that doesn't do anything when its methods are called.
// This is useful for test, but not much else.
type NopLogWriter struct{}

func (NopLogWriter) UpdateRunState(context.Context, RunLogBase, time.Time, RunStatus) error {
	return nil
}

func (NopLogWriter) AddRunLog(context.Context, RunLogBase, time.Time, string) error {
	return nil
}

// LogReader reads log information and log data from a store.
type LogReader interface {
	// ListRuns returns a list of runs belonging to a task.
	ListRuns(ctx context.Context, runFilter platform.RunFilter) ([]*platform.Run, error)

	// FindRunByID finds a run given a orgID and runID.
	// orgID is necessary to look in the correct system bucket.
	FindRunByID(ctx context.Context, orgID, runID platform.ID) (*platform.Run, error)

	// ListLogs lists logs for a task or a specified run of a task.
	ListLogs(ctx context.Context, logFilter platform.LogFilter) ([]platform.Log, error)
}

// NopLogWriter is a LogWriter that doesn't do anything when its methods are called.
// This is useful for test, but not much else.
type NopLogReader struct{}

func (NopLogReader) ListRuns(ctx context.Context, runFilter platform.RunFilter) ([]*platform.Run, error) {
	return nil, nil
}

func (NopLogReader) FindRunByID(ctx context.Context, orgID, runID platform.ID) (*platform.Run, error) {
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

// StoreTaskWithMeta is a single struct with a StoreTask and a StoreTaskMeta.
type StoreTaskWithMeta struct {
	Task StoreTask
	Meta StoreTaskMeta
}

// StoreValidator is a package-level StoreValidation, so that you can write
//    backend.StoreValidator.CreateArgs(...)
var StoreValidator StoreValidation

// StoreValidation is used for namespacing the store validation methods.
type StoreValidation struct{}

// CreateArgs returns the script's parsed options,
// and an error if any of the provided fields are invalid for creating a task.
func (StoreValidation) CreateArgs(req CreateTaskRequest) (options.Options, error) {
	var missing []string
	var o options.Options

	if req.Script == "" {
		missing = append(missing, "script")
	} else {
		var err error
		o, err = options.FromScript(req.Script)
		if err != nil {
			return o, err
		}
	}

	if !req.Org.Valid() {
		missing = append(missing, "organization ID")
	}
	if !req.User.Valid() {
		missing = append(missing, "user ID")
	}

	if len(missing) > 0 {
		return o, fmt.Errorf("missing required fields to create task: %s", strings.Join(missing, ", "))
	}

	if err := req.Status.validate(true); err != nil {
		return o, err
	}

	return o, nil
}

// UpdateArgs validates the UpdateTaskRequest.
// If the update only includes a new status (i.e. req.Script is empty), the returned options are zero.
// If the update contains neither a new script nor a new status, or if the script is invalid, an error is returned.
func (StoreValidation) UpdateArgs(req UpdateTaskRequest) (options.Options, error) {
	var missing []string
	o := req.Options
	if req.Script == "" && req.Status == "" && req.Options.IsZero() {
		missing = append(missing, "script or status or options")
	}
	if req.Script != "" {
		err := req.UpdateFlux(req.Script)
		if err != nil {
			return o, err
		}
		req.Clear()
		o, err = options.FromScript(req.Script)
		if err != nil {
			return o, err
		}
	}
	if err := req.Status.validate(true); err != nil {
		return o, err
	}

	if !req.ID.Valid() {
		missing = append(missing, "task ID")
	}

	if len(missing) > 0 {
		return o, fmt.Errorf("missing required fields to modify task: %s", strings.Join(missing, ", "))
	}

	return o, nil
}
