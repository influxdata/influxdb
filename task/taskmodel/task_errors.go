package taskmodel

import (
	"fmt"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"
)

var (
	// ErrRunCanceled is returned from the RunResult when a Run is Canceled.  It is used mostly internally.
	ErrRunCanceled = &errors.Error{
		Code: errors.EInternal,
		Msg:  "run canceled",
	}

	// ErrTaskNotClaimed is returned when attempting to operate against a task that must be claimed but is not.
	ErrTaskNotClaimed = &errors.Error{
		Code: errors.EConflict,
		Msg:  "task not claimed",
	}

	// ErrTaskAlreadyClaimed is returned when attempting to operate against a task that must not be claimed but is.
	ErrTaskAlreadyClaimed = &errors.Error{
		Code: errors.EConflict,
		Msg:  "task already claimed",
	}

	// ErrNoRunsFound is returned when searching for a range of runs, but none are found.
	ErrNoRunsFound = &errors.Error{
		Code: errors.ENotFound,
		Msg:  "no matching runs found",
	}

	// ErrInvalidTaskID error object for bad id's
	ErrInvalidTaskID = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "invalid id",
	}

	// ErrTaskNotFound indicates no task could be found for given parameters.
	ErrTaskNotFound = &errors.Error{
		Code: errors.ENotFound,
		Msg:  "task not found",
	}

	// ErrRunNotFound is returned when searching for a single run that doesn't exist.
	ErrRunNotFound = &errors.Error{
		Code: errors.ENotFound,
		Msg:  "run not found",
	}

	ErrRunKeyNotFound = &errors.Error{
		Code: errors.ENotFound,
		Msg:  "run key not found",
	}

	ErrPageSizeTooSmall = &errors.Error{
		Msg:  "cannot have negative page limit",
		Code: errors.EInvalid,
	}

	// ErrPageSizeTooLarge indicates the page size is too large. This error is only
	// used in the kv task service implementation. The name of this error may lead it
	// to be used in a place that is not useful. The TaskMaxPageSize is the only one
	// at 500, the rest at 100. This would likely benefit from a more specific name
	// since those limits aren't shared globally.
	ErrPageSizeTooLarge = &errors.Error{
		Msg:  fmt.Sprintf("cannot use page size larger then %d", TaskMaxPageSize),
		Code: errors.EInvalid,
	}

	ErrOrgNotFound = &errors.Error{
		Msg:  "organization not found",
		Code: errors.ENotFound,
	}

	ErrTaskRunAlreadyQueued = &errors.Error{
		Msg:  "run already queued",
		Code: errors.EConflict,
	}

	// ErrOutOfBoundsLimit is returned with FindRuns is called with an invalid filter limit.
	ErrOutOfBoundsLimit = &errors.Error{
		Code: errors.EUnprocessableEntity,
		Msg:  "run limit is out of bounds, must be between 1 and 500",
	}

	// ErrInvalidOwnerID is called when trying to create a task with out a valid ownerID
	ErrInvalidOwnerID = &errors.Error{
		Code: errors.EInvalid,
		Msg:  "cannot create task with invalid ownerID",
	}
)

// ErrFluxParseError is returned when an error is thrown by Flux.Parse in the task executor
func ErrFluxParseError(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInvalid,
		Msg:  "could not parse Flux script",
		Op:   "taskExecutor",
		Err:  err,
	}
}

// ErrQueryError is returned when an error is thrown by Query service in the task executor
func ErrQueryError(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  "unexpected error from queryd",
		Op:   "taskExecutor",
		Err:  err,
	}
}

// ErrResultIteratorError is returned when an error is thrown by exhaustResultIterators in the executor
func ErrResultIteratorError(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInvalid,
		Msg:  "error exhausting result iterator",
		Op:   "taskExecutor",
		Err:  err,
	}
}

func ErrInternalTaskServiceError(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  "unexpected error in tasks",
		Op:   "task",
		Err:  err,
	}
}

// ErrUnexpectedTaskBucketErr a generic error we can use when we rail to retrieve a bucket
func ErrUnexpectedTaskBucketErr(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  "unexpected error retrieving task bucket",
		Op:   "taskBucket",
		Err:  err,
	}
}

// ErrTaskTimeParse an error for time parsing errors
func ErrTaskTimeParse(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  "unexpected error parsing time",
		Op:   "taskCron",
		Err:  err,
	}
}

func ErrTaskOptionParse(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInvalid,
		Msg:  "invalid options",
		Op:   "taskOptions",
		Err:  err,
	}
}

func ErrRunExecutionError(err error) *errors.Error {
	return &errors.Error{
		Code: errors.EInternal,
		Msg:  "could not execute task run",
		Op:   "taskExecutor",
		Err:  err,
	}
}

func ErrTaskConcurrencyLimitReached(runsInFront int) *errors.Error {
	return &errors.Error{
		Code: errors.ETooManyRequests,
		Msg:  fmt.Sprintf("could not execute task, concurrency limit reached, runs in front: %d", runsInFront),
		Op:   "taskExecutor",
	}
}
