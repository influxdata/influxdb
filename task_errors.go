package influxdb

import (
	"fmt"
	"time"
)

var (
	// ErrRunCanceled is returned from the RunResult when a Run is Canceled.  It is used mostly internally.
	ErrRunCanceled = &Error{
		Code: EInternal,
		Msg:  "run canceled",
	}

	// ErrTaskNotClaimed is returned when attempting to operate against a task that must be claimed but is not.
	ErrTaskNotClaimed = &Error{
		Code: EConflict,
		Msg:  "task not claimed",
	}

	// ErrTaskAlreadyClaimed is returned when attempting to operate against a task that must not be claimed but is.
	ErrTaskAlreadyClaimed = &Error{
		Code: EConflict,
		Msg:  "task already claimed",
	}

	// ErrNoRunsFound is returned when searching for a range of runs, but none are found.
	ErrNoRunsFound = &Error{
		Code: ENotFound,
		Msg:  "no matching runs found",
	}

	// ErrInvalidTaskID error object for bad id's
	ErrInvalidTaskID = &Error{
		Code: EInvalid,
		Msg:  "invalid id",
	}

	// ErrInvalidTaskType error object for bad id's
	ErrInvalidTaskType = &Error{
		Code: EInvalid,
		Msg:  "invalid task type",
	}
	// ErrTaskNotFound indicates no task could be found for given parameters.
	ErrTaskNotFound = &Error{
		Code: ENotFound,
		Msg:  "task not found",
	}

	// ErrRunNotFound is returned when searching for a single run that doesn't exist.
	ErrRunNotFound = &Error{
		Code: ENotFound,
		Msg:  "run not found",
	}

	ErrPageSizeTooSmall = &Error{
		Msg:  "cannot have negative page limit",
		Code: EInvalid,
	}

	ErrPageSizeTooLarge = &Error{
		Msg:  fmt.Sprintf("cannot use page size larger then %d", MaxPageSize),
		Code: EInvalid,
	}

	ErrOrgNotFound = &Error{
		Msg:  "organization not found",
		Code: ENotFound,
	}

	ErrTaskRunAlreadyQueued = &Error{
		Msg:  "run already queued",
		Code: EConflict,
	}

	// ErrOutOfBoundsLimit is returned with FindRuns is called with an invalid filter limit.
	ErrOutOfBoundsLimit = &Error{
		Code: EUnprocessableEntity,
		Msg:  "run limit is out of bounds, must be between 1 and 500",
	}

	// ErrMissingToken is called when trying to create a Task without providing a token
	ErrMissingToken = &Error{
		Code: EInvalid,
		Msg:  "cannot create task without valid token",
	}
)

func ErrInternalTaskServiceError(err error) *Error {
	return &Error{
		Code: EInternal,
		Msg:  fmt.Sprintf("unexpected error in tasks; Err: %v", err),
		Op:   "kv/task",
		Err:  err,
	}
}

// ErrUnexpectedTaskBucketErr a generic error we can use when we rail to retrieve a bucket
func ErrUnexpectedTaskBucketErr(err error) *Error {
	return &Error{
		Code: EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving task bucket; Err: %v", err),
		Op:   "kv/taskBucket",
		Err:  err,
	}
}

// ErrTaskTimeParse an error for time parsing errors
func ErrTaskTimeParse(err error) *Error {
	return &Error{
		Code: EInvalid,
		Msg:  fmt.Sprintf("unexpected error parsing time; Err: %v", err),
		Op:   "kv/taskCron",
		Err:  err,
	}
}

func ErrTaskOptionParse(err error) *Error {
	return &Error{
		Code: EInvalid,
		Msg:  fmt.Sprintf("invalid options; Err: %v", err),
		Op:   "kv/taskOptions",
		Err:  err,
	}
}

// ErrRunNotDueYet is returned from CreateNextRun if a run is not yet due.
func ErrRunNotDueYet(dueAt int64) *Error {
	return &Error{
		Code: EInvalid,
		Msg:  fmt.Sprintf("run not due until: %v", time.Unix(dueAt, 0).UTC().Format(time.RFC3339)),
	}
}
