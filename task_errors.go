package influxdb

import (
	"fmt"
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

	ErrRunKeyNotFound = &Error{
		Code: ENotFound,
		Msg:  "run key not found",
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

	// ErrInvalidOwnerID is called when trying to create a task with out a valid ownerID
	ErrInvalidOwnerID = &Error{
		Code: EInvalid,
		Msg:  "cannot create task with invalid ownerID",
	}
)

// ErrFluxParseError is returned when an error is thrown by Flux.Parse in the task executor
func ErrFluxParseError(err error) *Error {
	return &Error{
		Code: EInvalid,
		Msg:  fmt.Sprintf("could not parse Flux script; Err: %v", err),
		Op:   "taskExecutor",
		Err:  err,
	}
}

// ErrQueryError is returned when an error is thrown by Query service in the task executor
func ErrQueryError(err error) *Error {
	return &Error{
		Code: EInternal,
		Msg:  fmt.Sprintf("unexpected error from queryd; Err: %v", err),
		Op:   "taskExecutor",
		Err:  err,
	}
}

// ErrResultIteratorError is returned when an error is thrown by exhaustResultIterators in the executor
func ErrResultIteratorError(err error) *Error {
	return &Error{
		Code: EInvalid,
		Msg:  fmt.Sprintf("Error exhausting result iterator; Err: %v", err),
		Op:   "taskExecutor",
		Err:  err,
	}
}

func ErrInternalTaskServiceError(err error) *Error {
	return &Error{
		Code: EInternal,
		Msg:  fmt.Sprintf("unexpected error in tasks; Err: %v", err),
		Op:   "task",
		Err:  err,
	}
}

// ErrUnexpectedTaskBucketErr a generic error we can use when we rail to retrieve a bucket
func ErrUnexpectedTaskBucketErr(err error) *Error {
	return &Error{
		Code: EInternal,
		Msg:  fmt.Sprintf("unexpected error retrieving task bucket; Err: %v", err),
		Op:   "taskBucket",
		Err:  err,
	}
}

// ErrTaskTimeParse an error for time parsing errors
func ErrTaskTimeParse(err error) *Error {
	return &Error{
		Code: EInternal,
		Msg:  fmt.Sprintf("unexpected error parsing time; Err: %v", err),
		Op:   "taskCron",
		Err:  err,
	}
}

func ErrTaskOptionParse(err error) *Error {
	return &Error{
		Code: EInvalid,
		Msg:  fmt.Sprintf("invalid options; Err: %v", err),
		Op:   "taskOptions",
		Err:  err,
	}
}

func ErrJsonMarshalError(err error) *Error {
	return &Error{
		Code: EInvalid,
		Msg:  fmt.Sprintf("unable to marshal JSON; Err: %v", err),
		Op:   "taskScheduler",
		Err:  err,
	}
}

func ErrRunExecutionError(err error) *Error {
	return &Error{
		Code: EInternal,
		Msg:  fmt.Sprintf("could not execute task run; Err: %v", err),
		Op:   "taskExecutor",
		Err:  err,
	}
}

func ErrTaskConcurrencyLimitReached(runsInFront int) *Error {
	return &Error{
		Code: ETooManyRequests,
		Msg:  fmt.Sprintf("could not execute task, concurrency limit reached, runs in front: %d", runsInFront),
		Op:   "taskExecutor",
	}
}
