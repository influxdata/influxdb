package options

import (
	"fmt"
)

func ErrParseTaskOptionField(opt string) error {
	return fmt.Errorf("failed to parse field '%s' in task options", opt)
}

func ErrMissingRequiredTaskOption(opt string) error {
	return fmt.Errorf("missing required option: %s", opt)
}

// ErrTaskInvalidDuration is returned when an "every" or "offset" option is invalid in a task.
func ErrTaskInvalidDuration(err error) error {
	return fmt.Errorf("invalid duration in task %s", err)
}

var (
	ErrDuplicateIntervalField = fmt.Errorf("cannot use both cron and every in task options")
)
