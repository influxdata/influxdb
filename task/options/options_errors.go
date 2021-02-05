package options

import (
	"errors"
	"fmt"
)

// errParseTaskOptionField is returned when we fail to parse a single field in
// task options.
func errParseTaskOptionField(opt string) error {
	return fmt.Errorf("failed to parse field '%s' in task options", opt)
}

// errMissingRequiredTaskOption is returned when we a required option is
// missing.
func errMissingRequiredTaskOption(opt string) error {
	return fmt.Errorf("missing required option: %s", opt)
}

// errTaskInvalidDuration is returned when an "every" or "offset" option is invalid in a task.
func errTaskInvalidDuration(err error) error {
	return fmt.Errorf("invalid duration in task %s", err)
}

// errTaskOptionNotObjectExpression is returned when the type of an task option
// value is not an object literal expression.
func errTaskOptionNotObjectExpression(actualType string) error {
	return fmt.Errorf("task option expected to be object literal, but found %q", actualType)
}

var (
	ErrDuplicateIntervalField     = errors.New("cannot use both cron and every in task options")
	ErrNoTaskOptionsDefined       = errors.New("no task options defined")
	ErrMultipleTaskOptionsDefined = errors.New("multiple task options defined")
	ErrNoASTFile                  = errors.New("expected parsed file, but found none")
)
