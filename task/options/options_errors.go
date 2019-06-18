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

var (
	ErrDuplicateIntervalField = fmt.Errorf("cannot use both cron and every in task options")
)
