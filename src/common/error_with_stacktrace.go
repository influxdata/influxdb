package common

import (
	"fmt"
	"runtime"
)

// An error that remembers where it was thrown from
type ErrorWithStacktrace struct {
	msg        string
	stacktrace string
	cause      error
}

// if msg is provided, msg is treated as the error message and the
// error is treated as the original cause of this error. Otherwise the
// error is used as the error message. This is useful for chaining
// multiple errors to trace what was the original error that triggered
// the subsequent errors
func NewErrorWithStacktrace(cause error, msg ...interface{}) *ErrorWithStacktrace {
	buffer := make([]byte, 1024, 1042)
	n := runtime.Stack(buffer, false)
	message := ""
	if len(msg) > 0 {
		message = msg[0].(string)
		if len(msg) > 1 {
			message = fmt.Sprintf(message, msg[1:])
		}
	}
	return &ErrorWithStacktrace{message, string(buffer[:n]), cause}
}

func (self *ErrorWithStacktrace) Error() string {
	if self.msg == "" {
		return fmt.Sprintf("%s. Stacktrace:\n%s\n", self.cause, self.stacktrace)
	}
	if self.cause == nil {
		return fmt.Sprintf("%s. Stacktrace:\n%s\n", self.msg, self.stacktrace)
	}
	return fmt.Sprintf("%s. Stacktrace:\n%s\nCaused by: %s", self.msg, self.stacktrace, self.cause)
}
