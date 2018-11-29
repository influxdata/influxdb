package assert

import (
	"fmt"
	"reflect"
)

func fail(t TestingT, failureMsg string, msgAndArgs ...interface{}) bool {
	if th, ok := t.(helper); ok {
		th.Helper()
	}

	msg := formatMsgAndArgs(msgAndArgs...)
	if msg == "" {
		t.Errorf("%s", failureMsg)
	} else {
		t.Errorf("%s: %s", failureMsg, msg)
	}

	return false
}

func formatValues(got, expected interface{}) (string, string) {
	if reflect.TypeOf(got) != reflect.TypeOf(expected) {
		return fmt.Sprintf("%T(%#v)", got, got), fmt.Sprintf("%T(%#v)", expected, expected)
	}

	return fmt.Sprintf("%#v", got), fmt.Sprintf("%#v", expected)
}

func formatMsgAndArgs(msgAndArgs ...interface{}) string {
	if len(msgAndArgs) == 0 || msgAndArgs == nil {
		return ""
	}
	if len(msgAndArgs) == 1 {
		return msgAndArgs[0].(string)
	}
	if len(msgAndArgs) > 1 {
		return fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
	}
	return ""
}

// didPanic returns true if fn panics when called.
func didPanic(fn PanicTestFunc) (panicked bool, message interface{}) {
	defer func() {
		if message = recover(); message != nil {
			panicked = true
		}
	}()

	fn()

	return panicked, message
}
