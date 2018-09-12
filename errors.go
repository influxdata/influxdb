package platform

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Some error code constant, ideally we want define common platform codes here
// projects on use platform's error, should have their own central place like this.
const (
	EInternal   = "internal error"
	ENotFound   = "not found"
	EConflict   = "conflict" // action cannot be performed
	EInvalid    = "invalid"  // validation failed
	EEmptyValue = "empty value"
)

// Error is the error struct of platform.
//
// Errors may have error codes, human-readable messages,
// and a logical stack trace.
//
// The Code targets automated handlers so that recovery can occur.
// Msg is used by the system operator to help diagnose and fix the problem.
// Op and Err chain errors together in a logical stack trace to
// further help operators.
//
// To create a simple error,
//     &Error{
//         Code:ENotFound,
//     }
// To show where the error happens, add Op.
//     &Error{
//         Code: ENotFound,
//         Op: "bolt.FindUserByID"
//     }
// To show an error with a unpredictable value, add the value in Msg.
//     &Error{
//        Code: EConflict,
//        Message: fmt.Sprintf("organization with name %s already exist", aName),
//     }
// To show an error wrapped with another error.
//     &Error{
//         Code:EInternal,
//         Err: err,
//     }.
type Error struct {
	Code string `json:"code"`          // Code is the machine-readable error code.
	Msg  string `json:"msg,omitempty"` // Msg is a human-readable message.
	Op   string `json:"op,omitempty"`  // Op describes the logical code operation during error.
	Err  error  `json:"err,omitempty"` // Err is a stack of additional errors.
}

// Error implement the error interface by outputing the Code and Err.
func (e *Error) Error() string {
	var b strings.Builder

	// Print the current operation in our stack, if any.
	if e.Op != "" {
		fmt.Fprintf(&b, "%s: ", e.Op)
	}

	// If wrapping an error, print its Error() message.
	// Otherwise print the error code & message.
	if e.Err != nil {
		b.WriteString(e.Err.Error())
	} else {
		if e.Code != "" {
			fmt.Fprintf(&b, "<%s>", e.Code)
			if e.Msg != "" {
				b.WriteString(" ")
			}
		}
		b.WriteString(e.Msg)
	}
	return b.String()
}

// ErrorCode returns the code of the root error, if available; otherwise returns EINTERNAL.
func ErrorCode(err error) string {
	if err == nil {
		return ""
	} else if e, ok := err.(*Error); ok && e.Code != "" {
		return e.Code
	}
	return EInternal
}

// ErrorMessage returns the human-readable message of the error, if available.
// Otherwise returns a generic error message.
func ErrorMessage(err error) string {
	if err == nil {
		return ""
	} else if e, ok := err.(*Error); ok && e.Msg != "" {
		return e.Msg
	} else if ok && e.Err != nil {
		return ErrorMessage(e.Err)
	}
	return "An internal error has occurred."
}

// errEncode an JSON encoding helper that is needed to handle the recursive stack of errors.
type errEncode struct {
	Code string `json:"code"`          // Code is the machine-readable error code.
	Msg  string `json:"msg,omitempty"` // Msg is a human-readable message.
	Op   string `json:"op,omitempty"`  // Op describes the logical code operation during error.
	Err  string `json:"err,omitempty"` // Err is a stack of additional errors.
}

// MarshalJSON recursively marshals the stack of Err.
func (e *Error) MarshalJSON() (result []byte, err error) {
	ee := errEncode{
		Code: e.Code,
		Msg:  e.Msg,
		Op:   e.Op,
	}
	if e.Err != nil {
		if _, ok := e.Err.(*Error); ok {
			b, err := e.Err.(*Error).MarshalJSON()
			if err != nil {
				return result, err
			}
			ee.Err = string(b)
		} else {
			ee.Err = e.Err.Error()
		}
	}
	return json.Marshal(ee)
}

// UnmarshalJSON recursively unmarshals the error stack.
func (e *Error) UnmarshalJSON(b []byte) (err error) {
	ee := new(errEncode)
	err = json.Unmarshal(b, ee)
	e.Code = ee.Code
	e.Msg = ee.Msg
	e.Op = ee.Op
	if ee.Err != "" {
		var innerErr error
		innerResult := new(Error)
		innerErr = innerResult.UnmarshalJSON([]byte(ee.Err))
		if innerErr != nil {
			e.Err = errors.New(ee.Err)
			return err
		}
		e.Err = innerResult
	}
	return err
}
