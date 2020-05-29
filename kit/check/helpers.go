package check

import (
	"context"
	"fmt"
)

// NamedChecker is a superset of Checker that also indicates the name of the service.
// Prefer to implement NamedChecker if your service has a fixed name,
// as opposed to calling *Health.AddNamed.
type NamedChecker interface {
	Checker
	CheckName() string
}

// CheckerFunc is an adapter of a plain func() error to the Checker interface.
type CheckerFunc func(ctx context.Context) Response

// Check implements Checker.
func (f CheckerFunc) Check(ctx context.Context) Response {
	return f(ctx)
}

// Named returns a Checker that will attach a name to the Response from the check.
// This way, it is possible to augment a Response with a human-readable name, but not have to encode
// that logic in the actual check itself.
func Named(name string, checker Checker) Checker {
	return CheckerFunc(func(ctx context.Context) Response {
		resp := checker.Check(ctx)
		resp.Name = name
		return resp
	})
}

// NamedFunc is the same as Named except it takes a CheckerFunc.
func NamedFunc(name string, fn CheckerFunc) Checker {
	return Named(name, fn)
}

// ErrCheck will create a health checker that executes a function. If the function returns an error,
// it will return an unhealthy response. Otherwise, it will be as if the Ok function was called.
// Note: it is better to use CheckFunc, because with Check, the context is ignored.
func ErrCheck(fn func() error) Checker {
	return CheckerFunc(func(_ context.Context) Response {
		if err := fn(); err != nil {
			return Error(err)
		}
		return Pass()
	})
}

// Pass is a utility function to generate a passing status response with the default parameters.
func Pass() Response {
	return Response{
		Status: StatusPass,
	}
}

// Info is a utility function to generate a healthy status with a printf message.
func Info(msg string, args ...interface{}) Response {
	return Response{
		Status:  StatusPass,
		Message: fmt.Sprintf(msg, args...),
	}
}

// Error is a utility function for creating a response from an error message.
func Error(err error) Response {
	return Response{
		Status:  StatusFail,
		Message: err.Error(),
	}
}
