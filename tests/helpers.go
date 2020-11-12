package tests

import (
	"context"
)

// VeryVerbose when set to true, will enable very verbose logging of services. Controlled via
// flag in tests_test package.
var VeryVerbose bool

// An OpenCloser can both open and close.
type OpenCloser interface {
	Open() error
	Close() error
}

// OpenClosers is a collection of OpenCloser objects.
type OpenClosers []OpenCloser

// OpenAll opens all the OpenClosers, returning the first error encountered,
// if any.
func (ocs OpenClosers) OpenAll() error {
	for _, oc := range ocs {
		if err := oc.Open(); err != nil {
			return err
		}
	}
	return nil
}

// MustOpenAll opens all the OpenClosers, panicking if any error is encountered.
func (ocs OpenClosers) MustOpenAll() {
	if err := ocs.OpenAll(); err != nil {
		panic(err)
	}
}

// CloseAll closes all the OpenClosers, returning the first error encountered,
// if any.
func (ocs OpenClosers) CloseAll() error {
	// Close in the reverse order that we opened,
	// under the assumption that later ocs depend on earlier ones.
	for i := range ocs {
		if err := ocs[len(ocs)-i-1].Close(); err != nil && err != context.Canceled {
			return err
		}
	}
	return nil
}

// MustCloseAll closes all the OpenClosers, panicking if any error is encountered.
func (ocs OpenClosers) MustCloseAll() {
	if err := ocs.CloseAll(); err != nil {
		panic(err)
	}
}
