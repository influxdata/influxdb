package errors

import (
	"errors"
	"strings"
)

// List represents a list of errors.
type List struct {
	errs []error
	err  error // cached error
}

// Append adds err to the errors list.
func (l *List) Append(err error) {
	l.errs = append(l.errs, err)
	l.err = nil
}

// AppendString adds a new error that formats as the given text.
func (l *List) AppendString(text string) {
	l.errs = append(l.errs, errors.New(text))
	l.err = nil
}

// Clear removes all the previously appended errors from the list.
func (l *List) Clear() {
	for i := range l.errs {
		l.errs[i] = nil
	}
	l.errs = l.errs[:0]
	l.err = nil
}

// Err returns an error composed of the list of errors, separated by a new line, or nil if no errors
// were appended.
func (l *List) Err() error {
	if len(l.errs) == 0 {
		return nil
	}

	if l.err != nil {
		switch len(l.errs) {
		case 1:
			l.err = l.errs[0]

		default:
			var sb strings.Builder
			sb.WriteString(l.errs[0].Error())
			for _, err := range l.errs[1:] {
				sb.WriteRune('\n')
				sb.WriteString(err.Error())
			}
			l.err = errors.New(sb.String())
		}
	}

	return l.err
}
