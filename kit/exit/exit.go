// Package exit maps an error to the status the process should exit with.
//
// influxd historically exited 0 or 1, which tells an init system nothing it can
// act on: a typo in a config file and a full disk are reported identically, so
// systemd's Restart=on-failure retries both forever. The codes here answer the
// one question a supervisor can actually use -- will retrying help? -- by
// naming the category of the failure.
//
// The vocabulary is sysexits.h (see sysexits.h(3head)), whose 64-78 block is
// the conventional home for an application's own statuses: it sits inside the
// 1-125 "standard errors" range and clear of every reserved band (126-127 for
// the shell, 128-165 for signals, 255 for overflow).
//
// # Classify and Code are not the same function
//
// Code never guesses. It reports a status only for an error that was explicitly
// given one with WithCode, and returns CodeGeneric for everything else. That is
// what keeps this package's reach equal to its wiring: a command that has not
// opted in exits 1 exactly as it always has, rather than acquiring a sysexits
// status by accident.
//
// Classify does guess, from the OS-level cause underneath the error. It is
// meant to run once, at the boundary where a failure leaves the subsystem that
// produced it, with the result pinned by WithCode. Pinning is what makes the
// answer stable: a caller that joins the startup error with a later teardown
// error would otherwise have a second, differently-caused chain for a late
// Classify to walk into, and could report the teardown's category as the
// startup's.
//
// # Which statuses influxd emits
//
// Every sysexits value is defined here, because a partial copy of a standard
// header invites a later collision. influxd emits a subset: CodeUsage,
// CodeDataErr, CodeNoInput, CodeUnavailable, CodeSoftware, CodeOSErr,
// CodeCantCreate, CodeIOErr, CodeTempFail, CodeNoPerm and CodeConfig.
// CodeNoUser, CodeNoHost, CodeOSFile and CodeProtocol are defined and unused --
// influxd has no addressee, no remote host lookup, no /etc-style system file
// dependency and no protocol peer at startup.
//
// See EXIT_CODES.md at the repository root for the operator-facing table.
package exit

import (
	"context"
	"errors"
	"io/fs"
	"syscall"
)

// Process exit statuses.
//
// CodeOK and CodeGeneric are the two influxd has always used. The rest are the
// sysexits.h block, with the EX_ prefix dropped to read as Go identifiers;
// Name maps each back to its C macro name.
const (
	// CodeOK is a successful run: startup completed and teardown completed.
	CodeOK = 0
	// CodeGeneric is an error carrying no category. It is what Code reports
	// for an error nothing pinned a status to, and is the status influxd has
	// always exited with on failure.
	CodeGeneric = 1

	CodeUsage       = 64 // EX_USAGE: the command line was wrong.
	CodeDataErr     = 65 // EX_DATAERR: input data was wrong.
	CodeNoInput     = 66 // EX_NOINPUT: an input path is missing or unreadable.
	CodeNoUser      = 67 // EX_NOUSER: the named user does not exist.
	CodeNoHost      = 68 // EX_NOHOST: the named host does not exist.
	CodeUnavailable = 69 // EX_UNAVAILABLE: a service or resource is not available.
	CodeSoftware    = 70 // EX_SOFTWARE: an internal error with no OS-level cause.
	CodeOSErr       = 71 // EX_OSERR: an operating system error, such as resource exhaustion.
	CodeOSFile      = 72 // EX_OSFILE: a system file is missing or broken.
	CodeCantCreate  = 73 // EX_CANTCREAT: an output file cannot be created or extended.
	CodeIOErr       = 74 // EX_IOERR: an I/O error occurred.
	CodeTempFail    = 75 // EX_TEMPFAIL: a temporary failure; retrying may succeed.
	CodeProtocol    = 76 // EX_PROTOCOL: a remote peer violated a protocol.
	CodeNoPerm      = 77 // EX_NOPERM: permission was denied.
	CodeConfig      = 78 // EX_CONFIG: something is configured wrongly.
)

// codeNames gives each defined status its sysexits macro name, for log fields,
// documentation and test failure messages.
var codeNames = map[int]string{
	CodeOK:          "EX_OK",
	CodeGeneric:     "generic",
	CodeUsage:       "EX_USAGE",
	CodeDataErr:     "EX_DATAERR",
	CodeNoInput:     "EX_NOINPUT",
	CodeNoUser:      "EX_NOUSER",
	CodeNoHost:      "EX_NOHOST",
	CodeUnavailable: "EX_UNAVAILABLE",
	CodeSoftware:    "EX_SOFTWARE",
	CodeOSErr:       "EX_OSERR",
	CodeOSFile:      "EX_OSFILE",
	CodeCantCreate:  "EX_CANTCREAT",
	CodeIOErr:       "EX_IOERR",
	CodeTempFail:    "EX_TEMPFAIL",
	CodeProtocol:    "EX_PROTOCOL",
	CodeNoPerm:      "EX_NOPERM",
	CodeConfig:      "EX_CONFIG",
}

// Name returns the sysexits macro name for a status, or "" if the status is not
// one this package defines.
func Name(code int) string {
	return codeNames[code]
}

// Coder is an error that names the process exit status it should produce.
//
// Anything in an error chain may implement it; Code and Classify find it with
// errors.As, so a status pinned deep in a chain survives further wrapping.
type Coder interface {
	error
	ExitCode() int
}

// codedError pins a status onto an error without disturbing anything else about
// it. Error delegates verbatim rather than prefixing, because these errors are
// printed to the operator by cobra and by influxd's own exit path: a status is
// for the shell to read, not something to say twice in the message.
type codedError struct {
	code int
	err  error
}

var _ Coder = (*codedError)(nil)

func (e *codedError) Error() string { return e.err.Error() }
func (e *codedError) Unwrap() error { return e.err }
func (e *codedError) ExitCode() int { return e.code }

// WithCode returns err with code pinned to it as the process exit status.
//
// The result's message is err's message unchanged, and errors.Is and errors.As
// still reach err and everything it wraps. WithCode(code, nil) is nil, so it is
// safe to apply to a result that may not be an error.
//
// Pin a status at a site that knows something Classify cannot infer -- a
// sentinel or a formatted message with no OS-level cause beneath it -- or at
// the boundary where a failure leaves the component that produced it, to fix
// the answer before the error is joined with any other.
func WithCode(code int, err error) error {
	if err == nil {
		return nil
	}
	return &codedError{code: code, err: err}
}

// Code returns the exit status pinned to err, CodeOK if err is nil, and
// CodeGeneric if nothing in the chain pinned one.
//
// It performs no classification of its own. An error that was never given a
// status exits 1, which is what influxd has always done, so wiring this into a
// process's exit path changes nothing for the paths that have not opted in.
//
// errors.As walks a tree built by errors.Join depth-first and left to right, so
// when a joined error carries more than one pinned status the leftmost wins.
// Callers that join a primary failure with a secondary one should put the
// primary first, which is also the order that produces the right message.
func Code(err error) int {
	if err == nil {
		return CodeOK
	}
	var c Coder
	if errors.As(err, &c) {
		return c.ExitCode()
	}
	return CodeGeneric
}

// Classify chooses an exit status for err from the cause underneath it.
//
// The order is: a status already pinned anywhere in the chain wins, because a
// site that knew its own category outranks a guess made from a syscall; then
// the platform's errno table, consulted for every errno in the tree rather than
// only the first; then the portable io/fs and context sentinels; then
// CodeSoftware for a failure with no OS-level cause to point at.
//
// The sentinel checks run after the errno table and are not redundant with it.
// On Windows the errno values are Win32 and Winsock numbers rather than the
// POSIX ones, so a file error that the table cannot place still resolves
// through fs.ErrPermission or fs.ErrNotExist, which the standard library maps
// per platform.
//
// The categories, stated once so call sites need not re-derive them: a
// path-shaped problem is CodeNoInput, a permission problem is CodeNoPerm, a
// space problem is CodeCantCreate, and a value the operator declared being
// wrong is CodeConfig -- the last of which has no OS-level signature and so is
// always pinned at its site rather than found here.
func Classify(err error) int {
	if err == nil {
		return CodeOK
	}

	var c Coder
	if errors.As(err, &c) {
		return c.ExitCode()
	}

	if code, ok := mappedErrno(err); ok {
		return code
	}

	switch {
	case errors.Is(err, fs.ErrPermission):
		return CodeNoPerm
	case errors.Is(err, fs.ErrNotExist):
		return CodeNoInput
	// An interrupt is not a fault. kit/signals turns SIGINT into a cancel of
	// the context startup runs under, so Ctrl-C during a long start -- shard
	// loading can run for minutes -- arrives here as context.Canceled.
	// CodeTempFail is what is true of both sentinels: the work did not get to
	// finish, and starting again may well succeed. CodeSoftware would call an
	// operator's own stop a software bug, and the systemd recipe in
	// EXIT_CODES.md puts that status in RestartPreventExitStatus.
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return CodeTempFail
	}

	return CodeSoftware
}

// mappedErrno reports the status for the first errno in err's tree that the
// platform's table has an entry for, walking depth-first and left to right --
// the order errors.As uses, and the order that reaches a joined error's primary
// arm first.
//
// errors.As cannot do this itself: it stops at the first syscall.Errno it finds
// and hands it over whether or not the table knows it, ending the search. That
// is exactly wrong for the shape influxd produces on the way out --
// Launcher.shutdownError joins one error per failing closer -- where a mapped
// ENOSPC routinely sits behind an unmapped EINVAL from a listener that was
// already closed. Stopping at the EINVAL would report a full disk as an
// unclassifiable software fault; skipping it and continuing does not.
//
// The walk follows Unwrap rather than calling errors.As per node, because an As
// method that descends on its own would put back the behavior being avoided.
func mappedErrno(err error) (int, bool) {
	for err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			if code, ok := errnoCodes[errno]; ok {
				return code, true
			}
		}

		switch u := err.(type) {
		case interface{ Unwrap() error }:
			err = u.Unwrap()
		case interface{ Unwrap() []error }:
			for _, e := range u.Unwrap() {
				if code, ok := mappedErrno(e); ok {
					return code, true
				}
			}
			return 0, false
		default:
			return 0, false
		}
	}

	return 0, false
}
