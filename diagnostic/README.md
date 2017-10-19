# Diagnostic Package

The diagnostic package contains the logger and all log messages. The
idea behind the diagnostic package is that each package will have its
own logging interface. The logging interface will be hyperspecific and
relate to what kinds of messages can be emitted. It is then up to the
implementor to determine how these messages are printed out.

## Adding a New Diagnostic Context

When adding a new handler to the diagnostic package, remember to avoid
adding dependencies to the diagnostic package.

All diagnostics should have their interfaces defined in as concrete a
way as possible. Do not worry about making the interface generic. We do
not care about keeping the interfaces for the diagnostic packages
backwards compatible at the moment. The interfaces should have all
parameters be as generic as possible. The diagnostic package should have
no dependencies outside of the standard library. That includes a
dependency on the package itself.

The structure should look similar to this:

    package/
    └── diagnostic/
        └── context.go

The `diagnostic` package should have a single file `context.go` that
defines the logging interface.

The primary type should be an interface named `Context` that defines the
primary logging interface. There should be no generic methods in the
interface like `Info`, `Debug`, or `Error`. That is a decision for the
handler itself.

## Defining a Handler

A handler should be added to the global `diagnostic` package at
`github.com/influxdata/influxdb/diagnostic`. The handler should
implement the interface defined above. This handler should not have any
dependencies to any packages in `influxdb` except for other `diagnostic`
packages. So a dependency on
`github.com/influxdata/influxdb/services/meta` is bad, but a dependency
on `github.com/influxdata/influxdb/services/meta/diagnostic` is fine and
encouraged.

The handler should also define a construction function that looks like
this:

    func (s *Service) ServiceContext() service.Context {
        if s == nil {
            return nil
        }
        return ...
    }

Replace `Service` with the name of the service. A reference to the
package's `diagnostic` package should be renamed to the primary package
name. So the above example will create a context for the `service`
service.

## Considerations

Avoid leaking any details of the logging implementation into the context
or handler. The diagnostic packages themselves should be small so they
are easy to include and do not drag in dependencies that may be
unnecessary. We split each of the context's into their own package so
you do not need to drag in the entirety of the `tsdb` package to use the
`query` diagnostic.
