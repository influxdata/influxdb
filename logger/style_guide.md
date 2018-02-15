# Logging Style Guide

The intention of logging is to give insight to the administrator of how
the server is running and also notify the administrator of any problems
or potential problems with the system.

At the moment, log level filtering is the only option to configure
logging in InfluxDB. Adding a logging message and choosing its level
should be done according to the guidelines in this document for
operational clarity. The available log levels are:

* Error
* Warn
* Info
* Debug

InfluxDB uses structured logging. Structured logging is when you log
messages and attach context to those messages with more easily read data
regarding the state of the system. A structured log message is composed
of:

* Time
* Level
* Message
* (Optionally) Additional context

## Guidelines

**Log messages** should be simple statements or phrases that begin with
a capital letter, but have no punctuation at the end. The message should be a
constant so that every time it is logged it is easily identified and can
be filtered by without regular expressions.

Any **dynamic content** should be expressed by context. The key should
be a constant and the value is the dynamic content.

Do not log messages in tight loops or other high performance locations.
It will likely create a performance problem.

## Naming Conventions

If the log encoding format uses keys for the time, message, or level,
the key names should be `ts` for time, `msg` for the message, and
`lvl` for the level.

If the log encoding format does not use keys for the time, message, or
level and instead outputs them in some other method, this guideline can
be ignored. The output formats logfmt and json both use keys when
encoding these values.

### Context Key Names

The key for the dynamic content in the context should be formatted in
`snake_case`. The key should be completely lower case.

## Levels

As a reminder, levels are usually the only way to configure what is
logged. There are four available logging levels.

* Error
* Warn
* Info
* Debug

It is important to get the right logging level to ensure the log
messages are useful for end users to act on.

In general, when considering which log level to use, you should use
**info**. If you are considering using another level, read the below
expanded descriptions to determine which level your message belongs in.

### Error

The **error** level is intended to communicate that there is a serious
problem with the server. **An error should be emitted only when an
on-call engineer can take some action to remedy the situation _and_ the
system cannot continue operating properly without remedying the
situation.**

An example of what may qualify as an error level message is the creation
of the internal storage for the monitor service. For that system to
function at all, a database must be created. If no database is created,
the service itself cannot function. The error has a clear actionable
solution. Figure out why the database isn't being created and create it.

An example of what does not qualify as an error is failing to parse a
query or a socket closing prematurely. Both of these usually indicate
some kind of user error rather than system error. Both are ephemeral
errors and they would not be clearly actionable to an administrator who
was paged at 3 AM. Both of these are examples of logging messages that
should be emitted at the info level with an error key rather than being
logged at the error level.

Logged errors **must not propagate**. Propagating the error risks
logging it in multiple locations and confusing users when the same error
is reported multiple times. In general, if you are returning an error,
never log at any level. By returning the error, you are telling the
parent function to handle the error. Logging a message at any level is
handling the error.

This logging message should be used very rarely and any messages that
use this logging level should not repeat frequently. Assume that
anything that is logged with error will page someone in the middle of
the night.

### Warn

The **warn** level is intended to communicate that there is likely to be
a serious problem with the server if it not addressed. **A warning
should be emitted only when a support engineer can take some action to
remedy the situation _and_ the system may not continue operating
properly in the near future without remedying the situation.**

An example of what may qualify as a warning is the `max-values-per-tag`
setting. If the server starts to approach the maximum number of values,
the server may stop being able to function properly when it reaches the
maximum number.

An example of what does not qualify as a warning is the
`log-queries-after` setting. While the message is "warning" that a query
was running for a long period of time, it is not clearly actionable and
does not indicate that the server will fail in the near future. This
should be logged at the info level instead.

This logging message should be used very rarely and any messages that
use this logging level should not repeat frequently. Assume that
anything that is logged with warn will page someone in the middle of the
night and potentially ignored until normal working hours.

### Info

The **info** level should be used for almost anything. If you are not
sure which logging level to use, use info. Temporary or user errors
should be logged at the info level and any informational messages for
administrators should be logged at this level. Info level messages
should be safe for an administrator to discard if they really want to,
but most people will run the system at the info level.

### Debug

The **debug** level exists to log messages that are useful only for
debugging a bad running instance.

This level should be rarely used if ever. If you intend to use this
level, please have a rationale ready. Most messages that could be
considered debug either shouldn't exist or should be logged at the info
level. Debug messages will be suppressed by default.

## Value Formatting

Formatting for strings, integers, and other standard values are usually
determined by the log format itself and those will be kept ambiguous.
The following specific formatting choices are for data types that could
be output in multiple ways.

### Time

Time values should be encoded using RFC3339 with microsecond precision.
The size of the string should be normalized to the same number of digits
every time to ensure that it is easier to read the time as a column.

### Duration

Duration values that denote a period of time should be output in
milliseconds with microsecond precision. The microseconds should be in
decimal form with three decimal points. Durations that denote a static
period of time should be output with a single number and a suffix with
the largest possible unit that doesn't cause the value to be a decimal.

There are two types of durations.

* Tracks a (usually small) period of time and is meant for timing how
  long something take. The content is dynamic and may be graphed.
* Duration literal where the content is dynamic, is unlikely to be
  graphed, and usually comes from some type of configuration.

If the content is dynamic, the duration should be printed as a number of
milliseconds with a decimal indicating the number of microseconds. Any
duration lower than microseconds should be truncated. The decimal section
should always print exactly 3 points after the decimal point.

If the content is static, the duration should be printed with a single
number and a suffix indicating the unit in years (`y`), weeks (`w`),
days (`d`), hours (`h`), minutes (`m`), seconds (`s`), or
milliseconds (`ms`). The suffix should be the greatest unit that can be
used without truncating the value. As an example, if the duration is
60 minutes, then `1h` should be used. If the duration is 61 minutes,
then `61m` should be used.

For anything lower than milliseconds that is static, the duration should
be truncated. A value of zero should be shown as `0s`.
