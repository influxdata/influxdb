# Line Protocol

The line protocol is a text based format for representing measurments.  Each line defines a measurement
and multiple lines are separated by the newline character `\n`. The format of the line consists of three parts:

```
[key] [fields] [timestamp]
```

Each section is separated by spaces.  The minimum required line consists of a measurement name and at least one field.

## Key

The key is the measurement name and any optional tags separated by commas.  Measurement names, tag keys and values must escape any spaces, commas using `\ ` and `\,` respectively.  Values should not be surrounded in quotes.

Tags should always be sorted by key before being sent for best performance.

### Examples

```
# measurement only
cpu

# measurment and tags
cpu,host=serverA,region=us-west

# measurment with commas
cpu\,01,host=serverA,region=us-west

# tag value with spaces
cpu,host=server\ A,region=us\ west
```

## Fields

Fields are are values associated with the measurement.  Every line must have at least one field.  Multiple fields should be separated with commas and not spaces in between.

Fields can be one of four types.  The value written for a given field defines the type of the field.

* _integer_ - Numeric values that do not include a decimal.  (e.g. 1, 345, 2015, -10)
* _float_ - Numeric values that include a decimal.  (e.g. 1.0, -3.14, 6.0+e5).  Note that all values _must_ have a decial even if the
decimal value is zero.
* _boolean_ - A value indicating true or false.  Valid boolean strings are (t, T, true, TRUE, f, F, false, and FALSE).
* _string_ - A textual values.  All string values _must_ be surrounded in double-quotes `"`.  If the string contains
a double-quote, it must be escaped with `\"`.


```
# integer value
cpu value=1

# float value
cpu_load value=1.2

# boolean value
error fatal=true

# string value
event msg="logged out"

# multiple values
cpu load=10.0,alert=true,reason="value above maximum threshold"
```

## Timestamp

The timestamp section is optional but should be specified if possible.  The value is an integer representing nanoseconds since the epoch.

Some write APIs allow passing a lower precision.  If the API supports a lower precision, the timestamp may also be
and integer epoch in microseconds, milliseconds, seconds, minutes and hours.

## Full Example
A full example is shown below.
```
cpu,host=server01,region=uswest value=1.0 1434055562000000000
cpu,host=server02,region=uswest value=3.0 1434055562000010000
```
In this example the first line shows a `measurement` of "cpu", there are two tags "host" and "region, the `value` is 1.0, and the `timestamp` is 1434055562000000000. Following this is a second line, also of the `measurement` "cpu" but of a different machine.
