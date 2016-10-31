package tickscripts

// TODO: I don't think mean is correct here.  It's probably any value.
// TODO: seems like we should only have statechanges

// ThresholdTemplate is a tickscript template template for threshold alerts
var ThresholdTemplate = `var database = 'telegraf'
var rp = 'autogen'
var measurement string
var metric string
var groupby = ['host']
var crit int
var period duration
var every duration
var message string
var id string

stream
  |from()
    .database(database)
    .retentionPolicy(rp)
    .measurement(measurement)
    .groupBy(groupby)
  |window()
    .period(period)
    .every(every)
  |mean(metric)
    .as('stat')
  |alert()
    .id(id)
    .message(message)
    .crit(lambda: "stat" {{ .Operator }} crit)
    .{{ .Service }}()`

// RelativeTemplate compares one window of data versus another.
var RelativeTemplate = `var database = 'telegraf'
var rp = 'autogen'
var measurement string
var metric string
var groupby = ['host']
var crit int
var period duration
var every duration
var shift duration
var message string
var id string

var data  = stream
	|from()
        .database(database)
        .retentionPolicy(rp)
		.measurement(measurement)
		.groupBy(groupby)

var past = data
	|window()
		.period(period)
		.every(every)
		.align()
    |{{ .Aggregate }}(metric)
        .as('stat')
	|shift(shift)

var current = data
	|window()
		.period(period)
		.every(every)
		.align()
    |{{ .Aggregate }}(metric)
        .as('stat')

past
	|join(current)
		.as('past', 'current')
	|eval(lambda: abs(float("current.stat" - "past.stat"))/float("past.stat"))
		.keep()
		.as('perc')
    |alert()
        .id(id)
        .message(message)
        .crit(lambda: "perc" {{ .Operator }} crit)
        .{{ .Service }}()`

// DeadmanTemplate checks if any data has been streamed in the last period of time
var DeadmanTemplate = `var database = 'telegraf'
var rp = 'autogen'
var measurement string
var groupby = ['host']
var threshold float
var period duration

var id string
var message string

stream
  |from()
    .database(database)
    .retentionPolicy(rp)
    .measurement(measurement)
    .groupBy(groupby)
  |deadman(threshold, period)
    .id(id)
    .message(message)
	.{{ .Service }}()
`
