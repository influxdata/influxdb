package kapacitor

import (
	"bytes"
	"log"
	"text/template"
)

// ThresholdTrigger is the trickscript trigger for alerts that exceed a value
var ThresholdTrigger = `
  var trigger = data|{{ .Aggregate }}(metric)
    .as('value')
  |alert()
    .stateChangesOnly()
    .crit(lambda: "value" {{ .Operator }} crit)
    .message(message)
	.id(idVar)
	.idTag(idtag)
	.levelField(levelfield)
	.messageField(messagefield)
	.durationField(durationfield)
`

// RelativeTrigger compares one window of data versus another.
var RelativeTrigger = `
var past = data
    |{{ .Aggregate }}(metric)
        .as('stat')
	|shift(shift)

var current = data
    |{{ .Aggregate }}(metric)
        .as('stat')

var trigger = past
	|join(current)
		.as('past', 'current')
	|eval(lambda: abs(float("current.stat" - "past.stat"))/float("past.stat"))
		.keep()
		.as('value')
    |alert()
        .stateChangesOnly()
        .crit(lambda: "value" {{ .Operator }} crit)
        .message(message)
		.id(idVar)
		.idTag(idtag)
		.levelField(levelfield)
		.messageField(messagefield)
		.durationField(durationfield)
`

// DeadmanTrigger checks if any data has been streamed in the last period of time
var DeadmanTrigger = `
  var trigger = data|deadman(threshold, every)
    .stateChangesOnly()
    .message(message)
	.id(idVar)
	.idTag(idtag)
	.levelField(levelfield)
	.messageField(messagefield)
	.durationField(durationfield)
`

func execTemplate(tick string, alert interface{}) (string, error) {
	p := template.New("template")
	t, err := p.Parse(tick)
	if err != nil {
		log.Fatalf("template parse: %s", err)
		return "", err
	}
	buf := new(bytes.Buffer)
	err = t.Execute(buf, alert)
	if err != nil {
		log.Fatalf("template execution: %s", err)
		return "", err
	}
	return buf.String(), nil
}
