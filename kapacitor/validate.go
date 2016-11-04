package kapacitor

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/influxdata/chronograf"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

// ValidateAlert checks if the alert is a valid kapacitor alert service.
func ValidateAlert(service string) error {
	// Simple tick script to check alert service.
	// If a pipeline cannot be created then we know this is an invalid
	// service.  At least with this version of kapacitor!
	script := fmt.Sprintf("stream|from()|alert().%s()", service)
	return validateTick(chronograf.TICKScript(script))
}

func formatTick(tickscript string) (chronograf.TICKScript, error) {
	node, err := ast.Parse(tickscript)
	if err != nil {
		log.Fatalf("parse execution: %s", err)
		return "", err
	}

	output := new(bytes.Buffer)
	node.Format(output, "", true)
	return chronograf.TICKScript(output.String()), nil
}

func validateTick(script chronograf.TICKScript) error {
	scope := stateful.NewScope()
	predefinedVars := map[string]tick.Var{}
	_, err := pipeline.CreatePipeline(string(script), pipeline.StreamEdge, scope, &deadman{}, predefinedVars)
	return err
}

type deadman struct {
	interval  time.Duration
	threshold float64
	id        string
	message   string
	global    bool
}

func (d deadman) Interval() time.Duration { return d.interval }
func (d deadman) Threshold() float64      { return d.threshold }
func (d deadman) Id() string              { return d.id }
func (d deadman) Message() string         { return d.message }
func (d deadman) Global() bool            { return d.global }
