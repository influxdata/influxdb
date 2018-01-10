package kapacitor

import (
	"bytes"
	"fmt"
	"strings"
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
	script := fmt.Sprintf("stream|from()|alert()%s", service)
	return validateTick(chronograf.TICKScript(script))
}

func formatTick(tickscript string) (chronograf.TICKScript, error) {
	node, err := ast.Parse(tickscript)
	if err != nil {
		return "", err
	}

	output := new(bytes.Buffer)
	node.Format(output, "", true)
	return chronograf.TICKScript(output.String()), nil
}

func validateTick(script chronograf.TICKScript) error {
	_, err := newPipeline(script)
	return err
}

func newPipeline(script chronograf.TICKScript) (*pipeline.Pipeline, error) {
	edge := pipeline.StreamEdge
	if strings.Contains(string(script), "batch") {
		edge = pipeline.BatchEdge
	}

	scope := stateful.NewScope()
	predefinedVars := map[string]tick.Var{}
	return pipeline.CreatePipeline(string(script), edge, scope, &deadman{}, predefinedVars)
}

// deadman is an empty implementation of a kapacitor DeadmanService to allow CreatePipeline
var _ pipeline.DeadmanService = &deadman{}

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
