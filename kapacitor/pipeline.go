package kapacitor

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/influxdata/kapacitor/pipeline"
	totick "github.com/influxdata/kapacitor/pipeline/tick"
	"github.com/influxdata/kapacitor/tick"
	"github.com/influxdata/kapacitor/tick/stateful"
)

func MarshalTICK(script string) ([]byte, error) {
	edge := pipeline.StreamEdge
	if strings.Contains(script, "batch") {
		edge = pipeline.BatchEdge
	}

	scope := stateful.NewScope()
	predefinedVars := map[string]tick.Var{}
	pipeline, err := pipeline.CreatePipeline(script, edge, scope, &deadman{}, predefinedVars)
	if err != nil {
		return nil, err
	}
	return json.MarshalIndent(pipeline, "", "    ")
}

func UnmarshalTICK(octets []byte) (string, error) {
	pipe := &pipeline.Pipeline{}
	if err := pipe.Unmarshal(octets); err != nil {
		return "", err
	}

	ast := totick.AST{}
	err := ast.Build(pipe)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	ast.Program.Format(&buf, "", false)
	return buf.String(), nil
}
