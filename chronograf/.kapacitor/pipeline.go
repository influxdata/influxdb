package kapacitor

import (
	"bytes"
	"encoding/json"

	"github.com/influxdata/kapacitor/pipeline"
	totick "github.com/influxdata/kapacitor/pipeline/tick"
	"github.com/influxdata/platform/chronograf"
)

// MarshalTICK converts tickscript to JSON representation
func MarshalTICK(script string) ([]byte, error) {
	pipeline, err := newPipeline(chronograf.TICKScript(script))
	if err != nil {
		return nil, err
	}
	return json.MarshalIndent(pipeline, "", "    ")
}

// UnmarshalTICK converts JSON to tickscript
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
