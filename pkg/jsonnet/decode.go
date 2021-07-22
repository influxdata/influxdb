package jsonnet

import (
	"encoding/json"
	"io"
	"io/ioutil"

	"github.com/google/go-jsonnet"
)

// Decoder type can decoce a jsonnet stream into the given output.
type Decoder struct {
	r io.Reader
}

// NewDecoder creates a new decoder.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

// Decode decodes the stream into the provide value.
func (d *Decoder) Decode(v interface{}) error {
	b, err := ioutil.ReadAll(d.r)
	if err != nil {
		return err
	}

	vm := jsonnet.MakeVM()
	jsonStr, err := vm.EvaluateSnippet("memory", string(b))
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(jsonStr), &v)
}
