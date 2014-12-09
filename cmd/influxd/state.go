package main

import (
	"encoding/json"
	"io"
)

// State represents high-level state of the InfluxDB server that must be persisted
// across restarts.
type State struct {
	// Local indicates whether the server should run in stand-alone mode.
	Local bool `json:"local"`
}

// StateEncoder encodes a State to a writer.
type StateEncoder struct {
	w io.Writer
}

// NewStateEncoder returns a new instance of StateEncoder attached to a writer.
func NewStateEncoder(w io.Writer) *StateEncoder {
	return &StateEncoder{w}
}

// Encode marshals the state to the encoder's writer.
func (enc *StateEncoder) Encode(s *State) error {
	return json.NewEncoder(enc.w).Encode(&s)
}

// StateDecoder decodes a State from a reader.
type StateDecoder struct {
	r io.Reader
}

// NewStateDecoder returns a new instance of StateDecoder attached to a reader.
func NewStateDecoder(r io.Reader) *StateDecoder {
	return &StateDecoder{r}
}

// Decode marshals the State to the decoder's reader.
func (dec *StateDecoder) Decode(s *State) error {
	if err := json.NewDecoder(dec.r).Decode(&s); err != nil {
		return err
	}
	return nil
}
