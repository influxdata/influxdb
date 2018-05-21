package id

import (
	"encoding/hex"
	"encoding/json"
)

// TODO(nathanielc): This code is copied, once we have the monorepos sorted, we need to import this code.

// ID is a unique identifier.
type ID []byte

// Decode parses b as a hex-encoded byte-slice-string.
func (i *ID) Decode(b []byte) error {
	dst := make([]byte, hex.DecodedLen(len(b)))
	_, err := hex.Decode(dst, b)
	if err != nil {
		return err
	}
	*i = dst
	return nil
}

// DecodeFromString parses s as a hex-encoded string.
func (i *ID) DecodeFromString(s string) error {
	return i.Decode([]byte(s))
}

// Encode converts ID to a hex-encoded byte-slice-string.
func (i ID) Encode() []byte {
	dst := make([]byte, hex.EncodedLen(len(i)))
	hex.Encode(dst, i)
	return dst
}

// String returns the ID as a hex encoded string
func (i *ID) String() string {
	return string(i.Encode())
}

// UnmarshalJSON implements JSON unmarshaller for IDs.
func (i *ID) UnmarshalJSON(b []byte) error {
	b = b[1 : len(b)-1]
	return i.Decode(b)
}

// MarshalJSON implements JSON marshaller for IDs.
func (i ID) MarshalJSON() ([]byte, error) {
	id := i.Encode()
	return json.Marshal(string(id[:]))
}
