package platform

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

const maxIDLength = 16

// ID is a unique identifier.
type ID struct {
	value *uint64
}

// IDGenerator represents a generator for IDs.
type IDGenerator interface {
	// ID creates unique byte slice ID.
	ID() ID
}

// IDFromString creates an ID from a given string
func IDFromString(idstr string) (*ID, error) {
	var id ID
	err := id.DecodeFromString(idstr)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

// Empty tells whether the ID is empty or not.
func (i *ID) Empty() bool {
	return i.value == nil
}

// Decode parses b as a hex-encoded byte-slice-string.
func (i *ID) Decode(b []byte) error {
	if len(b) != maxIDLength {
		return fmt.Errorf("input must be an array of 8 bytes")
	}

	dst := make([]byte, hex.DecodedLen(maxIDLength))
	_, err := hex.Decode(dst, b)
	if err != nil {
		return err
	}
	out := binary.BigEndian.Uint64(dst)
	i.value = &out
	return nil
}

// DecodeFromString parses s as a hex-encoded string.
func (i *ID) DecodeFromString(s string) error {
	return i.Decode([]byte(s))
}

// Encode converts ID to a hex-encoded byte-slice-string.
func (i ID) Encode() ([]byte, error) {
	if i.Empty() {
		return nil, fmt.Errorf("cannot encode an empty ID")
	}
	b := make([]byte, hex.DecodedLen(maxIDLength))
	binary.BigEndian.PutUint64(b, *i.value)

	dst := make([]byte, hex.EncodedLen(len(b)))
	hex.Encode(dst, b)
	return dst, nil
}

// String returns the ID as a hex encoded string
func (i ID) String() string {
	res, err := i.Encode()
	if err != nil {
		return ""
	}
	return string(res)
}

// UnmarshalJSON implements JSON unmarshaller for IDs.
func (i *ID) UnmarshalJSON(b []byte) error {
	b = b[1 : len(b)-1]
	return i.Decode(b)
}

// MarshalJSON implements JSON marshaller for IDs.
func (i ID) MarshalJSON() ([]byte, error) {
	id, err := i.Encode()
	if err != nil {
		return nil, err
	}
	return json.Marshal(string(id[:]))
}
