package platform

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// IDLength is the exact length a byte slice must have in order to be decoded into an ID
const IDLength = 16

// ID is a unique identifier.
type ID uint64

// IDGenerator represents a generator for IDs.
type IDGenerator interface {
	// ID creates unique byte slice ID.
	ID() *ID
}

// IDFromString creates an ID from a given string.
func IDFromString(str string) (*ID, error) {
	var id ID
	err := id.DecodeFromString(str)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

// Decode parses b as a hex-encoded byte-slice-string.
func (i *ID) Decode(b []byte) error {
	if len(b) != IDLength {
		return fmt.Errorf("input must be an array of %d bytes", IDLength)
	}

	dst := make([]byte, hex.DecodedLen(IDLength))
	_, err := hex.Decode(dst, b)
	if err != nil {
		return err
	}
	*i = ID(binary.BigEndian.Uint64(dst))
	return nil
}

// DecodeFromString parses s as a hex-encoded string.
func (i *ID) DecodeFromString(s string) error {
	return i.Decode([]byte(s))
}

// Encode converts ID to a hex-encoded byte-slice-string.
func (i ID) Encode() []byte {
	b := make([]byte, hex.DecodedLen(IDLength))
	binary.BigEndian.PutUint64(b, uint64(i))

	dst := make([]byte, hex.EncodedLen(len(b)))
	hex.Encode(dst, b)
	return dst
}

// String returns the ID as a hex encoded string
func (i ID) String() string {
	return string(i.Encode())
}

// UnmarshalJSON implements JSON unmarshaller for IDs.
func (i *ID) UnmarshalJSON(b []byte) error {
	b = b[1 : len(b)-1]
	return i.Decode(b)
}

// MarshalJSON implements JSON marshaller for IDs.
func (i ID) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(i.Encode()))
}
