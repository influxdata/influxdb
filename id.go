package platform

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
)

const maxIDLength = 16

// ID is a unique identifier.
type ID uint64

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

// Decode parses b as a hex-encoded byte-slice-string.
func (i *ID) Decode(b []byte) error {
	dst := make([]byte, hex.DecodedLen(maxIDLength))
	if len(b) > maxIDLength {
		b = b[:maxIDLength]
	}
	_, err := hex.Decode(dst, b)
	if err != nil {
		return err
	}
	*i = ID(binary.LittleEndian.Uint64(dst))
	return nil
}

// DecodeFromString parses s as a hex-encoded string.
func (i *ID) DecodeFromString(s string) error {
	return i.Decode([]byte(s))
}

// Encode converts ID to a hex-encoded byte-slice-string.
func (i ID) Encode() []byte {
	b := make([]byte, hex.DecodedLen(maxIDLength))
	binary.LittleEndian.PutUint64(b, uint64(i))

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
	id := i.Encode()
	return json.Marshal(string(id[:]))
}
