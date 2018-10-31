package platform

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"unsafe"
)

// IDLength is the exact length a string (or a byte slice representing it) must have in order to be decoded into a valid ID.
const IDLength = 16

// ErrInvalidID signifies invalid IDs.
var ErrInvalidID = errors.New("invalid ID")

// ErrInvalidIDLength is returned when an ID has the incorrect number of bytes.
var ErrInvalidIDLength = errors.New("id must have a length of 16 bytes")

// ID is a unique identifier.
//
// Its zero value is not a valid ID.
type ID uint64

// IDGenerator represents a generator for IDs.
type IDGenerator interface {
	// ID creates unique byte slice ID.
	ID() ID
}

// IDFromString creates an ID from a given string.
//
// It errors if the input string does not match a valid ID.
func IDFromString(str string) (*ID, error) {
	var id ID
	err := id.DecodeFromString(str)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

// InvalidID returns a zero ID.
func InvalidID() ID {
	return 0
}

// Decode parses b as a hex-encoded byte-slice-string.
//
// It errors if the input byte slice does not have the correct length
// or if it contains all zeros.
func (i *ID) Decode(b []byte) error {
	if len(b) != IDLength {
		return ErrInvalidIDLength
	}

	res, err := strconv.ParseUint(unsafeBytesToString(b), 16, 64)
	if err != nil {
		return err
	}

	if *i = ID(res); !i.Valid() {
		return ErrInvalidID
	}
	return nil
}

func unsafeBytesToString(in []byte) string {
	src := *(*reflect.SliceHeader)(unsafe.Pointer(&in))
	dst := reflect.StringHeader{
		Data: src.Data,
		Len:  src.Len,
	}
	s := *(*string)(unsafe.Pointer(&dst))
	return s
}

// DecodeFromString parses s as a hex-encoded string.
func (i *ID) DecodeFromString(s string) error {
	return i.Decode([]byte(s))
}

// Encode converts ID to a hex-encoded byte-slice-string.
//
// It errors if the receiving ID holds its zero value.
func (i ID) Encode() ([]byte, error) {
	if !i.Valid() {
		return nil, ErrInvalidID
	}

	b := make([]byte, hex.DecodedLen(IDLength))
	binary.BigEndian.PutUint64(b, uint64(i))

	dst := make([]byte, hex.EncodedLen(len(b)))
	hex.Encode(dst, b)
	return dst, nil
}

// Valid checks whether the receiving ID is a valid one or not.
func (i ID) Valid() bool {
	return i != 0
}

// String returns the ID as a hex encoded string.
//
// Returns an empty string in the case the ID is invalid.
func (i ID) String() string {
	enc, _ := i.Encode()
	return string(enc)
}

// GoString formats the ID the same as the String method.
// Without this, when using the %#v verb, an ID would be printed as a uint64,
// so you would see e.g. 0x2def021097c6000 instead of 02def021097c6000
// (note the leading 0x, which means the former doesn't show up in searches for the latter).
func (i ID) GoString() string {
	return `"` + i.String() + `"`
}

// UnmarshalJSON implements JSON unmarshaller for IDs.
func (i *ID) UnmarshalJSON(b []byte) error {
	if b[0] == '"' {
		b = b[1:]
	}
	if b[len(b)-1] == '"' {
		b = b[:len(b)-1]
	}
	return i.Decode(b)
}

// MarshalJSON implements JSON marshaller for IDs.
func (i ID) MarshalJSON() ([]byte, error) {
	enc, err := i.Encode()
	if err != nil {
		return nil, err
	}
	return json.Marshal(string(enc))
}
