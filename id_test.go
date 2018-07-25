package platform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func TestIDFromString(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		want    ID
		wantErr bool
		err     string
	}{
		{
			name: "Should be able to decode an all zeros ID",
			id:   "0000000000000000",
			want: ID(0),
		},
		{
			name: "Should be able to decode an ID",
			id:   "020f755c3c082000",
			want: ID(148466351731122176),
		},
		{
			name:    "Should not be able to decode a non hex ID",
			id:      "gggggggggggggggg",
			wantErr: true,
			err:     "encoding/hex: invalid byte: U+0067 'g'",
		},
		{
			name:    "Should not be able to decode inputs with length other than 16 bytes",
			id:      "abc",
			wantErr: true,
			err:     fmt.Sprintf("input must be an array of %d bytes", IDLength),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := IDFromString(tt.id)

			// Check negative test cases
			if (err != nil) && tt.wantErr {
				if tt.err != err.Error() {
					t.Errorf("IDFromString() errors out \"%s\", want \"%s\"", err, tt.err)
				}
				return
			}

			// Check positive test cases
			if !reflect.DeepEqual(*got, tt.want) && !tt.wantErr {
				t.Errorf("IDFromString() outputs %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDecodeFromString(t *testing.T) {
	var id ID
	err := id.DecodeFromString("020f755c3c082000")
	if err != nil {
		t.Errorf(err.Error())
	}
	want := []byte{48, 50, 48, 102, 55, 53, 53, 99, 51, 99, 48, 56, 50, 48, 48, 48}
	got := id.Encode()
	if !bytes.Equal(want, got) {
		t.Errorf("encoding error")
	}
	if id.String() != "020f755c3c082000" {
		t.Errorf("expecting string representation to contain the right value")
	}
}

func TestEncode(t *testing.T) {
	var id ID
	want := []byte{48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48}
	got := id.Encode()
	if !bytes.Equal(got, want) {
		t.Errorf("encoding an empty ID must result in a zeros ID")
	}

	id.DecodeFromString("5ca1ab1eba5eba11")
	want = []byte{53, 99, 97, 49, 97, 98, 49, 101, 98, 97, 53, 101, 98, 97, 49, 49}
	got = id.Encode()
	if !bytes.Equal(want, got) {
		t.Errorf("encoding error")
	}
	if id.String() != "5ca1ab1eba5eba11" {
		t.Errorf("expecting string representation to contain the right value")
	}
}

func TestDecodeFromShorterString(t *testing.T) {
	var id ID
	err := id.DecodeFromString("020f75")
	if err == nil {
		t.Errorf("expecting shorter inputs to error")
	}
	if id.String() != "0000000000000000" {
		t.Errorf("expecting empty ID to be serialized into empty string")
	}
}

func TestDecodeFromLongerString(t *testing.T) {
	var id ID
	err := id.DecodeFromString("020f755c3c082000aaa")
	if err == nil {
		t.Errorf("expecting shorter inputs to error")
	}
	if id.String() != "0000000000000000" {
		t.Errorf("expecting empty ID to be serialized into empty string")
	}
}

func TestDecodeFromEmptyString(t *testing.T) {
	var id ID
	err := id.DecodeFromString("")
	if err == nil {
		t.Errorf("expecting empty inputs to error")
	}
	if id.String() != "0000000000000000" {
		t.Errorf("expecting empty ID to be serialized into empty string")
	}
}

func TestMarshalling(t *testing.T) {
	init := "ca55e77eca55e77e"
	id1, err := IDFromString(init)
	if err != nil {
		t.Errorf(err.Error())
	}

	serialized, err := json.Marshal(id1)
	if err != nil {
		t.Errorf(err.Error())
	}

	var id2 ID
	json.Unmarshal(serialized, &id2)

	if !bytes.Equal(id1.Encode(), id2.Encode()) {
		t.Errorf("error marshalling/unmarshalling ID")
	}
}
