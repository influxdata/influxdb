package platform

import (
	"bytes"
	"reflect"
	"testing"
)

func TestIDFromString(t *testing.T) {
	tests := []struct {
		name    string
		idstr   string
		want    *ID
		wantErr bool
	}{
		{
			name:  "Is able to decode an id",
			idstr: "020f755c3c082000",
			want:  &ID{0x02, 0x0f, 0x75, 0x5c, 0x3c, 0x08, 0x20, 0x00},
		},
		{
			name:    "It should not be able to decode an id that's not hex",
			idstr:   "gggggggggggggg",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := IDFromString(tt.idstr)
			if (err != nil) != tt.wantErr {
				t.Errorf("IDFromString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("IDFromString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDecodeFromString(t *testing.T) {
	var id1 ID
	err := id1.DecodeFromString("020f755c3c082000")
	if err != nil {
		t.Errorf(err.Error())
	}
	want1 := []byte{48, 50, 48, 102, 55, 53, 53, 99, 51, 99, 48, 56, 50, 48, 48, 48}
	byte1 := id1.Encode()
	if !bytes.Equal(want1, byte1) {
		t.Errorf("encoding error")
	}
}

func TestDecodeFromShorterString(t *testing.T) {
	var id1 ID
	err := id1.DecodeFromString("020f75")
	if err != nil {
		t.Errorf(err.Error())
	}
	want1 := []byte{48, 50, 48, 102, 55, 53, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48}
	byte1 := id1.Encode()
	if !bytes.Equal(want1, byte1) {
		t.Errorf("encoding error")
	}

	if id1.String() != "020f750000000000" {
		t.Errorf("wrong id")
	}
}

func TestDecodeFromLongerString(t *testing.T) {
	var id1 ID
	err := id1.DecodeFromString("020f755c3c082000aaa")
	if err != nil {
		t.Errorf(err.Error())
	}
	want1 := []byte{48, 50, 48, 102, 55, 53, 53, 99, 51, 99, 48, 56, 50, 48, 48, 48}
	byte1 := id1.Encode()
	if !bytes.Equal(want1, byte1) {
		t.Errorf("encoding error")
	}
}

// todo
// func TestDecodeFromEmptyString(t *testing.T) {}
