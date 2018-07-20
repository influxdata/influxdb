package platform

import (
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
