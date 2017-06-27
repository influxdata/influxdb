package server

import (
	"reflect"
	"testing"
)

func TestNewCustomLinks(t *testing.T) {
	tests := []struct {
		name    string
		args    map[string]string
		want    []CustomLink
		wantErr bool
	}{
		{
			name: "Unknown error in NewCustomLinks",
			args: map[string]string{
				"cubeapple": "https://cube.apple",
			},
			want: []CustomLink{
				{
					Name: "cubeapple",
					URL:  "https://cube.apple",
				},
			},
		},
		{
			name: "CustomLink missing Name",
			args: map[string]string{
				"": "https://cube.apple",
			},
			wantErr: true,
		},
		{
			name: "CustomLink missing URL",
			args: map[string]string{
				"cubeapple": "",
			},
			wantErr: true,
		},
		{
			name: "Missing protocol scheme",
			args: map[string]string{
				"cubeapple": ":k%8a#",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		got, err := NewCustomLinks(tt.args)
		if (err != nil) != tt.wantErr {
			t.Errorf("%q. NewCustomLinks() error = %v, wantErr %v", tt.name, err, tt.wantErr)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%q. NewCustomLinks() = %v, want %v", tt.name, got, tt.want)
		}
	}
}
