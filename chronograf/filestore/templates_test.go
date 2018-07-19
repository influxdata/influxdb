package filestore

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func Test_templated(t *testing.T) {
	tests := []struct {
		name    string
		content []string
		data    interface{}
		want    []byte
		wantErr bool
	}{
		{
			name: "files with templates are rendered correctly",
			content: []string{
				"{{ .MYVAR }}",
			},
			data: map[string]string{
				"MYVAR": "howdy",
			},
			want: []byte("howdy"),
		},
		{
			name: "missing key gives an error",
			content: []string{
				"{{ .MYVAR }}",
			},
			wantErr: true,
		},
		{
			name:    "no files make me an error!",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filenames := make([]string, len(tt.content))
			for i, c := range tt.content {
				f, err := ioutil.TempFile("", "")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.Write([]byte(c)); err != nil {
					t.Fatal(err)
				}
				filenames[i] = f.Name()
				defer os.Remove(f.Name())
			}
			got, err := templated(tt.data, filenames...)
			if (err != nil) != tt.wantErr {
				t.Errorf("templated() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("templated() = %v, want %v", got, tt.want)
			}
		})
	}
}
