package server

import (
	"context"
	"net/http"
	"testing"

	"github.com/bouk/httprouter"
)

// WithContext is a helper function to cut down on boilerplate in server test files
func WithContext(ctx context.Context, r *http.Request, kv map[string]string) *http.Request {
	params := make(httprouter.Params, 0, len(kv))
	for k, v := range kv {
		params = append(params, httprouter.Param{
			Key:   k,
			Value: v,
		})
	}
	return r.WithContext(httprouter.WithParams(ctx, params))
}

func Test_validBasepath(t *testing.T) {
	type args struct {
		basepath string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Basepath can be empty",
			args: args{
				basepath: "",
			},
			want: true,
		},
		{
			name: "Basepath is not empty and valid",
			args: args{
				basepath: "/russ",
			},
			want: true,
		},
		{
			name: "Basepath can include numbers, hyphens, and underscores",
			args: args{
				basepath: "/3shishka-bob/-rus4s_rus-1_s-",
			},
			want: true,
		},
		{
			name: "Basepath is not empty and invalid - no slashes",
			args: args{
				basepath: "russ",
			},
			want: false,
		},
		{
			name: "Basepath is not empty and invalid - extra slashes",
			args: args{
				basepath: "//russ//",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validBasepath(tt.args.basepath); got != tt.want {
				t.Errorf("validBasepath() = %v, want %v", got, tt.want)
			}
		})
	}
}
