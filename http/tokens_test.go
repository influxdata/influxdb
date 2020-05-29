package http

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetToken(t *testing.T) {
	type args struct {
		header string
	}
	type wants struct {
		err    error
		result string
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name: "empty header",
			args: args{
				header: "",
			},
			wants: wants{
				err: ErrAuthHeaderMissing,
			},
		},
		{
			name: "bad none empty header",
			args: args{
				header: "a bad header",
			},
			wants: wants{
				err: ErrAuthBadScheme,
			},
		},
		{
			name: "bad basic header",
			args: args{
				header: "Basic header",
			},
			wants: wants{
				err: ErrAuthBadScheme,
			},
		},
		{
			name: "good token",
			args: args{
				header: "Token tok2",
			},
			wants: wants{
				result: "tok2",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &http.Request{
				Header: make(http.Header),
			}
			req.Header.Set("Authorization", tt.args.header)
			result, err := GetToken(req)
			if err != tt.wants.err {
				t.Errorf("err incorrect want %v, got %v", tt.wants.err, err)
				return
			}
			if result != tt.wants.result {
				t.Errorf("result incorrect want %s, got %s", tt.wants.result, result)
			}
		})
	}

}

func TestSetToken(t *testing.T) {
	tests := []struct {
		name  string
		token string
		req   *http.Request
		want  string
	}{
		{
			name:  "adding token to Authorization header",
			token: "1234",
			req:   httptest.NewRequest("GET", "/", nil),
			want:  "Token 1234",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SetToken(tt.token, tt.req)
			if got := tt.req.Header.Get("Authorization"); got != tt.want {
				t.Errorf("SetToken() want %s, got %s", tt.want, got)
			}
		})
	}
}
