package server

import (
	"context"
	"net/http"

	"github.com/bouk/httprouter"
)

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
