package server

//go:generate env GO111MODULE=on go run github.com/kevinburke/go-bindata/go-bindata -o swagger_gen.go -tags assets -ignore go -nocompress -pkg server .

import "net/http"

// Spec servers the swagger.json file from bindata
func Spec() http.HandlerFunc {
	swagger, err := Asset("swagger.json")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(swagger)
	})
}
