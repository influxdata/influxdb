package server

import "net/http"

func location(w http.ResponseWriter, self string) {
	w.Header().Add("Location", self)
}
