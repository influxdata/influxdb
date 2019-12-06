package http

import (
	"net/http"

	useragent "github.com/mileusna/useragent"
)

func userAgent(r *http.Request) string {
	header := r.Header.Get("User-Agent")
	if header == "" {
		return "unknown"
	}

	ua := useragent.Parse(header)
	return ua.Name
}
