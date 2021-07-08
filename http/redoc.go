package http

import (
	"fmt"
	"net/http"
)

const index = `<!DOCTYPE html>
<html>
  <head>
    <title>InfluxDB 2 API</title>
    <!-- needed for adaptive design -->
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <!--
    ReDoc doesn't change outer page styles
    -->
    <style>
      body {
        margin: 0;
        padding: 0;
      }
    </style>
  </head>
  <body>
    <redoc spec-url='%s' suppressWarnings=true></redoc>
	<script src="https://cdn.jsdelivr.net/npm/redoc/bundles/redoc.standalone.js"> </script>
  </body>
</html>
`

// Redoc servers the swagger JSON using the redoc package.
func Redoc(swagger string) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)

		_, _ = w.Write([]byte(fmt.Sprintf(index, swagger)))
	})
}
