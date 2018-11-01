package server

import (
	"fmt"
	"net/http"
)

const index = `<!DOCTYPE html>
<html>
  <head>
    <title>Chronograf API</title>
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
    <redoc spec-url='%s'></redoc>
    <script src="https://rebilly.github.io/ReDoc/releases/latest/redoc.min.js"> </script>
  </body>
</html>
`

// Redoc servers the swagger JSON using the redoc package.
func Redoc(swagger string) http.HandlerFunc {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "text/html; charset=utf-8")
		rw.WriteHeader(http.StatusOK)

		_, _ = rw.Write([]byte(fmt.Sprintf(index, swagger)))
	})
}
