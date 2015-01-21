package httpd

import (
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/influxdb/influxdb"
)

// getUsernameAndPassword returns the username and password encoded in
// a request. The credentials may be present as URL query params, or as
// a Basic Authentication header.
func basicAuthCredentials(r *http.Request) (string, string, error) {
	q := r.URL.Query()
	username, password := q.Get("u"), q.Get("p")
	if username != "" && password != "" {
		return username, password, nil
	}
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return "", "", nil
	}
	fields := strings.Split(auth, " ")
	if len(fields) != 2 {
		return "", "", fmt.Errorf("invalid Basic Authentication header")
	}
	bs, err := base64.StdEncoding.DecodeString(fields[1])
	if err != nil {
		return "", "", fmt.Errorf("invalid Base64 encoding")
	}
	fields = strings.Split(string(bs), ":")
	if len(fields) != 2 {
		return "", "", fmt.Errorf("invalid Basic Authentication value")
	}
	return fields[0], fields[1], nil
}

// authorize ensures that if user credentials are passed in, an attempt is made to authenticate that user.
// If authentication fails, an error is returned to the user.
//
// There is one exception: if there are no users in the system, authentication is not required. This
// is to facilitate bootstrapping of a system with authentication enabled.
func authorize(inner http.Handler, h *Handler, requireAuthentication bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var user *influxdb.User

		// TODO corylanou: never allow this in the future without users
		if requireAuthentication && h.server.UserCount() > 0 {
			username, password, err := basicAuthCredentials(r)
			if err != nil {
				httpError(w, err.Error(), http.StatusUnauthorized)
				return
			}
			if username == "" {
				httpError(w, "username required", http.StatusUnauthorized)
				return
			}

			user, err = h.server.Authenticate(username, password)
			if err != nil {
				httpError(w, err.Error(), http.StatusUnauthorized)
				return
			}
		}
		h.user = user
		inner.ServeHTTP(w, r)
	})
}

func cors(inner http.Handler) http.Handler {
	// I think in general we should take the standard path, and if they need custom config
	// allow to put that in the config.

	// TODO corylanou: incorporate this appropriately
	//w.Header().Add("Access-Control-Allow-Origin", "*")
	//w.Header().Add("Access-Control-Max-Age", "2592000")
	//w.Header().Add("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
	//w.Header().Add("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set(`Access-Control-Allow-Origin`, origin)
			w.Header().Set(`Access-Control-Allow-Methods`, strings.Join([]string{
				`DELETE`,
				`GET`,
				`OPTIONS`,
				`POST`,
				`PUT`,
			}, ", "))

			w.Header().Set(`Access-Control-Allow-Headers`, strings.Join([]string{
				`Accept`,
				`Accept-Encoding`,
				`Authorization`,
				`Content-Length`,
				`Content-Type`,
				`X-CSRF-Token`,
				`X-HTTP-Method-Override`,
			}, ", "))
		}

		if r.Method == "OPTIONS" {
			return
		}

		inner.ServeHTTP(w, r)
	})
}

func logging(inner http.Handler, name string, weblog *log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		record := &responseLogger{
			w: w,
		}

		inner.ServeHTTP(record, r)

		logLine := buildLogLine(record, r, start)

		weblog.Println(logLine)
	})
}

func recovery(inner http.Handler, name string, weblog *log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		record := &responseLogger{
			w: w,
		}

		inner.ServeHTTP(record, r)

		if err := recover(); err != nil {
			logLine := buildLogLine(record, r, start)
			logLine = fmt.Sprintf(`%s [err:%s]`, logLine, err)

			weblog.Println(logLine)
		}
	})
}
