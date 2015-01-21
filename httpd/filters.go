package httpd

import (
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/influxdb/influxdb"
)

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
			username, password, err := getUsernameAndPassword(r)
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

		inner.ServeHTTP(w, r)

		weblog.Printf(
			"%s %s %s %s %s",
			r.RemoteAddr,
			r.Method,
			r.RequestURI,
			name,
			time.Since(start),
		)
	})
}

func recovery(inner http.Handler, name string, weblog *log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		inner.ServeHTTP(w, r)

		if err := recover(); err != nil {
			weblog.Printf(
				"%s %s %s %s %s",
				r.Method,
				r.RequestURI,
				name,
				time.Since(start),
				err,
			)
		}
	})
}
