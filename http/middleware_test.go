package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/v2/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestLoggingMW(t *testing.T) {
	newDebugLogger := func(t *testing.T) (*zap.Logger, *bytes.Buffer) {
		t.Helper()

		var buf bytes.Buffer
		log, err := (&logger.Config{
			Format: "auto",
			Level:  zapcore.DebugLevel,
		}).New(&buf)
		if err != nil {
			t.Fatal(err)
		}

		return log, &buf
	}

	urlWithQueries := func(path string, queryPairs ...string) url.URL {
		u := url.URL{
			Path: path,
		}
		params := u.Query()
		for i := 0; i < len(queryPairs)/2; i++ {
			k, v := queryPairs[i*2], queryPairs[i*2+1]
			params.Add(k, v)
		}
		return u
	}

	encodeBody := func(t *testing.T, k, v string) *bytes.Buffer {
		t.Helper()

		m := map[string]string{k: v}

		var buf bytes.Buffer
		err := json.NewEncoder(&buf).Encode(m)
		if err != nil {
			t.Fatal(err)
		}

		return &buf
	}

	getKVPair := func(s string) (string, string) {
		k, v, _ := strings.Cut(s, "=")
		if v != "" {
			v = strings.TrimSuffix(v, "\n")
		}
		return k, v
	}

	echoHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var m map[string]string
		err := json.NewDecoder(r.Body).Decode(&m)
		if err != nil {
			w.WriteHeader(422)
			return
		}
		defer r.Body.Close()

		// set a non 200 status code here
		w.WriteHeader(202)

		_, err = w.Write([]byte("ack"))
		if err != nil {
			w.WriteHeader(500)
			return
		}
	})

	teeReader := func(r *bytes.Buffer, w io.Writer) io.Reader {
		if r == nil {
			return nil
		}
		return io.TeeReader(r, w)
	}

	type testRun struct {
		name       string
		method     string
		path       string
		queryPairs []string
		hasBody    bool
		hideBody   bool
	}

	testEndpoint := func(tt testRun) func(t *testing.T) {
		fn := func(t *testing.T) {
			t.Helper()

			log, buf := newDebugLogger(t)

			reqURL := urlWithQueries(tt.path, tt.queryPairs...)
			var body *bytes.Buffer
			if tt.hasBody {
				body = encodeBody(t, "bin", "shake")
			}

			var trackerBuf bytes.Buffer
			req := httptest.NewRequest(tt.method, reqURL.String(), teeReader(body, &trackerBuf))
			rec := httptest.NewRecorder()

			LoggingMW(log)(echoHandler).ServeHTTP(rec, req)

			expected := map[string]string{
				"method":         tt.method,
				"host":           "example.com",
				"path":           reqURL.Path,
				"query":          reqURL.RawQuery,
				"proto":          "HTTP/1.1",
				"status_code":    strconv.Itoa(rec.Code),
				"response_size":  strconv.Itoa(rec.Body.Len()),
				"content_length": strconv.FormatInt(req.ContentLength, 10),
			}
			if tt.hasBody {
				expected["body"] = fmt.Sprintf("%q", trackerBuf.String())
			}

			// skip first 4 pairs, is the base logger stuff
			for _, pair := range strings.Split(buf.String(), " ")[4:] {
				k, v := getKVPair(pair)
				switch k {
				case "took", "remote":
					if v == "" {
						t.Errorf("unexpected value(%q) for key(%q): expected=non empty string", v, k)
					}
				case "body":
					if tt.hideBody && v != "" {
						t.Errorf("expected body to be \"\" but got=%q", v)
						continue
					}
					fallthrough
				case "user_agent":
				default:
					if expectedV := expected[k]; expectedV != v {
						t.Errorf("unexpected value(%q) for key(%q): expected=%q", v, k, expectedV)
					}
				}
			}
		}

		return fn
	}

	t.Run("logs the http request", func(t *testing.T) {
		tests := []testRun{
			{
				name:       "GET",
				method:     "GET",
				path:       "/foo",
				queryPairs: []string{"dodgers", "are", "the", "terrible"},
			},
			{
				name:       "POST",
				method:     "POST",
				path:       "/foo",
				queryPairs: []string{"bin", "shake"},
				hasBody:    true,
			},
			{
				name:       "PUT",
				method:     "PUT",
				path:       "/foo",
				queryPairs: []string{"ninja", "turtles"},
				hasBody:    true,
			},
			{
				name:       "PATCH",
				method:     "PATCH",
				path:       "/foo",
				queryPairs: []string{"peach", "daisy", "mario", "luigi"},
				hasBody:    true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, testEndpoint(tt))
		}
	})

	t.Run("does not log body for blacklisted routes", func(t *testing.T) {
		tests := []testRun{
			{
				name:   "signin",
				method: "POSTT",
				path:   "/api/v2/signin",
			},
			{
				name:   "signout",
				method: "POST",
				path:   "/api/v2/signout",
			},
			{
				name:   "me path",
				method: "POST",
				path:   "/api/v2/me",
			},
			{
				name:   "me password path",
				method: "POST",
				path:   "/api/v2/me/password",
			},
			{
				name:   "user password path",
				method: "POST",
				path:   "/api/v2/users/user-id/password",
			},
			{
				name:   "write path",
				method: "POST",
				path:   "/api/v2/write",
			},
			{
				name:   "legacy write path",
				method: "POST",
				path:   "/write",
			},
			{
				name:   "orgs id secrets path",
				method: "PATCH",
				path:   "/api/v2/orgs/org-id/secrets",
			},
			{
				name:   "orgs id secrets delete path",
				method: "POST",
				path:   "/api/v2/orgs/org-id/secrets/delete",
			},
			{
				name:   "setup path",
				method: "POST",
				path:   "/api/v2/setup",
			},
			{
				name:   "notifications endpoints path",
				method: "POST",
				path:   "/api/v2/notificationEndpoints",
			},
			{
				name:   "notifications endpoints id path",
				method: "PUT",
				path:   "/api/v2/notificationEndpoints/notification-id",
			},
		}

		for _, tt := range tests {
			tt.hasBody = true
			tt.hideBody = true
			t.Run(tt.name, testEndpoint(tt))
		}
	})

}
