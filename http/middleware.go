package http

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"go.uber.org/zap"
)

// LoggingMW middleware for logging inflight http requests.
func LoggingMW(log *zap.Logger) kithttp.Middleware {
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			srw := kithttp.NewStatusResponseWriter(w)

			var buf bytes.Buffer
			r.Body = &bodyEchoer{
				rc:    r.Body,
				teedR: io.TeeReader(r.Body, &buf),
			}

			defer func(start time.Time) {
				errField := zap.Skip()
				if errStr := w.Header().Get(kithttp.PlatformErrorCodeHeader); errStr != "" {
					errField = zap.Error(errors.New(errStr))
				}

				errReferenceField := zap.Skip()
				if errReference := w.Header().Get(kithttp.PlatformErrorCodeHeader); errReference != "" {
					errReferenceField = zap.String("error_code", errReference)
				}

				fields := []zap.Field{
					zap.String("method", r.Method),
					zap.String("host", r.Host),
					zap.String("path", r.URL.Path),
					zap.String("query", r.URL.Query().Encode()),
					zap.String("proto", r.Proto),
					zap.Int("status_code", srw.Code()),
					zap.Int("response_size", srw.ResponseBytes()),
					zap.Int64("content_length", r.ContentLength),
					zap.String("referrer", r.Referer()),
					zap.String("remote", r.RemoteAddr),
					zap.String("user_agent", kithttp.UserAgent(r)),
					zap.Duration("took", time.Since(start)),
					errField,
					errReferenceField,
				}

				invalidMethodFn, ok := mapURLPath(r.URL.Path)
				if !ok || !invalidMethodFn(r.Method) {
					fields = append(fields, zap.ByteString("body", buf.Bytes()))
				}

				log.Debug("Request", fields...)
			}(time.Now())

			next.ServeHTTP(srw, r)
		}
		return http.HandlerFunc(fn)
	}
}

type isValidMethodFn func(method string) bool

func mapURLPath(rawPath string) (isValidMethodFn, bool) {
	if fn, ok := blacklistEndpoints[rawPath]; ok {
		return fn, true
	}

	shiftPath := func(p string) (head, tail string) {
		p = path.Clean("/" + p)
		i := strings.Index(p[1:], "/") + 1
		if i <= 0 {
			return p[1:], "/"
		}
		return p[1:i], p[i:]
	}

	// ugh, should probably make this whole operation use a trie
	partsMatch := func(raw, source string) bool {
		return raw == source || (strings.HasPrefix(source, ":") && raw != "")
	}

	compareRawSourceURLs := func(raw, source string) bool {
		sourceHead, sourceTail := shiftPath(source)
		for rawHead, rawTail := shiftPath(rawPath); rawHead != ""; {
			if !partsMatch(rawHead, sourceHead) {
				return false
			}
			rawHead, rawTail = shiftPath(rawTail)
			sourceHead, sourceTail = shiftPath(sourceTail)
		}
		return sourceHead == ""
	}

	for sourcePath, fn := range blacklistEndpoints {
		match := compareRawSourceURLs(rawPath, sourcePath)
		if match {
			return fn, true
		}
	}

	return nil, false
}

func ignoreMethod(ignoredMethods ...string) isValidMethodFn {
	if len(ignoredMethods) == 0 {
		return func(string) bool { return true }
	}

	ignoreMap := make(map[string]bool)
	for _, method := range ignoredMethods {
		ignoreMap[method] = true
	}

	return func(method string) bool {
		return ignoreMap[method]
	}
}

const (
	prefixSetup                      = "/api/v2/setup"
	organizationsIDSecretsPath       = "/api/v2/orgs/:id/secrets"
	organizationsIDSecretsDeletePath = "/api/v2/orgs/:id/secrets/delete"
)

// TODO(@jsteenb2): make this a stronger type that handlers can register routes that should not be logged.
var blacklistEndpoints = map[string]isValidMethodFn{
	prefixSignIn:                     ignoreMethod(),
	prefixSignOut:                    ignoreMethod(),
	prefixMe:                         ignoreMethod(),
	mePasswordPath:                   ignoreMethod(),
	usersPasswordPath:                ignoreMethod(),
	"/api/v2/packages/apply":         ignoreMethod(),
	prefixWrite:                      ignoreMethod("POST"),
	organizationsIDSecretsPath:       ignoreMethod("PATCH"),
	organizationsIDSecretsDeletePath: ignoreMethod("POST"),
	prefixSetup:                      ignoreMethod("POST"),
	prefixNotificationEndpoints:      ignoreMethod("POST"),
	notificationEndpointsIDPath:      ignoreMethod("PUT"),
}

type bodyEchoer struct {
	rc    io.ReadCloser
	teedR io.Reader
}

func (b *bodyEchoer) Read(p []byte) (int, error) {
	return b.teedR.Read(p)
}

func (b *bodyEchoer) Close() error {
	return b.rc.Close()
}
