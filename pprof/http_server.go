package pprof

import (
	"fmt"
	"io"
	"net/http"
	httppprof "net/http/pprof"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform/errors"

	"github.com/go-chi/chi"
	ihttp "github.com/influxdata/influxdb/v2/kit/transport/http"
)

type Handler struct {
	chi.Router
}

func NewHTTPHandler(profilingEnabled bool) *Handler {
	r := chi.NewRouter()
	r.Route("/pprof", func(r chi.Router) {
		if !profilingEnabled {
			r.NotFound(profilingDisabledHandler)
			return
		}
		r.Get("/cmdline", httppprof.Cmdline)
		r.Get("/profile", httppprof.Profile)
		r.Get("/symbol", httppprof.Symbol)
		r.Get("/trace", httppprof.Trace)
		r.Get("/all", archiveProfilesHandler)
		r.Mount("/", http.HandlerFunc(httppprof.Index))
	})

	return &Handler{r}
}

func profilingDisabledHandler(w http.ResponseWriter, r *http.Request) {
	ihttp.WriteErrorResponse(r.Context(), w, errors.EForbidden, "profiling disabled")
}

func archiveProfilesHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// We parse the form here so that we can use the http.Request.Form map.
	//
	// Otherwise we'd have to use r.FormValue() which makes it impossible to
	// distinguish between a form value that exists and has no value and one that
	// does not exist at all.
	if err := r.ParseForm(); err != nil {
		ihttp.WriteErrorResponse(ctx, w, errors.EInternal, err.Error())
		return
	}

	// In the following two blocks, we check if the request should include cpu
	// profiles and a trace log.
	//
	// Since the submitted form can contain multiple version of a variable like:
	//
	//   http://localhost:8086?cpu=1s&cpu=30s&trace=3s&cpu=5s
	//
	// the question arises: which value should we use?  We choose to use the LAST
	// value supplied.
	//
	// This is an edge case but if for some reason, for example, a url is
	// programmatically built and multiple values are supplied, this will do what
	// is expected.
	//
	var traceDuration, cpuDuration time.Duration

	// last() returns either the last item from a slice of strings or an empty
	// string if the supplied slice is empty or nil.
	last := func(s []string) string {
		if len(s) == 0 {
			return ""
		}
		return s[len(s)-1]
	}

	// If trace exists as a form value, add it to the profiles slice with the
	// decoded duration.
	//
	// Requests for a trace should look like:
	//
	//  ?trace=10s
	//
	if vals, exists := r.Form["trace"]; exists {
		// parse the duration encoded in the last "trace" value supplied.
		val := last(vals)
		duration, err := time.ParseDuration(val)

		// If we can't parse the duration or if the user supplies a negative
		// number, return an appropriate error status and message.
		//
		// In this case it is a StatusBadRequest (400) since the problem is in the
		// supplied form data.
		if duration < 0 {
			ihttp.WriteErrorResponse(ctx, w, errors.EInvalid, "negative trace durations not allowed")
			return
		}

		if err != nil {
			ihttp.WriteErrorResponse(ctx, w, errors.EInvalid, fmt.Sprintf("could not parse supplied duration for trace %q", val))
			return
		}

		// Trace files can get big. Lets clamp the maximum trace duration to 45s.
		if duration > 45*time.Second {
			ihttp.WriteErrorResponse(ctx, w, errors.EInvalid, "cannot trace for longer than 45s")
			return
		}

		traceDuration = duration
	}

	// Capturing CPU profiles is a little trickier. The preferred way to send the
	// the cpu profile duration is via the supplied "cpu" variable's value.
	//
	// The duration should be encoded as a Go duration that can be parsed by
	// time.ParseDuration().
	//
	// In the past users were encouraged to assign any value to cpu and provide
	// the duration in a separate "seconds" value.
	//
	// The code below handles both -- first it attempts to use the old method
	// which would look like:
	//
	//    ?cpu=foobar&seconds=10
	//
	// Then it attempts to ascertain the duration provided with:
	//
	//    ?cpu=10s
	//
	// This preserves backwards compatibility with any tools that have been
	// written to gather profiles.
	//
	if vals, exists := r.Form["cpu"]; exists {
		duration := time.Second * 30
		val := last(vals)

		// getDuration is a small function literal that encapsulates the logic
		// for getting the duration from either the "seconds" form value or from
		// the value assigned to "cpu".
		getDuration := func() (time.Duration, error) {
			if seconds, exists := r.Form["seconds"]; exists {
				s, err := strconv.ParseInt(last(seconds), 10, 64)
				if err != nil {
					return 0, err
				}
				return time.Second * time.Duration(s), nil
			}
			// see if the value of cpu is a duration like:  cpu=10s
			return time.ParseDuration(val)
		}

		duration, err := getDuration()
		if err != nil {
			ihttp.WriteErrorResponse(ctx, w, errors.EInvalid, fmt.Sprintf("could not parse supplied duration for cpu profile %q", val))
			return
		}

		cpuDuration = duration
	}

	tarstream, err := collectAllProfiles(ctx, traceDuration, cpuDuration)
	if err != nil {
		ihttp.WriteErrorResponse(ctx, w, errors.EInternal, err.Error())
		return
	}
	_, _ = io.Copy(w, tarstream)
}
