package log_control

import (
	"net/http"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type middleware struct {
	l zap.AtomicLevel
	h http.Handler
}

// NewControl creates a enw log controll middleware and returns a http handler
func NewControl(l zap.AtomicLevel, h http.Handler) http.Handler {
	return &middleware{
		l: l,
		h: h,
	}
}

// ServeHTTP serves /debug/log
func (m *middleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	// pass through anything not intended for me
	if r.URL.Path != "/debug/log" {
		if m.h != nil {
			m.h.ServeHTTP(w, r)
			return
		}

		// We cant handle this request.
		w.WriteHeader(http.StatusNotFound)
		return
	}

	lvl := r.FormValue("level")
	dur := r.FormValue("duration")

	var clvl zapcore.Level
	err := clvl.Set(lvl)
	if err != nil {
		w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	tdur, err := time.ParseDuration(dur)
	if err != nil {
		w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	oldlvl := m.l.Level()
	m.l.SetLevel(clvl)
	go func() {
		<-time.After(tdur)
		m.l.SetLevel(oldlvl)
	}()

}
