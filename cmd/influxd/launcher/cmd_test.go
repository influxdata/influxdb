package launcher

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestInvalidFlags(t *testing.T) {
	t.Parallel()

	v2config := `
bolt-path = "/db/.influxdbv2/influxd.bolt"
engine-path = "/db/.influxdbv2/engine"
http-bind-address = ":8086"
`

	v1config := `
reporting-disabled = false

# Bind address to use for the RPC service for backup and restore.
bind-address = "127.0.0.1:8088"

[http]
  flux-enabled = false

[data]
  index-version = "inmem"`

	tests := []struct {
		name   string
		config string
		want   []string
	}{
		{
			name:   "empty config",
			config: "",
			want:   []string(nil),
		},
		{
			name:   "v2 config",
			config: v2config,
			want:   []string(nil),
		},
		{
			name:   "v1 config",
			config: v1config,
			want:   []string{"http.flux-enabled", "data.index-version", "bind-address"},
		},
		{
			name:   "mixed config",
			config: v2config + v1config,
			want:   []string{"http.flux-enabled", "data.index-version", "bind-address"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := strings.NewReader(tt.config)
			v := viper.GetViper()
			v.SetConfigType("toml")
			require.NoError(t, v.ReadConfig(r))
			got := invalidFlags(v)
			require.ElementsMatch(t, tt.want, got)
		})
	}
}

// TestHoldForStartupError covers the wait that keeps /health and /ready
// scrapeable after a failed startup. The early-return cases matter as much as
// the wait itself: each one exists to avoid delaying an exit that nothing can
// observe.
func TestHoldForStartupError(t *testing.T) {
	t.Parallel()

	// newLauncher builds a Launcher with just the fields holdForStartupError
	// reads. run() is not involved; these cases are about the wait alone.
	newLauncher := func(serving bool, done <-chan struct{}) *Launcher {
		return &Launcher{
			log:         zap.NewNop(),
			httpServing: serving,
			doneChan:    done,
		}
	}

	// requireReturnsWithin runs hold in a goroutine so a regression fails this
	// subtest in a second rather than blocking until the package test timeout.
	requireReturnsWithin := func(t *testing.T, limit time.Duration, hold func()) {
		t.Helper()
		returned := make(chan struct{})
		go func() {
			defer close(returned)
			hold()
		}()
		select {
		case <-returned:
		case <-time.After(limit):
			t.Fatalf("holdForStartupError did not return within %s", limit)
		}
	}

	open := make(chan struct{}) // never closed: Done() will not fire
	closed := make(chan struct{})
	close(closed)

	// Each case must return without waiting out the duration: there is either
	// no wait to make (non-positive d), nothing to scrape, or nothing left to
	// wait for.
	immediate := []struct {
		name    string
		serving bool
		done    <-chan struct{}
		d       time.Duration
	}{
		{"duration is zero", true, open, 0},
		{"duration is negative", true, open, -5 * time.Second},
		{"nothing is listening", false, open, time.Hour}, // runHTTP failed
		{"already done", true, closed, time.Hour},
	}
	for _, tt := range immediate {
		t.Run("returns immediately when "+tt.name, func(t *testing.T) {
			t.Parallel()
			l := newLauncher(tt.serving, tt.done)
			requireReturnsWithin(t, time.Second, func() {
				l.holdForStartupError(context.Background(), tt.d)
			})
		})
	}

	t.Run("a signal cuts the wait short", func(t *testing.T) {
		t.Parallel()
		done := make(chan struct{})
		l := newLauncher(true, done)

		go func() {
			time.Sleep(10 * time.Millisecond)
			close(done) // stands in for SIGTERM
		}()

		requireReturnsWithin(t, time.Second, func() {
			l.holdForStartupError(context.Background(), time.Hour)
		})
	})

	t.Run("waits for the duration when nothing interrupts", func(t *testing.T) {
		t.Parallel()
		l := newLauncher(true, open)

		const d = 50 * time.Millisecond
		start := time.Now()
		l.holdForStartupError(context.Background(), d)
		require.GreaterOrEqual(t, time.Since(start), d)
	})

	// The wait is only useful if what it waits with is the listener alone.
	// Everything else must already be released, or a supervisor restarting
	// influxd is blocked on state belonging to the run that just failed.
	t.Run("releases every subsystem but the listener before waiting", func(t *testing.T) {
		t.Parallel()

		var stopped []string
		closer := func(label string) labeledCloser {
			return labeledCloser{
				label: label,
				closer: func(context.Context) error {
					stopped = append(stopped, label)
					return nil
				},
			}
		}

		done := make(chan struct{})
		close(done) // cut the wait short; the teardown precedes it either way
		l := newLauncher(true, done)
		l.closers = []labeledCloser{
			closer(SubsystemPIDFile),
			closer(SubsystemEngine),
			closer(SubsystemHTTPServer), // registered last, torn down last
		}

		l.holdForStartupError(context.Background(), time.Hour)
		require.Equal(t, []string{SubsystemEngine, SubsystemPIDFile}, stopped,
			"subsystems tear down in reverse registration order, listener retained")

		require.NoError(t, l.Shutdown(context.Background()))
		require.Equal(t,
			[]string{SubsystemEngine, SubsystemPIDFile, SubsystemHTTPServer}, stopped,
			"Shutdown closes the listener the hold was left with")
	})
}

// TestLauncherShutdown_PhasedTeardown covers the contract that lets teardown
// split around the startup-error hold: every closer runs at most once across
// the phases, and Shutdown reports the failures from all of them, so one call
// site can log the whole teardown.
func TestLauncherShutdown_PhasedTeardown(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engineErr := errors.New("engine close failed")
	listenerErr := errors.New("listener close failed")

	var stopped int
	l := &Launcher{
		log: zap.NewNop(),
		closers: []labeledCloser{
			{label: SubsystemEngine, closer: func(context.Context) error {
				stopped++
				return engineErr
			}},
			{label: SubsystemHTTPServer, closer: func(context.Context) error {
				stopped++
				return listenerErr
			}},
		},
	}

	err := l.shutdownSubsystems(ctx)
	require.ErrorContains(t, err, engineErr.Error())
	require.NotContains(t, err.Error(), listenerErr.Error(),
		"the listener must still be open after the first phase")
	require.Equal(t, 1, stopped)

	err = l.Shutdown(ctx)
	require.ErrorContains(t, err, listenerErr.Error())
	require.ErrorContains(t, err, engineErr.Error(),
		"Shutdown reports the earlier phase's failures too")
	require.Equal(t, 2, stopped)

	require.EqualError(t, l.Shutdown(ctx), err.Error(),
		"a repeated Shutdown returns the same accumulated error")
	require.Equal(t, 2, stopped, "a repeated Shutdown must not re-run closers")
}
