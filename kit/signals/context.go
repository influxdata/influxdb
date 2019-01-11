package signals

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// WithSignals returns a context that is canceled with any signal in sigs.
func WithSignals(ctx context.Context, sigs ...os.Signal) context.Context {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, sigs...)

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()
		select {
		case <-ctx.Done():
			return
		case <-sigCh:
			return
		}
	}()
	return ctx
}

// WithStandardSignals cancels the context on os.Interrupt, syscall.SIGTERM.
func WithStandardSignals(ctx context.Context) context.Context {
	return WithSignals(ctx, os.Interrupt, syscall.SIGTERM)
}
