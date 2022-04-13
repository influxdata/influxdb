//go:build !windows

package signals

import (
	"context"
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"
)

func ExampleWithSignals() {
	ctx := WithSignals(context.Background(), syscall.SIGUSR1)
	go func() {
		time.Sleep(500 * time.Millisecond) // after some time SIGUSR1 is sent
		// mimicking a signal from the outside
		syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
	}()

	<-ctx.Done()
	fmt.Println("finished")
	// Output:
	// finished
}

func Example_withUnregisteredSignals() {
	dctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond*100)
	defer cancel()

	ctx := WithSignals(dctx, syscall.SIGUSR1)
	go func() {
		time.Sleep(10 * time.Millisecond) // after some time SIGUSR2 is sent
		// mimicking a signal from the outside, WithSignals will not handle it
		syscall.Kill(syscall.Getpid(), syscall.SIGUSR2)
	}()

	<-ctx.Done()
	fmt.Println("finished")
	// Output:
	// finished
}

func TestWithSignals(t *testing.T) {
	tests := []struct {
		name       string
		ctx        context.Context
		sigs       []os.Signal
		wantSignal bool
	}{
		{
			name:       "sending signal SIGUSR2 should exit context.",
			ctx:        context.Background(),
			sigs:       []os.Signal{syscall.SIGUSR2},
			wantSignal: true,
		},
		{
			name: "sending signal SIGUSR2 should NOT exit context.",
			ctx:  context.Background(),
			sigs: []os.Signal{syscall.SIGUSR1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := WithSignals(tt.ctx, tt.sigs...)
			syscall.Kill(syscall.Getpid(), syscall.SIGUSR2)
			timer := time.NewTimer(500 * time.Millisecond)
			select {
			case <-ctx.Done():
				if !tt.wantSignal {
					t.Errorf("unexpected exit with signal")
				}
			case <-timer.C:
				if tt.wantSignal {
					t.Errorf("expected to exit with signal but did not")
				}
			}
		})
	}
}
