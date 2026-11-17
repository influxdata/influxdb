package errors

import (
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCapture(t *testing.T) {
	errClose := stderrors.New("close error")
	errOriginal := stderrors.New("original error")

	tests := []struct {
		name     string
		origErr  error
		closeErr error
		wantErr  error
	}{
		{
			name:     "nil original, nil close",
			origErr:  nil,
			closeErr: nil,
			wantErr:  nil,
		},
		{
			name:     "nil original, non-nil close",
			origErr:  nil,
			closeErr: errClose,
			wantErr:  errClose,
		},
		{
			name:     "non-nil original, nil close",
			origErr:  errOriginal,
			closeErr: nil,
			wantErr:  errOriginal,
		},
		{
			name:     "non-nil original, non-nil close",
			origErr:  errOriginal,
			closeErr: errClose,
			wantErr:  errOriginal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.origErr
			fn := Capture(&err, func() error {
				return tt.closeErr
			})
			fn()

			if tt.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

func TestCapture_DeferPattern(t *testing.T) {
	errClose := stderrors.New("close error")

	fn := func() (err error) {
		defer Capture(&err, func() error {
			return errClose
		})()
		return nil
	}

	require.ErrorIs(t, fn(), errClose)
}

func TestCapture_DeferPatternPreservesOriginal(t *testing.T) {
	errClose := stderrors.New("close error")
	errOriginal := stderrors.New("original error")

	fn := func() (err error) {
		defer Capture(&err, func() error {
			return errClose
		})()
		return errOriginal
	}

	err := fn()
	require.ErrorIs(t, err, errOriginal)
}

func TestCapturef(t *testing.T) {
	errClose := stderrors.New("close error")
	errOriginal := stderrors.New("original error")

	t.Run("nil fn is no-op", func(t *testing.T) {
		var err error
		fn := Capturef(&err, nil, "")
		fn()
		require.NoError(t, err)
	})

	t.Run("fn returns nil is no-op", func(t *testing.T) {
		var err error
		fn := Capturef(&err, func() error { return nil }, "")
		fn()
		require.NoError(t, err)
	})

	t.Run("nil rErr does not panic", func(t *testing.T) {
		fn := Capturef(nil, func() error { return errClose }, "")
		require.NotPanics(t, fn)
	})

	t.Run("captures close error when original is nil and format is empty", func(t *testing.T) {
		var err error
		fn := Capturef(&err, func() error { return errClose }, "")
		fn()
		require.ErrorIs(t, err, errClose)
		require.EqualError(t, err, "close error")
	})

	t.Run("joins errors when both present and format is empty", func(t *testing.T) {
		err := errOriginal
		fn := Capturef(&err, func() error { return errClose }, "")
		fn()
		require.ErrorIs(t, err, errOriginal)
		require.ErrorIs(t, err, errClose)
		require.EqualError(t, err, "original error\nclose error")
	})

	t.Run("preserves original when fn returns nil", func(t *testing.T) {
		err := errOriginal
		fn := Capturef(&err, func() error { return nil }, "")
		fn()
		require.ErrorIs(t, err, errOriginal)
	})

	t.Run("wraps close error with format string", func(t *testing.T) {
		var err error
		errDisk := stderrors.New("no space left on device")
		fn := Capturef(&err, func() error { return errDisk }, "error closing %q", "meta.boltdb")
		fn()
		require.ErrorIs(t, err, errDisk)
		require.EqualError(t, err, `error closing "meta.boltdb": no space left on device`)
	})

	t.Run("joins formatted close error with original", func(t *testing.T) {
		err := errOriginal
		errDisk := stderrors.New("no space left on device")
		fn := Capturef(&err, func() error { return errDisk }, "error closing %q", "meta.boltdb")
		fn()
		require.ErrorIs(t, err, errOriginal)
		require.ErrorIs(t, err, errDisk)
		require.EqualError(t, err, "original error\nerror closing \"meta.boltdb\": no space left on device")
	})
}

func TestCapturef_DeferPattern(t *testing.T) {
	errClose := stderrors.New("close error")

	fn := func() (err error) {
		defer Capturef(&err, func() error {
			return errClose
		}, "closing resource")()
		return nil
	}

	err := fn()
	require.ErrorIs(t, err, errClose)
	require.EqualError(t, err, "closing resource: close error")
}

func TestCapturef_DeferPatternJoinsErrors(t *testing.T) {
	errClose := stderrors.New("close error")
	errOriginal := stderrors.New("original error")

	fn := func() (err error) {
		defer Capturef(&err, func() error {
			return errClose
		}, "closing resource")()
		return errOriginal
	}

	err := fn()
	require.ErrorIs(t, err, errOriginal)
	require.ErrorIs(t, err, errClose)
	require.EqualError(t, err, "original error\nclosing resource: close error")
}

func TestCapturef_FormatStringDocExample(t *testing.T) {
	errDisk := stderrors.New("no space left on device")

	fn := func(name string) (rErr error) {
		closer := func() error { return errDisk }
		defer Capturef(&rErr, closer, "error closing %q", name)()
		return nil
	}

	err := fn("meta.boltdb")
	require.ErrorIs(t, err, errDisk)
	require.EqualError(t, err, `error closing "meta.boltdb": no space left on device`)
}
