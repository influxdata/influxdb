package influxdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateReplicationRequestOK_MaxAge(t *testing.T) {
	okSize := DefaultReplicationMaxQueueSizeBytes

	tests := []struct {
		name          string
		maxAgeSeconds int64
		expectErr     error
	}{
		{"zero accepted as default", 0, nil},
		{"in range lower bound", MinReplicationMaxAgeSeconds, nil},
		{"in range typical", DefaultReplicationMaxAge, nil},
		{"in range upper bound", MaxReplicationMaxAgeSeconds, nil},
		{"negative rejected", -1, &ErrMaxAgeOutOfRange},
		{"below minimum rejected", MinReplicationMaxAgeSeconds - 1, &ErrMaxAgeOutOfRange},
		{"above maximum rejected", MaxReplicationMaxAgeSeconds + 1, &ErrMaxAgeOutOfRange},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := CreateReplicationRequest{
				MaxQueueSizeBytes: okSize,
				MaxAgeSeconds:     tt.maxAgeSeconds,
			}
			err := req.OK()
			if tt.expectErr == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, tt.expectErr, err)
			}
		})
	}
}

func TestUpdateReplicationRequestOK_MaxAge(t *testing.T) {
	okSize := DefaultReplicationMaxQueueSizeBytes
	mk := func(v int64) *int64 { return &v }

	tests := []struct {
		name          string
		maxAgeSeconds *int64
		expectErr     error
	}{
		{"nil skipped", nil, nil},
		{"zero accepted as default", mk(0), nil},
		{"in range lower bound", mk(MinReplicationMaxAgeSeconds), nil},
		{"in range upper bound", mk(MaxReplicationMaxAgeSeconds), nil},
		{"negative rejected", mk(-1), &ErrMaxAgeOutOfRange},
		{"below minimum rejected", mk(MinReplicationMaxAgeSeconds - 1), &ErrMaxAgeOutOfRange},
		{"above maximum rejected", mk(MaxReplicationMaxAgeSeconds + 1), &ErrMaxAgeOutOfRange},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := UpdateReplicationRequest{
				MaxQueueSizeBytes: &okSize,
				MaxAgeSeconds:     tt.maxAgeSeconds,
			}
			err := req.OK()
			if tt.expectErr == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, tt.expectErr, err)
			}
		})
	}
}
