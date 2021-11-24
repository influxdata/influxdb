package internal

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/stretchr/testify/require"
)

func TestValidateReplication(t *testing.T) {
	tests := []struct {
		status int
		valid  bool
	}{
		{http.StatusNoContent, true},
		{http.StatusOK, false},
		{http.StatusBadRequest, false},
		{http.StatusTeapot, false},
		{http.StatusInternalServerError, false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("status code %d", tt.status), func(t *testing.T) {
			svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
			}))
			defer svr.Close()

			validator := noopWriteValidator{}

			config := &influxdb.ReplicationHTTPConfig{
				RemoteURL: svr.URL,
			}

			err := validator.ValidateReplication(context.Background(), config)
			if tt.valid {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
		})
	}
}
