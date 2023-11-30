package remotewrite

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/kit/prom/promtest"
	ihttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/replications/metrics"
	replicationsMock "github.com/influxdata/influxdb/v2/replications/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

//go:generate go run github.com/golang/mock/mockgen -package mock -destination ../mock/http_config_store.go github.com/influxdata/influxdb/v2/replications/remotewrite HttpConfigStore

var (
	testID = platform.ID(1)
)

func testWriter(t *testing.T) (*writer, *replicationsMock.MockHttpConfigStore, chan struct{}) {
	ctrl := gomock.NewController(t)
	configStore := replicationsMock.NewMockHttpConfigStore(ctrl)
	done := make(chan struct{})
	w := NewWriter(testID, configStore, metrics.NewReplicationsMetrics(), zaptest.NewLogger(t), done)
	return w, configStore, done
}

func constantStatus(i int) func(int) int {
	return func(int) int {
		return i
	}
}

func testServer(t *testing.T, statusForCount func(int) int, wantData []byte) *httptest.Server {
	count := 0
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotData, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, wantData, gotData)
		w.WriteHeader(statusForCount(count))
		count++
	}))
}

func instaWait() waitFunc {
	return func(t time.Duration) <-chan time.Time {
		out := make(chan time.Time)
		close(out)
		return out
	}
}

type containsMatcher struct {
	substring string
}

func (cm *containsMatcher) Matches(x interface{}) bool {
	if st, ok := x.(fmt.Stringer); ok {
		return strings.Contains(st.String(), cm.substring)
	} else {
		s, ok := x.(string)
		return ok && strings.Contains(s, cm.substring)
	}
}

func (cm *containsMatcher) String() string {
	if cm != nil {
		return cm.substring
	} else {
		return ""
	}
}

func TestWrite(t *testing.T) {
	t.Parallel()

	testData := []byte("some data")

	t.Run("error getting config", func(t *testing.T) {
		wantErr := errors.New("uh oh")

		w, configStore, _ := testWriter(t)

		configStore.EXPECT().GetFullHTTPConfig(gomock.Any(), testID).Return(nil, wantErr)
		_, actualErr := w.Write([]byte{}, 1)
		require.Equal(t, wantErr, actualErr)
	})

	t.Run("nil response from PostWrite", func(t *testing.T) {
		testConfig := &influxdb.ReplicationHTTPConfig{
			RemoteURL: "not a good URL",
		}
		w, configStore, _ := testWriter(t)
		configStore.EXPECT().GetFullHTTPConfig(gomock.Any(), testID).Return(testConfig, nil)
		configStore.EXPECT().UpdateResponseInfo(gomock.Any(), testID, int(0), gomock.Any())
		_, actualErr := w.Write([]byte{}, 1)
		require.Error(t, actualErr)
	})

	t.Run("immediate good response", func(t *testing.T) {
		svr := testServer(t, constantStatus(http.StatusNoContent), testData)
		defer svr.Close()

		testConfig := &influxdb.ReplicationHTTPConfig{
			RemoteURL: svr.URL,
		}

		w, configStore, _ := testWriter(t)

		configStore.EXPECT().GetFullHTTPConfig(gomock.Any(), testID).Return(testConfig, nil)
		configStore.EXPECT().UpdateResponseInfo(gomock.Any(), testID, http.StatusNoContent, "").Return(nil)
		_, actualErr := w.Write(testData, 0)
		require.NoError(t, actualErr)
	})

	t.Run("error updating response info", func(t *testing.T) {
		wantErr := errors.New("o no")

		svr := testServer(t, constantStatus(http.StatusNoContent), testData)
		defer svr.Close()

		testConfig := &influxdb.ReplicationHTTPConfig{
			RemoteURL: svr.URL,
		}

		w, configStore, _ := testWriter(t)

		configStore.EXPECT().GetFullHTTPConfig(gomock.Any(), testID).Return(testConfig, nil)
		configStore.EXPECT().UpdateResponseInfo(gomock.Any(), testID, http.StatusNoContent, "").Return(wantErr)
		_, actualErr := w.Write(testData, 1)
		require.Equal(t, wantErr, actualErr)
	})

	t.Run("bad server responses that never succeed", func(t *testing.T) {
		testAttempts := 3

		for _, status := range []int{http.StatusOK, http.StatusTeapot, http.StatusInternalServerError} {
			t.Run(fmt.Sprintf("status code %d", status), func(t *testing.T) {
				svr := testServer(t, constantStatus(status), testData)
				defer svr.Close()

				testConfig := &influxdb.ReplicationHTTPConfig{
					RemoteURL: svr.URL,
				}

				w, configStore, _ := testWriter(t)
				w.waitFunc = instaWait()

				configStore.EXPECT().GetFullHTTPConfig(gomock.Any(), testID).Return(testConfig, nil)
				configStore.EXPECT().UpdateResponseInfo(gomock.Any(), testID, status, &containsMatcher{invalidResponseCode(status, nil).Error()}).Return(nil)
				_, actualErr := w.Write(testData, testAttempts)
				require.NotNil(t, actualErr)
				require.Contains(t, actualErr.Error(), fmt.Sprintf("invalid response code %d", status))
			})
		}
	})

	t.Run("drops bad data after config is updated", func(t *testing.T) {
		testAttempts := 5

		svr := testServer(t, constantStatus(http.StatusBadRequest), testData)
		defer svr.Close()

		testConfig := &influxdb.ReplicationHTTPConfig{
			RemoteURL: svr.URL,
		}

		updatedConfig := &influxdb.ReplicationHTTPConfig{
			RemoteURL:            svr.URL,
			DropNonRetryableData: true,
		}

		w, configStore, _ := testWriter(t)
		w.waitFunc = instaWait()

		configStore.EXPECT().GetFullHTTPConfig(gomock.Any(), testID).Return(testConfig, nil).Times(testAttempts - 1)
		configStore.EXPECT().GetFullHTTPConfig(gomock.Any(), testID).Return(updatedConfig, nil)
		configStore.EXPECT().UpdateResponseInfo(gomock.Any(), testID, http.StatusBadRequest, &containsMatcher{invalidResponseCode(http.StatusBadRequest, nil).Error()}).Return(nil).Times(testAttempts)
		for i := 1; i <= testAttempts; i++ {
			_, actualErr := w.Write(testData, i)
			if testAttempts == i {
				require.NoError(t, actualErr)
			} else {
				require.Error(t, actualErr)
			}
		}
	})

	t.Run("gives backoff time on write response", func(t *testing.T) {
		svr := testServer(t, constantStatus(http.StatusBadRequest), testData)
		defer svr.Close()

		testConfig := &influxdb.ReplicationHTTPConfig{
			RemoteURL: svr.URL,
		}

		w, configStore, _ := testWriter(t)

		configStore.EXPECT().GetFullHTTPConfig(gomock.Any(), testID).Return(testConfig, nil)
		configStore.EXPECT().UpdateResponseInfo(gomock.Any(), testID, http.StatusBadRequest, gomock.Any()).Return(nil)
		backoff, actualErr := w.Write(testData, 1)
		require.Equal(t, backoff, w.backoff(1))
		require.ErrorContains(t, actualErr, invalidResponseCode(http.StatusBadRequest, nil).Error())
	})

	t.Run("uses wait time from response header if present", func(t *testing.T) {
		numSeconds := 5
		waitTimeFromHeader := 5 * time.Second

		svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotData, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.Equal(t, testData, gotData)
			w.Header().Set(retryAfterHeaderKey, strconv.Itoa(numSeconds))
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		defer svr.Close()

		testConfig := &influxdb.ReplicationHTTPConfig{
			RemoteURL: svr.URL,
		}

		w, configStore, done := testWriter(t)
		w.waitFunc = func(dur time.Duration) <-chan time.Time {
			require.Equal(t, waitTimeFromHeader, dur)
			close(done)
			return instaWait()(dur)
		}

		configStore.EXPECT().GetFullHTTPConfig(gomock.Any(), testID).Return(testConfig, nil)
		configStore.EXPECT().UpdateResponseInfo(gomock.Any(), testID, http.StatusTooManyRequests, &containsMatcher{invalidResponseCode(http.StatusTooManyRequests, nil).Error()}).Return(nil)
		_, actualErr := w.Write(testData, 1)
		require.ErrorContains(t, actualErr, invalidResponseCode(http.StatusTooManyRequests, nil).Error())
	})

	t.Run("can cancel with done channel", func(t *testing.T) {
		svr := testServer(t, constantStatus(http.StatusInternalServerError), testData)
		defer svr.Close()

		testConfig := &influxdb.ReplicationHTTPConfig{
			RemoteURL: svr.URL,
		}

		w, configStore, _ := testWriter(t)

		configStore.EXPECT().GetFullHTTPConfig(gomock.Any(), testID).Return(testConfig, nil)
		configStore.EXPECT().UpdateResponseInfo(gomock.Any(), testID, http.StatusInternalServerError, &containsMatcher{invalidResponseCode(http.StatusInternalServerError, nil).Error()}).Return(nil)
		_, actualErr := w.Write(testData, 1)
		require.ErrorContains(t, actualErr, invalidResponseCode(http.StatusInternalServerError, nil).Error())
	})

	t.Run("writes resume after temporary remote disconnect", func(t *testing.T) {
		// Attempt to write data a total of 5 times.
		// Succeed on the first point, writing point 1. (baseline test)
		// Fail on the second and third, then succeed on the fourth, writing point 2.
		// Fail on the fifth, sixth and seventh, then succeed on the eighth, writing point 3.
		attemptMap := make([]bool, 8)
		attemptMap[0] = true
		attemptMap[3] = true
		attemptMap[7] = true
		var attempt uint8

		var currentWrite int
		testWrites := []string{
			"this is some data",
			"this is also some data",
			"this is even more data",
		}

		svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if attemptMap[attempt] {
				gotData, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Equal(t, []byte(testWrites[currentWrite]), gotData)
				w.WriteHeader(http.StatusNoContent)
			} else {
				// Simulate a timeout, as if the remote connection were offline
				w.WriteHeader(http.StatusGatewayTimeout)
			}
			attempt++
		}))
		defer svr.Close()

		testConfig := &influxdb.ReplicationHTTPConfig{
			RemoteURL: svr.URL,
		}
		w, configStore, _ := testWriter(t)

		numAttempts := 0
		for i := 0; i < len(testWrites); i++ {
			currentWrite = i
			configStore.EXPECT().GetFullHTTPConfig(gomock.Any(), testID).Return(testConfig, nil)
			if attemptMap[attempt] {
				// should succeed
				configStore.EXPECT().UpdateResponseInfo(gomock.Any(), testID, http.StatusNoContent, gomock.Any()).Return(nil)
				_, err := w.Write([]byte(testWrites[i]), numAttempts)
				require.NoError(t, err)
				numAttempts = 0
			} else {
				// should fail
				configStore.EXPECT().UpdateResponseInfo(gomock.Any(), testID, http.StatusGatewayTimeout, &containsMatcher{invalidResponseCode(http.StatusGatewayTimeout, nil).Error()}).Return(nil)
				_, err := w.Write([]byte(testWrites[i]), numAttempts)
				require.Error(t, err)
				numAttempts++
				i-- // decrement so that we retry this same data point in the next loop iteration
			}
		}
	})
}

func TestWrite_Metrics(t *testing.T) {
	testData := []byte("this is some data")

	tests := []struct {
		name                 string
		status               func(int) int
		expectedErr          error
		data                 []byte
		registerExpectations func(*testing.T, *replicationsMock.MockHttpConfigStore, *influxdb.ReplicationHTTPConfig)
		checkMetrics         func(*testing.T, *prom.Registry)
	}{
		{
			name:        "server errors",
			status:      constantStatus(http.StatusTeapot),
			expectedErr: invalidResponseCode(http.StatusTeapot, nil),
			data:        []byte{},
			registerExpectations: func(t *testing.T, store *replicationsMock.MockHttpConfigStore, conf *influxdb.ReplicationHTTPConfig) {
				store.EXPECT().GetFullHTTPConfig(gomock.Any(), testID).Return(conf, nil)
				store.EXPECT().UpdateResponseInfo(gomock.Any(), testID, http.StatusTeapot, &containsMatcher{invalidResponseCode(http.StatusTeapot, nil).Error()}).Return(nil)
			},
			checkMetrics: func(t *testing.T, reg *prom.Registry) {
				mfs := promtest.MustGather(t, reg)
				errorCodes := promtest.FindMetric(mfs, "replications_queue_remote_write_errors", map[string]string{
					"replicationID": testID.String(),
					"code":          strconv.Itoa(http.StatusTeapot),
				})
				require.NotNil(t, errorCodes)
			},
		},
		{
			name:   "successful write",
			status: constantStatus(http.StatusNoContent),
			data:   testData,
			registerExpectations: func(t *testing.T, store *replicationsMock.MockHttpConfigStore, conf *influxdb.ReplicationHTTPConfig) {
				store.EXPECT().GetFullHTTPConfig(gomock.Any(), testID).Return(conf, nil)
				store.EXPECT().UpdateResponseInfo(gomock.Any(), testID, http.StatusNoContent, "").Return(nil)
			},
			checkMetrics: func(t *testing.T, reg *prom.Registry) {
				mfs := promtest.MustGather(t, reg)

				bytesSent := promtest.FindMetric(mfs, "replications_queue_remote_write_bytes_sent", map[string]string{
					"replicationID": testID.String(),
				})
				require.NotNil(t, bytesSent)
				require.Equal(t, float64(len(testData)), bytesSent.Counter.GetValue())
			},
		},
		{
			name:   "dropped data",
			status: constantStatus(http.StatusBadRequest),
			data:   testData,
			registerExpectations: func(t *testing.T, store *replicationsMock.MockHttpConfigStore, conf *influxdb.ReplicationHTTPConfig) {
				store.EXPECT().GetFullHTTPConfig(gomock.Any(), testID).Return(conf, nil)
				store.EXPECT().UpdateResponseInfo(gomock.Any(), testID, http.StatusBadRequest, &containsMatcher{invalidResponseCode(http.StatusBadRequest, nil).Error()}).Return(nil)
			},
			checkMetrics: func(t *testing.T, reg *prom.Registry) {
				mfs := promtest.MustGather(t, reg)

				bytesDropped := promtest.FindMetric(mfs, "replications_queue_remote_write_bytes_dropped", map[string]string{
					"replicationID": testID.String(),
				})
				require.NotNil(t, bytesDropped)
				require.Equal(t, float64(len(testData)), bytesDropped.Counter.GetValue())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svr := testServer(t, tt.status, tt.data)
			defer svr.Close()

			testConfig := &influxdb.ReplicationHTTPConfig{
				RemoteURL:            svr.URL,
				DropNonRetryableData: true,
			}

			w, configStore, _ := testWriter(t)
			w.waitFunc = instaWait()
			reg := prom.NewRegistry(zaptest.NewLogger(t))
			reg.MustRegister(w.metrics.PrometheusCollectors()...)

			tt.registerExpectations(t, configStore, testConfig)
			_, actualErr := w.Write(tt.data, 1)
			if tt.expectedErr != nil {
				require.ErrorContains(t, actualErr, tt.expectedErr.Error())
			} else {
				require.NoError(t, actualErr)
			}
			tt.checkMetrics(t, reg)
		})
	}
}

func TestPostWrite(t *testing.T) {
	testData := []byte("some data")

	tests := []struct {
		status  int
		bodyErr error
		wantErr bool
	}{
		{
			status:  http.StatusOK,
			wantErr: true,
		},
		{
			status:  http.StatusNoContent,
			wantErr: false,
		},
		{
			status:  http.StatusBadRequest,
			wantErr: true,
			bodyErr: fmt.Errorf("This is a terrible error: %w", errors.New("there are bad things here")),
		},
		{
			status:  http.StatusMethodNotAllowed,
			wantErr: true,
			bodyErr: fmt.Errorf("method not allowed: %w", errors.New("what were you thinking")),
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("status code %d", tt.status), func(t *testing.T) {
			svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				recData, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Equal(t, testData, recData)

				if tt.bodyErr != nil {
					influxErrorCode := ihttp.StatusCodeToErrorCode(tt.status)
					ihttp.WriteErrorResponse(context.Background(), w, influxErrorCode, tt.bodyErr.Error())
				} else {
					w.WriteHeader(tt.status)
				}
			}))
			defer svr.Close()

			config := &influxdb.ReplicationHTTPConfig{
				RemoteURL: svr.URL,
			}

			res, err := PostWrite(context.Background(), config, testData, time.Second)
			if tt.wantErr {
				require.Error(t, err)
				if nil != tt.bodyErr {
					require.ErrorContains(t, err, tt.bodyErr.Error())
				}
			} else {
				require.Nil(t, err)
			}

			if res != nil {
				require.Equal(t, tt.status, res.StatusCode)
			}
		})
	}
}

func TestWaitTimeFromHeader(t *testing.T) {
	w := &writer{
		maximumAttemptsForBackoffTime: maximumAttempts,
	}

	tests := []struct {
		headerKey string
		headerVal string
		want      time.Duration
	}{
		{
			headerKey: retryAfterHeaderKey,
			headerVal: "30",
			want:      30 * time.Second,
		},
		{
			headerKey: retryAfterHeaderKey,
			headerVal: "0",
			want:      w.backoff(1),
		},
		{
			headerKey: retryAfterHeaderKey,
			headerVal: "not a number",
			want:      0,
		},
		{
			headerKey: "some other thing",
			headerVal: "not a number",
			want:      0,
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%q - %q", tt.headerKey, tt.headerVal), func(t *testing.T) {
			r := &http.Response{
				Header: http.Header{
					tt.headerKey: []string{tt.headerVal},
				},
			}

			got := w.waitTimeFromHeader(r)
			require.Equal(t, tt.want, got)
		})
	}
}
