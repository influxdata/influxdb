package remotewrite

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"time"

	"github.com/influxdata/influx-cli/v2/api"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	ierrors "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/replications/metrics"
	"go.uber.org/zap"
)

const (
	retryAfterHeaderKey = "Retry-After"
	maximumBackoffTime  = 15 * time.Minute
	maximumAttempts     = 10 // After this many attempts, wait maximumBackoffTime
	DefaultTimeout      = 2 * time.Minute
)

var (
	userAgent = fmt.Sprintf(
		"influxdb-oss/%s (%s) Sha/%s Date/%s",
		influxdb.GetBuildInfo().Version,
		runtime.GOOS,
		influxdb.GetBuildInfo().Commit,
		influxdb.GetBuildInfo().Date)
)

func invalidRemoteUrl(remoteUrl string, err error) *ierrors.Error {
	return &ierrors.Error{
		Code: ierrors.EInvalid,
		Msg:  fmt.Sprintf("host URL %q is invalid", remoteUrl),
		Err:  err,
	}
}

func invalidResponseCode(code int) *ierrors.Error {
	return &ierrors.Error{
		Code: ierrors.EInvalid,
		Msg:  fmt.Sprintf("invalid response code %d, must be %d", code, http.StatusNoContent),
	}
}

type HttpConfigStore interface {
	GetFullHTTPConfig(context.Context, platform.ID) (*influxdb.ReplicationHTTPConfig, error)
	UpdateResponseInfo(context.Context, platform.ID, int, string) error
}

type waitFunc func(time.Duration) <-chan time.Time

type writer struct {
	replicationID                 platform.ID
	configStore                   HttpConfigStore
	metrics                       *metrics.ReplicationsMetrics
	logger                        *zap.Logger
	maximumBackoffTime            time.Duration
	maximumAttemptsForBackoffTime int
	clientTimeout                 time.Duration
	maximumAttemptsBeforeErr      int      // used for testing, 0 for unlimited
	waitFunc                      waitFunc // used for testing
}

func NewWriter(replicationID platform.ID, store HttpConfigStore, metrics *metrics.ReplicationsMetrics, logger *zap.Logger) *writer {
	return &writer{
		replicationID:                 replicationID,
		configStore:                   store,
		metrics:                       metrics,
		logger:                        logger,
		maximumBackoffTime:            maximumBackoffTime,
		maximumAttemptsForBackoffTime: maximumAttempts,
		clientTimeout:                 DefaultTimeout,
		waitFunc: func(t time.Duration) <-chan time.Time {
			return time.After(t)
		},
	}
}

func (w *writer) Write(ctx context.Context, data []byte) error {
	attempts := 0

	for {
		if w.maximumAttemptsBeforeErr > 0 && attempts >= w.maximumAttemptsBeforeErr {
			return errors.New("maximum number of attempts exceeded")
		}

		// Get the most recent config on every attempt, in case the user has updated the config to correct errors.
		conf, err := w.configStore.GetFullHTTPConfig(ctx, w.replicationID)
		if err != nil {
			return err
		}

		res, err := PostWrite(ctx, conf, data, w.clientTimeout)
		res, msg, ok := normalizeResponse(res, err)
		if !ok {
			// Can't retry - bail out.
			return err
		}

		if err := w.configStore.UpdateResponseInfo(ctx, w.replicationID, res.StatusCode, msg); err != nil {
			return err
		}

		if err == nil {
			// Successful write
			return nil
		}

		attempts++
		var waitTime time.Duration
		hasSetWaitTime := false

		switch res.StatusCode {
		case http.StatusBadRequest:
			if conf.DropNonRetryableData {
				w.logger.Debug(fmt.Sprintf("dropped %d bytes of data due to %d response from server", len(data), http.StatusBadRequest))
				return nil
			}
		case http.StatusTooManyRequests:
			headerTime := w.waitTimeFromHeader(res)
			if headerTime != 0 {
				waitTime = headerTime
				hasSetWaitTime = true
			}
		}

		if !hasSetWaitTime {
			waitTime = w.backoff(attempts)
		}

		select {
		case <-w.waitFunc(waitTime):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// normalizeReponse returns a guaranteed non-nil value for *http.Response, and an extracted error message string for use
// in logging. The returned bool indicates that the response is retryable - false means that the write request should be
// aborted due to a malformed request.
func normalizeResponse(r *http.Response, err error) (*http.Response, string, bool) {
	var errMsg string
	if err != nil {
		errMsg = err.Error()
	}

	if r == nil {
		if errorIsTimeout(err) {
			return &http.Response{}, errMsg, true
		}

		return &http.Response{}, errMsg, false
	}

	return r, errMsg, true
}

func errorIsTimeout(err error) bool {
	if err, ok := err.(net.Error); ok && err.Timeout() {
		return true
	}

	return false
}

func PostWrite(ctx context.Context, config *influxdb.ReplicationHTTPConfig, data []byte, timeout time.Duration) (*http.Response, error) {
	u, err := url.Parse(config.RemoteURL)
	if err != nil {
		return nil, invalidRemoteUrl(config.RemoteURL, err)
	}

	params := api.ConfigParams{
		Host:             u,
		UserAgent:        userAgent,
		Token:            &config.RemoteToken,
		AllowInsecureTLS: config.AllowInsecureTLS,
	}
	conf := api.NewAPIConfig(params)
	conf.HTTPClient.Timeout = timeout
	client := api.NewAPIClient(conf).WriteApi

	req := client.PostWrite(ctx).
		Org(config.RemoteOrgID.String()).
		Bucket(config.RemoteBucketID.String()).
		Body(data)

	// Don't set the encoding header for empty bodies, like those used for validation.
	if len(data) > 0 {
		req = req.ContentEncoding("gzip")
	}

	res, err := req.ExecuteWithHttpInfo()
	if res == nil {
		return nil, err
	}

	// Only a response of 204 is valid for a successful write
	if res.StatusCode != http.StatusNoContent {
		err = invalidResponseCode(res.StatusCode)
	}

	// Must return the response so that the status code and headers can be inspected by the caller, even if the response
	// was not 204.
	return res, err
}

func (w *writer) backoff(numAttempts int) time.Duration {
	if numAttempts > w.maximumAttemptsForBackoffTime {
		return w.maximumBackoffTime
	}

	s := 0.5 * math.Pow(2, float64(numAttempts-1))
	return time.Duration(s * float64(time.Second))
}

func (w *writer) waitTimeFromHeader(r *http.Response) time.Duration {
	str := r.Header.Get(retryAfterHeaderKey)
	if str == "" {
		return 0
	}

	// Use a minimal backoff time if the header is set to 0 for some reason, maybe due to rounding.
	if str == "0" {
		return w.backoff(1)
	}

	rtr, err := strconv.Atoi(str)
	if err != nil {
		return 0
	}

	return time.Duration(rtr * int(time.Second))
}
