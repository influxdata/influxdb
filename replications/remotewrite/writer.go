package remotewrite

import (
	"context"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"sync"
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
		"influxdb-oss-replication/%s (%s) Sha/%s Date/%s",
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
	done                          chan struct{}
	waitFunc                      waitFunc // used for testing
}

func NewWriter(replicationID platform.ID, store HttpConfigStore, metrics *metrics.ReplicationsMetrics, logger *zap.Logger, done chan struct{}) *writer {
	return &writer{
		replicationID:                 replicationID,
		configStore:                   store,
		metrics:                       metrics,
		logger:                        logger,
		maximumBackoffTime:            maximumBackoffTime,
		maximumAttemptsForBackoffTime: maximumAttempts,
		clientTimeout:                 DefaultTimeout,
		done:                          done,
		waitFunc: func(t time.Duration) <-chan time.Time {
			return time.After(t)
		},
	}
}

func (w *writer) Write(data []byte, attempts int) (backoff time.Duration, err error) {
	cancelOnce := &sync.Once{}
	// Cancel any outstanding HTTP requests if the replicationQueue is closed.
	ctx, cancel := context.WithCancel(context.Background())

	defer func() {
		cancelOnce.Do(cancel)
	}()

	go func() {
		select {
		case <-w.done:
			cancelOnce.Do(cancel)
		case <-ctx.Done():
			// context is cancelled already
		}
	}()

	// Get the most recent config on every attempt, in case the user has updated the config to correct errors.
	conf, err := w.configStore.GetFullHTTPConfig(ctx, w.replicationID)
	if err != nil {
		return w.backoff(attempts), err
	}

	res, postWriteErr := PostWrite(ctx, conf, data, w.clientTimeout)
	res, msg, ok := normalizeResponse(res, postWriteErr)
	if !ok {
		// Update Response info:
		if err := w.configStore.UpdateResponseInfo(ctx, w.replicationID, res.StatusCode, msg); err != nil {
			w.logger.Debug("failed to update config store with latest remote write response info", zap.Error(err))
			return w.backoff(attempts), err
		}
		// bail out
		return w.backoff(attempts), postWriteErr
	}

	// Update metrics and most recent error diagnostic information.
	if err := w.configStore.UpdateResponseInfo(ctx, w.replicationID, res.StatusCode, msg); err != nil {
		// TODO: We shouldn't fail/retry a successful remote write for not successfully writing to the config store
		// we should only log instead of returning, like:
		w.logger.Debug("failed to update config store with latest remote write response info", zap.Error(err))
		// Unfortunately this will mess up a lot of tests that are using UpdateResponseInfo failures as a proxy for
		// write failures.
		return w.backoff(attempts), err
	}

	if postWriteErr == nil {
		// Successful write
		w.metrics.RemoteWriteSent(w.replicationID, len(data))
		w.logger.Debug("remote write successful", zap.Int("attempt", attempts), zap.Int("bytes", len(data)))
		return 0, nil
	}

	w.metrics.RemoteWriteError(w.replicationID, res.StatusCode)
	w.logger.Debug("remote write error", zap.Int("attempt", attempts), zap.String("error message", "msg"), zap.Int("status code", res.StatusCode))

	var waitTime time.Duration
	hasSetWaitTime := false

	switch res.StatusCode {
	case http.StatusBadRequest:
		if conf.DropNonRetryableData {
			var errBody []byte
			res.Body.Read(errBody)
			w.logger.Warn("dropped data", zap.Int("bytes", len(data)), zap.String("reason", string(errBody)))
			w.metrics.RemoteWriteDropped(w.replicationID, len(data))
			return 0, nil
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

	return waitTime, postWriteErr
}

// normalizeResponse returns a guaranteed non-nil value for *http.Response, and an extracted error message string for use
// in logging. The returned bool indicates if the response is a time-out - false means that the write request should be
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

	var bucket string
	if config.RemoteBucketID == nil || config.RemoteBucketName != "" {
		bucket = config.RemoteBucketName
	} else {
		bucket = config.RemoteBucketID.String()
	}

	var org string
	if config.RemoteOrgID != nil {
		org = config.RemoteOrgID.String()
	} else {
		// We need to provide something here for the write api to be happy
		org = platform.InvalidID().String()
	}

	req := client.PostWrite(ctx).
		Bucket(bucket).
		Body(data).
		Org(org)

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
