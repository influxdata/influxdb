package points

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"github.com/influxdata/influxdb/v2/kit/platform"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"

	io2 "github.com/influxdata/influxdb/v2/kit/io"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/opentracing/opentracing-go"
)

var (
	// ErrMaxBatchSizeExceeded is returned when a points batch exceeds
	// the defined upper limit in bytes. This pertains to the size of the
	// batch after inflation from any compression (i.e. ungzipped).
	ErrMaxBatchSizeExceeded = errors.New("points batch is too large")
)

const (
	opPointsWriter           = "http/pointsWriter"
	msgUnableToReadData      = "unable to read data"
	msgWritingRequiresPoints = "writing requires points"
)

// ParsedPoints contains the points parsed as well as the total number of bytes
// after decompression.
type ParsedPoints struct {
	Points  models.Points
	RawSize int
}

// Parser parses batches of Points.
type Parser struct {
	Precision string
	//ParserOptions []models.ParserOption
}

// Parse parses the points from an io.ReadCloser for a specific Bucket.
func (pw *Parser) Parse(ctx context.Context, orgID, bucketID platform.ID, rc io.ReadCloser) (*ParsedPoints, error) {
	span, ctx := opentracing.StartSpanFromContext(ctx, "write points")
	defer span.Finish()
	return pw.parsePoints(ctx, orgID, bucketID, rc)
}

func (pw *Parser) parsePoints(ctx context.Context, orgID, bucketID platform.ID, rc io.ReadCloser) (*ParsedPoints, error) {
	data, err := readAll(ctx, rc)
	if err != nil {
		code := errors2.EInternal
		if errors.Is(err, ErrMaxBatchSizeExceeded) {
			code = errors2.ETooLarge
		} else if errors.Is(err, gzip.ErrHeader) || errors.Is(err, gzip.ErrChecksum) {
			code = errors2.EInvalid
		}
		return nil, &errors2.Error{
			Code: code,
			Op:   opPointsWriter,
			Msg:  msgUnableToReadData,
			Err:  err,
		}
	}

	requestBytes := len(data)
	if requestBytes == 0 {
		return nil, &errors2.Error{
			Op:   opPointsWriter,
			Code: errors2.EInvalid,
			Msg:  msgWritingRequiresPoints,
		}
	}

	span, _ := tracing.StartSpanFromContextWithOperationName(ctx, "encoding and parsing")

	points, err := models.ParsePointsWithPrecision(data, time.Now().UTC(), pw.Precision)
	span.LogKV("values_total", len(points))
	span.Finish()
	if err != nil {
		tracing.LogError(span, fmt.Errorf("error parsing points: %v", err))

		code := errors2.EInvalid
		// TODO - backport these
		// if errors.Is(err, models.ErrLimitMaxBytesExceeded) ||
		// 	errors.Is(err, models.ErrLimitMaxLinesExceeded) ||
		// 	errors.Is(err, models.ErrLimitMaxValuesExceeded) {
		// 	code = influxdb.ETooLarge
		// }

		return nil, &errors2.Error{
			Code: code,
			Op:   opPointsWriter,
			Msg:  "",
			Err:  err,
		}
	}

	return &ParsedPoints{
		Points:  points,
		RawSize: requestBytes,
	}, nil
}

func readAll(ctx context.Context, rc io.ReadCloser) (data []byte, err error) {
	defer func() {
		if cerr := rc.Close(); cerr != nil && err == nil {
			if errors.Is(cerr, io2.ErrReadLimitExceeded) {
				cerr = ErrMaxBatchSizeExceeded
			}
			err = cerr
		}
	}()

	span, _ := tracing.StartSpanFromContextWithOperationName(ctx, "read request body")

	defer func() {
		span.LogKV("request_bytes", len(data))
		span.Finish()
	}()

	data, err = ioutil.ReadAll(rc)
	if err != nil {
		return nil, err

	}
	return data, nil
}

// NewParser returns a new Parser
func NewParser(precision string /*parserOptions ...models.ParserOption*/) *Parser {
	return &Parser{
		Precision: precision,
		//ParserOptions: parserOptions,
	}
}
