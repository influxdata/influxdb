package httpd

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
)

type Encoder interface {
	// ContentType returns the content-type used to identify this format in HTTP.
	ContentType() string

	// Encode encodes the full response from the results channel.
	Encode(w io.Writer, results <-chan *query.ResultSet)

	// Error encodes a top-level error to the io.Writer.
	Error(w io.Writer, err error)
}

type ResponseFormatter interface {
	WriteResponse(w io.Writer, resp Response) (int, error)
	WriteError(w io.Writer, err error)
	ContentType() string
}

func NewEncoder(r *http.Request, config *Config) Encoder {
	epoch := strings.TrimSpace(r.FormValue("epoch"))
	switch r.Header.Get("Accept") {
	case "application/csv", "text/csv":
		formatter := &csvFormatter{statementID: -1}
		chunked, size := parseChunkedOptions(r)
		if chunked {
			return &chunkedEncoder{
				Formatter: formatter,
				ChunkSize: size,
				Epoch:     epoch,
			}
		}
		return &defaultEncoder{
			Formatter:   formatter,
			MaxRowLimit: config.MaxRowLimit,
			Epoch:       epoch,
		}
	case "application/json":
		fallthrough
	default:
		pretty := r.URL.Query().Get("pretty") == "true"
		formatter := &jsonFormatter{Pretty: pretty}
		chunked, size := parseChunkedOptions(r)
		if chunked {
			return &chunkedEncoder{
				Formatter: formatter,
				ChunkSize: size,
				Epoch:     epoch,
			}
		}
		return &defaultEncoder{
			Formatter:   formatter,
			MaxRowLimit: config.MaxRowLimit,
			Epoch:       epoch,
		}
	}
}

type defaultEncoder struct {
	Formatter   ResponseFormatter
	MaxRowLimit int
	Epoch       string
}

func NewDefaultEncoder(formatter ResponseFormatter) Encoder {
	return &defaultEncoder{
		Formatter: formatter,
	}
}

func (e *defaultEncoder) ContentType() string {
	return e.Formatter.ContentType()
}

func (e *defaultEncoder) Encode(w io.Writer, results <-chan *query.ResultSet) {
	var convertToEpoch func(row *query.Row)
	if e.Epoch != "" {
		convertToEpoch = epochConverter(e.Epoch)
	}

	resp := Response{Results: make([]*query.Result, 0)}

	rows := 0
RESULTS:
	for result := range results {
		r := &query.Result{
			StatementID: result.ID,
			Messages:    result.Messages,
			Err:         result.Err,
		}
		resp.Results = append(resp.Results, r)
		if r.Err != nil {
			continue
		}

		for series := range result.SeriesCh() {
			if series.Err != nil {
				r.Err = series.Err
				continue RESULTS
			}

			s := &models.Row{
				Name:    series.Name,
				Tags:    series.Tags.KeyValues(),
				Columns: series.Columns,
			}
			r.Series = append(r.Series, s)

			for row := range series.RowCh() {
				if row.Err != nil {
					r.Err = row.Err
					r.Series = nil
					continue RESULTS
				} else if e.MaxRowLimit > 0 && rows+len(s.Values) >= e.MaxRowLimit {
					s.Partial = true
					break RESULTS
				}

				if convertToEpoch != nil {
					convertToEpoch(&row)
				}
				s.Values = append(s.Values, row.Values)
			}
			rows += len(s.Values)
		}
	}
	e.Formatter.WriteResponse(w, resp)
}

func (e *defaultEncoder) Error(w io.Writer, err error) {
	e.Formatter.WriteError(w, err)
}

type chunkedEncoder struct {
	Formatter ResponseFormatter
	ChunkSize int
	Epoch     string
}

func (e *chunkedEncoder) ContentType() string {
	return e.Formatter.ContentType()
}

func (e *chunkedEncoder) Encode(w io.Writer, results <-chan *query.ResultSet) {
	var convertToEpoch func(row *query.Row)
	if e.Epoch != "" {
		convertToEpoch = epochConverter(e.Epoch)
	}

	for result := range results {
		messages := result.Messages

		series := <-result.SeriesCh()
		if series == nil {
			e.Formatter.WriteResponse(w, Response{Results: []*query.Result{
				{
					StatementID: result.ID,
					Messages:    messages,
				},
			}})
			continue
		} else if series.Err != nil {
			// An error occurred while processing the result.
			e.Formatter.WriteResponse(w, Response{Results: []*query.Result{
				{
					StatementID: result.ID,
					Messages:    messages,
					Err:         series.Err,
				},
			}})
			continue
		}

		for series != nil {
			var values [][]interface{}
			for row := range series.RowCh() {
				if row.Err != nil {
					// An error occurred while processing the result.
					e.Formatter.WriteResponse(w, Response{Results: []*query.Result{
						{
							StatementID: result.ID,
							Messages:    messages,
							Err:         series.Err,
						},
					}})
					continue
				}

				if convertToEpoch != nil {
					convertToEpoch(&row)
				}

				if e.ChunkSize > 0 && len(values) >= e.ChunkSize {
					r := &query.Result{
						StatementID: result.ID,
						Series: []*models.Row{{
							Name:    series.Name,
							Tags:    series.Tags.KeyValues(),
							Columns: series.Columns,
							Values:  values,
							Partial: true,
						}},
						Messages: messages,
						Partial:  true,
					}
					e.Formatter.WriteResponse(w, Response{Results: []*query.Result{r}})
					messages = nil
					values = values[:0]
				}
				values = append(values, row.Values)
			}

			r := &query.Result{
				StatementID: result.ID,
				Series: []*models.Row{{
					Name:    series.Name,
					Tags:    series.Tags.KeyValues(),
					Columns: series.Columns,
					Values:  values,
				}},
				Messages: messages,
			}

			series = <-result.SeriesCh()
			if series != nil {
				r.Partial = true
			}
			e.Formatter.WriteResponse(w, Response{Results: []*query.Result{r}})
		}
	}
}

func (e *chunkedEncoder) Error(w io.Writer, err error) {
	e.Formatter.WriteError(w, err)
}

func epochConverter(epoch string) func(row *query.Row) {
	divisor := int64(1)

	switch epoch {
	case "u":
		divisor = int64(time.Microsecond)
	case "ms":
		divisor = int64(time.Millisecond)
	case "s":
		divisor = int64(time.Second)
	case "m":
		divisor = int64(time.Minute)
	case "h":
		divisor = int64(time.Hour)
	}
	return func(row *query.Row) {
		if ts, ok := row.Values[0].(time.Time); ok {
			row.Values[0] = ts.UnixNano() / divisor
		}
	}
}

func parseChunkedOptions(r *http.Request) (chunked bool, size int) {
	chunked = r.FormValue("chunked") == "true"
	if chunked {
		size = DefaultChunkSize
		if chunked {
			if n, err := strconv.ParseInt(r.FormValue("chunk_size"), 10, 64); err == nil && int(n) > 0 {
				size = int(n)
			}
		}
	}
	return chunked, size
}
