package httpd

import (
	"io"
	"net/http"

	"github.com/influxdata/influxdb/query"
	"github.com/tinylib/msgp/msgp"
)

type messagePackEncoder struct {
	Epoch     string
	ChunkSize int
}

func (e *messagePackEncoder) ContentType() string {
	return "application/vnd.influxdb-cursor+msgpack; version=1.0"
}

func (e *messagePackEncoder) Encode(w io.Writer, header ResponseHeader, results <-chan *query.ResultSet) {
	var convertToEpoch func(row *query.Row)
	if e.Epoch != "" {
		convertToEpoch = epochConverter(e.Epoch)
	}
	values := make([][]interface{}, 0, e.ChunkSize)

	enc := msgp.NewWriter(w)
	enc.WriteMapHeader(1)
	enc.WriteString("results")
	enc.WriteInt(header.Results)

	enc.Flush()
	if w, ok := w.(http.Flusher); ok {
		w.Flush()
	}

	for result := range results {
		var messages []Message
		if len(result.Messages) > 0 {
			messages = make([]Message, len(result.Messages))
			for i, m := range result.Messages {
				messages[i].Level = m.Level
				messages[i].Text = m.Text
			}
		}
		header := ResultHeader{
			ID:       result.ID,
			Messages: messages,
		}
		if result.Err != nil {
			err := result.Err.Error()
			header.Error = &err
		}
		header.EncodeMsg(enc)
		if result.Err != nil {
			enc.Flush()
			if w, ok := w.(http.Flusher); ok {
				w.Flush()
			}
			continue
		}

		for series := range result.SeriesCh() {
			enc.WriteBool(true)

			if series.Err != nil {
				err := series.Err.Error()
				header := SeriesError{Error: err}
				header.EncodeMsg(enc)

				enc.Flush()
				if w, ok := w.(http.Flusher); ok {
					w.Flush()
				}
				continue
			}

			columns := make([]Column, len(series.Columns))
			for i, col := range series.Columns {
				columns[i] = Column{Name: col.Name, Type: col.Type.String()}
			}
			header := SeriesHeader{
				Name:    &series.Name,
				Tags:    series.Tags.KeyValues(),
				Columns: columns,
			}
			header.EncodeMsg(enc)

			for row := range series.RowCh() {
				if row.Err != nil {
					header := RowBatchHeader{
						Length:   len(values),
						Continue: true,
					}
					header.EncodeMsg(enc)
					for _, v := range values {
						enc.WriteArrayHeader(uint32(len(v)))
						for _, col := range v {
							enc.WriteIntf(col)
						}
					}
					values = values[:0]

					err := RowBatchError{Error: row.Err.Error()}
					err.EncodeMsg(enc)
					continue
				}

				if convertToEpoch != nil {
					convertToEpoch(&row)
				}

				if len(values) == cap(values) {
					header := RowBatchHeader{
						Length:   len(values),
						Continue: true,
					}
					header.EncodeMsg(enc)
					for _, v := range values {
						enc.WriteArrayHeader(uint32(len(v)))
						for _, col := range v {
							enc.WriteIntf(col)
						}
					}
					values = values[:0]

					enc.Flush()
					if w, ok := w.(http.Flusher); ok {
						w.Flush()
					}
				}
				values = append(values, row.Values)
			}

			rowHeader := RowBatchHeader{Length: len(values)}
			rowHeader.EncodeMsg(enc)
			for _, v := range values {
				enc.WriteArrayHeader(uint32(len(v)))
				for _, col := range v {
					enc.WriteIntf(col)
				}
			}
			values = values[:0]

			enc.Flush()
			if w, ok := w.(http.Flusher); ok {
				w.Flush()
			}
		}
		enc.WriteBool(false)

		enc.Flush()
		if w, ok := w.(http.Flusher); ok {
			w.Flush()
		}
	}
}

func (e *messagePackEncoder) Error(w io.Writer, err error) {
	enc := msgp.NewWriter(w)
	enc.WriteMapHeader(1)
	enc.WriteString("error")
	enc.WriteString(err.Error())
	enc.Flush()
}
