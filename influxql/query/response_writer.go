package query

//lint:file-ignore SA1019 Ignore for now

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/influxdata/influxdb/v2/influxql"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/tinylib/msgp/msgp"
)

// ResponseWriter is an interface for writing a response.
type ResponseWriter interface {
	// WriteResponse writes a response.
	WriteResponse(ctx context.Context, w io.Writer, resp Response) error
}

// NewResponseWriter creates a new ResponseWriter based on the Accept header
// in the request that wraps the ResponseWriter.
func NewResponseWriter(encoding influxql.EncodingFormat) ResponseWriter {
	switch encoding {
	case influxql.EncodingFormatCSV:
		return &csvFormatter{statementID: -1}
	case influxql.EncodingFormatTable:
		return &textFormatter{}
	case influxql.EncodingFormatMessagePack:
		return &msgpFormatter{}
	case influxql.EncodingFormatJSON:
		fallthrough
	default:
		// TODO(sgc): Add EncodingFormatJSONPretty
		return &jsonFormatter{Pretty: false}
	}
}

type jsonFormatter struct {
	Pretty bool
}

func (f *jsonFormatter) WriteResponse(ctx context.Context, w io.Writer, resp Response) (err error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	var b []byte
	if f.Pretty {
		b, err = json.MarshalIndent(resp, "", "    ")
	} else {
		b, err = json.Marshal(resp)
	}

	if err != nil {
		_, err = io.WriteString(w, err.Error())
	} else {
		_, err = w.Write(b)
	}

	w.Write([]byte("\n"))
	return err
}

type csvFormatter struct {
	statementID int
	columns     []string
}

func (f *csvFormatter) WriteResponse(ctx context.Context, w io.Writer, resp Response) (err error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	wr := csv.NewWriter(w)
	if resp.Err != nil {
		wr.Write([]string{"error"})
		wr.Write([]string{resp.Err.Error()})
		wr.Flush()
		return wr.Error()
	}

	for _, result := range resp.Results {
		if result.StatementID != f.statementID {
			// If there are no series in the result, skip past this result.
			if len(result.Series) == 0 {
				continue
			}

			// Set the statement id and print out a newline if this is not the first statement.
			if f.statementID >= 0 {
				// Flush the csv writer and write a newline.
				wr.Flush()
				if err := wr.Error(); err != nil {
					return err
				}

				if _, err := io.WriteString(w, "\n"); err != nil {
					return err
				}
			}
			f.statementID = result.StatementID

			// Print out the column headers from the first series.
			f.columns = make([]string, 2+len(result.Series[0].Columns))
			f.columns[0] = "name"
			f.columns[1] = "tags"
			copy(f.columns[2:], result.Series[0].Columns)
			if err := wr.Write(f.columns); err != nil {
				return err
			}
		}

		for i, row := range result.Series {
			if i > 0 && !stringsEqual(result.Series[i-1].Columns, row.Columns) {
				// The columns have changed. Print a newline and reprint the header.
				wr.Flush()
				if err := wr.Error(); err != nil {
					return err
				}

				if _, err := io.WriteString(w, "\n"); err != nil {
					return err
				}

				f.columns = make([]string, 2+len(row.Columns))
				f.columns[0] = "name"
				f.columns[1] = "tags"
				copy(f.columns[2:], row.Columns)
				if err := wr.Write(f.columns); err != nil {
					return err
				}
			}

			f.columns[0] = row.Name
			f.columns[1] = ""
			if len(row.Tags) > 0 {
				hashKey := models.NewTags(row.Tags).HashKey()
				if len(hashKey) > 0 {
					f.columns[1] = string(hashKey[1:])
				}
			}
			for _, values := range row.Values {
				for i, value := range values {
					if value == nil {
						f.columns[i+2] = ""
						continue
					}

					switch v := value.(type) {
					case float64:
						f.columns[i+2] = strconv.FormatFloat(v, 'f', -1, 64)
					case int64:
						f.columns[i+2] = strconv.FormatInt(v, 10)
					case uint64:
						f.columns[i+2] = strconv.FormatUint(v, 10)
					case string:
						f.columns[i+2] = v
					case bool:
						if v {
							f.columns[i+2] = "true"
						} else {
							f.columns[i+2] = "false"
						}
					case time.Time:
						f.columns[i+2] = strconv.FormatInt(v.UnixNano(), 10)
					case *float64, *int64, *string, *bool:
						f.columns[i+2] = ""
					}
				}
				wr.Write(f.columns)
			}
		}
	}
	wr.Flush()
	return wr.Error()
}

type msgpFormatter struct{}

func (f *msgpFormatter) ContentType() string {
	return "application/x-msgpack"
}

func (f *msgpFormatter) WriteResponse(ctx context.Context, w io.Writer, resp Response) (err error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	enc := msgp.NewWriter(w)
	defer enc.Flush()

	enc.WriteMapHeader(1)
	if resp.Err != nil {
		enc.WriteString("error")
		enc.WriteString(resp.Err.Error())
		return nil
	} else {
		enc.WriteString("results")
		enc.WriteArrayHeader(uint32(len(resp.Results)))
		for _, result := range resp.Results {
			if result.Err != nil {
				enc.WriteMapHeader(1)
				enc.WriteString("error")
				enc.WriteString(result.Err.Error())
				continue
			}

			sz := 2
			if len(result.Messages) > 0 {
				sz++
			}
			if result.Partial {
				sz++
			}
			enc.WriteMapHeader(uint32(sz))
			enc.WriteString("statement_id")
			enc.WriteInt(result.StatementID)
			if len(result.Messages) > 0 {
				enc.WriteString("messages")
				enc.WriteArrayHeader(uint32(len(result.Messages)))
				for _, msg := range result.Messages {
					enc.WriteMapHeader(2)
					enc.WriteString("level")
					enc.WriteString(msg.Level)
					enc.WriteString("text")
					enc.WriteString(msg.Text)
				}
			}
			enc.WriteString("series")
			enc.WriteArrayHeader(uint32(len(result.Series)))
			for _, series := range result.Series {
				sz := 2
				if series.Name != "" {
					sz++
				}
				if len(series.Tags) > 0 {
					sz++
				}
				if series.Partial {
					sz++
				}
				enc.WriteMapHeader(uint32(sz))
				if series.Name != "" {
					enc.WriteString("name")
					enc.WriteString(series.Name)
				}
				if len(series.Tags) > 0 {
					enc.WriteString("tags")
					enc.WriteMapHeader(uint32(len(series.Tags)))
					for k, v := range series.Tags {
						enc.WriteString(k)
						enc.WriteString(v)
					}
				}
				enc.WriteString("columns")
				enc.WriteArrayHeader(uint32(len(series.Columns)))
				for _, col := range series.Columns {
					enc.WriteString(col)
				}
				enc.WriteString("values")
				enc.WriteArrayHeader(uint32(len(series.Values)))
				for _, values := range series.Values {
					enc.WriteArrayHeader(uint32(len(values)))
					for _, v := range values {
						enc.WriteIntf(v)
					}
				}
				if series.Partial {
					enc.WriteString("partial")
					enc.WriteBool(series.Partial)
				}
			}
			if result.Partial {
				enc.WriteString("partial")
				enc.WriteBool(true)
			}
		}
	}
	return nil
}

func stringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func tagsEqual(prev, current map[string]string) bool {
	return reflect.DeepEqual(prev, current)
}

func columnsEqual(prev, current []string) bool {
	return reflect.DeepEqual(prev, current)
}

func headersEqual(prev, current models.Row) bool {
	if prev.Name != current.Name {
		return false
	}
	return tagsEqual(prev.Tags, current.Tags) && columnsEqual(prev.Columns, current.Columns)
}

type textFormatter struct{}

func (f *textFormatter) WriteResponse(ctx context.Context, w io.Writer, resp Response) (err error) {
	span, _ := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	if err := resp.Error(); err != nil {
		fmt.Fprintln(w, err.Error())
		return nil
	}
	// Create a tabbed writer for each result as they won't always line up
	writer := new(tabwriter.Writer)
	writer.Init(w, 0, 8, 1, ' ', 0)

	var previousHeaders models.Row
	for i, result := range resp.Results {
		// Print out all messages first
		for _, m := range result.Messages {
			fmt.Fprintf(w, "%s: %s.\n", m.Level, m.Text)
		}
		// Check to see if the headers are the same as the previous row.  If so, suppress them in the output
		suppressHeaders := len(result.Series) > 0 && headersEqual(previousHeaders, *result.Series[0])
		if !suppressHeaders && len(result.Series) > 0 {
			previousHeaders = models.Row{
				Name:    result.Series[0].Name,
				Tags:    result.Series[0].Tags,
				Columns: result.Series[0].Columns,
			}
		}

		// If we are suppressing headers, don't output the extra line return. If we
		// aren't suppressing headers, then we put out line returns between results
		// (not before the first result, and not after the last result).
		if !suppressHeaders && i > 0 {
			fmt.Fprintln(writer, "")
		}

		rows := f.formatResults(result.Series, "\t", suppressHeaders)
		for _, r := range rows {
			fmt.Fprintln(writer, r)
		}

	}
	_ = writer.Flush()
	return nil
}

func (f *textFormatter) formatResults(result models.Rows, separator string, suppressHeaders bool) []string {
	var rows []string
	// Create a tabbed writer for each result as they won't always line up
	for i, row := range result {
		// gather tags
		var tags []string
		for k, v := range row.Tags {
			tags = append(tags, fmt.Sprintf("%s=%s", k, v))
			sort.Strings(tags)
		}

		var columnNames []string

		columnNames = append(columnNames, row.Columns...)

		// Output a line separator if we have more than one set or results and format is column
		if i > 0 && !suppressHeaders {
			rows = append(rows, "")
		}

		// If we are column format, we break out the name/tag to separate lines
		if !suppressHeaders {
			if row.Name != "" {
				n := fmt.Sprintf("name: %s", row.Name)
				rows = append(rows, n)
			}
			if len(tags) > 0 {
				t := fmt.Sprintf("tags: %s", strings.Join(tags, ", "))
				rows = append(rows, t)
			}
		}

		if !suppressHeaders {
			rows = append(rows, strings.Join(columnNames, separator))
		}

		// if format is column, write dashes under each column
		if !suppressHeaders {
			var lines []string
			for _, columnName := range columnNames {
				lines = append(lines, strings.Repeat("-", len(columnName)))
			}
			rows = append(rows, strings.Join(lines, separator))
		}

		for _, v := range row.Values {
			var values []string

			for _, vv := range v {
				values = append(values, interfaceToString(vv))
			}
			rows = append(rows, strings.Join(values, separator))
		}
	}
	return rows
}

func interfaceToString(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case bool:
		return fmt.Sprintf("%v", v)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr:
		return fmt.Sprintf("%d", t)
	case float32:
		// Default for floats via `fmt.Sprintf("%v", t)` is to represent them in scientific notation.
		// We want to represent them as they are, with the least digits as possible (prec: -1).
		return strconv.FormatFloat(float64(t), 'f', -1, 32)
	case float64:
		// Default for floats via `fmt.Sprintf("%v", t)` is to represent them in scientific notation.
		// We want to represent them as they are, with the least digits as possible (prec: -1).
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", t)
	}
}
