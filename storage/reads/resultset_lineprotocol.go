package reads

import (
	"errors"
	"io"
	"strconv"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/tsdb/cursors"
)

// ResultSetToLineProtocol transforms rs to line protocol and writes the
// output to wr.
func ResultSetToLineProtocol(wr io.Writer, rs ResultSet) (err error) {
	defer rs.Close()

	line := make([]byte, 0, 4096)
	for rs.Next() {
		tags := rs.Tags()
		name := tags.Get(models.MeasurementTagKeyBytes)
		field := tags.Get(models.FieldKeyTagKeyBytes)
		if len(name) == 0 || len(field) == 0 {
			return errors.New("missing measurement / field")
		}

		line = append(line[:0], name...)
		if tags.Len() > 2 {
			tags = tags[1 : len(tags)-1] // take first and last elements which are measurement and field keys
			line = tags.AppendHashKey(line)
		}

		line = append(line, ' ')
		line = append(line, field...)
		line = append(line, '=')
		err = cursorToLineProtocol(wr, line, rs.Cursor())
		if err != nil {
			return err
		}
	}

	return rs.Err()
}

func cursorToLineProtocol(wr io.Writer, line []byte, cur cursors.Cursor) error {
	var newLine = []byte{'\n'}

	switch ccur := cur.(type) {
	case cursors.IntegerArrayCursor:
		for {
			a := ccur.Next()
			if a.Len() > 0 {
				for i := range a.Timestamps {
					buf := strconv.AppendInt(line, a.Values[i], 10)
					buf = append(buf, 'i', ' ')
					buf = strconv.AppendInt(buf, a.Timestamps[i], 10)
					wr.Write(buf)
					wr.Write(newLine)
				}
			} else {
				break
			}
		}
	case cursors.FloatArrayCursor:
		for {
			a := ccur.Next()
			if a.Len() > 0 {
				for i := range a.Timestamps {
					buf := strconv.AppendFloat(line, a.Values[i], 'f', -1, 64)
					buf = append(buf, ' ')
					buf = strconv.AppendInt(buf, a.Timestamps[i], 10)
					wr.Write(buf)
					wr.Write(newLine)
				}
			} else {
				break
			}
		}
	case cursors.UnsignedArrayCursor:
		for {
			a := ccur.Next()
			if a.Len() > 0 {
				for i := range a.Timestamps {
					buf := strconv.AppendUint(line, a.Values[i], 10)
					buf = append(buf, 'u', ' ')
					buf = strconv.AppendInt(buf, a.Timestamps[i], 10)
					wr.Write(buf)
					wr.Write(newLine)
				}
			} else {
				break
			}
		}
	case cursors.BooleanArrayCursor:
		for {
			a := ccur.Next()
			if a.Len() > 0 {
				for i := range a.Timestamps {
					buf := strconv.AppendBool(line, a.Values[i])
					buf = append(buf, ' ')
					buf = strconv.AppendInt(buf, a.Timestamps[i], 10)
					wr.Write(buf)
					wr.Write(newLine)
				}
			} else {
				break
			}
		}
	case cursors.StringArrayCursor:
		for {
			a := ccur.Next()
			if a.Len() > 0 {
				for i := range a.Timestamps {
					buf := strconv.AppendQuote(line, a.Values[i])
					buf = append(buf, ' ')
					buf = strconv.AppendInt(buf, a.Timestamps[i], 10)
					wr.Write(buf)
					wr.Write(newLine)
				}
			} else {
				break
			}
		}
	default:
		panic("unreachable")
	}

	cur.Close()
	return cur.Err()
}
