package reads_test

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/tsdb/cursors"
)

func CursorToString(wr io.Writer, cur cursors.Cursor, opts ...optionFn) {
	switch ccur := cur.(type) {
	case cursors.IntegerArrayCursor:
		fmt.Fprintln(wr, "Integer")
		for {
			a := ccur.Next()
			if a.Len() > 0 {
				for i := range a.Timestamps {
					fmt.Fprintf(wr, "%20d | %20d\n", a.Timestamps[i], a.Values[i])
				}
			} else {
				break
			}
		}
	case cursors.FloatArrayCursor:
		fmt.Fprintln(wr, "Float")
		for {
			a := ccur.Next()
			if a.Len() > 0 {
				for i := range a.Timestamps {
					fmt.Fprintf(wr, "%20d | %18.2f\n", a.Timestamps[i], a.Values[i])
				}
			} else {
				break
			}
		}
	case cursors.UnsignedArrayCursor:
		fmt.Fprintln(wr, "Unsigned")
		for {
			a := ccur.Next()
			if a.Len() > 0 {
				for i := range a.Timestamps {
					fmt.Fprintf(wr, "%20d | %20d\n", a.Timestamps[i], a.Values[i])
				}
			} else {
				break
			}
		}
	case cursors.BooleanArrayCursor:
		fmt.Fprintln(wr, "Boolean")
		for {
			a := ccur.Next()
			if a.Len() > 0 {
				for i := range a.Timestamps {
					fmt.Fprintf(wr, "%20d | %t\n", a.Timestamps[i], a.Values[i])
				}
			} else {
				break
			}
		}
	case cursors.StringArrayCursor:
		fmt.Fprintln(wr, "String")
		for {
			a := ccur.Next()
			if a.Len() > 0 {
				for i := range a.Timestamps {
					fmt.Fprintf(wr, "%20d | %20s\n", a.Timestamps[i], a.Values[i])
				}
			} else {
				break
			}
		}
	default:
		fmt.Fprintln(wr, "Invalid")
		fmt.Fprintf(wr, "unreachable: %T\n", cur)
	}

	if err := cur.Err(); err != nil && err != io.EOF {
		fmt.Fprintf(wr, "cursor err: %s\n", cur.Err().Error())
	}

	cur.Close()
}

const nilVal = "<nil>"

var (
	nilValBytes = []byte(nilVal)
)

func joinString(b [][]byte) string {
	s := make([]string, len(b))
	for i := range b {
		v := b[i]
		if len(v) == 0 {
			s[i] = nilVal
		} else {
			s[i] = string(v)
		}
	}
	return strings.Join(s, ",")
}

func TagsToString(wr io.Writer, tags models.Tags, opts ...optionFn) {
	if k := tags.HashKey(); len(k) > 0 {
		fmt.Fprintf(wr, "%s", string(k[1:]))
	}
	fmt.Fprintln(wr)
}

func ResultSetToString(wr io.Writer, rs reads.ResultSet, opts ...optionFn) {
	var po PrintOptions
	for _, o := range opts {
		o(&po)
	}

	iw := ensureIndentWriter(wr)
	wr = iw

	for rs.Next() {
		fmt.Fprint(wr, "series: ")
		TagsToString(wr, rs.Tags())
		cur := rs.Cursor()

		if po.SkipNilCursor && cur == nil {
			continue
		}

		iw.Indent(2)

		fmt.Fprint(wr, "cursor:")
		if cur == nil {
			fmt.Fprintln(wr, nilVal)
			goto LOOP
		}

		CursorToString(wr, cur)
	LOOP:
		iw.Indent(-2)
	}
}

func GroupResultSetToString(wr io.Writer, rs reads.GroupResultSet, opts ...optionFn) {
	iw := ensureIndentWriter(wr)
	wr = iw

	gc := rs.Next()
	for gc != nil {
		fmt.Fprintln(wr, "group:")
		iw.Indent(2)
		fmt.Fprintf(wr, "tag key      : %s\n", joinString(gc.Keys()))
		fmt.Fprintf(wr, "partition key: %s\n", joinString(gc.PartitionKeyVals()))
		iw.Indent(2)
		ResultSetToString(wr, gc, opts...)
		iw.Indent(-4)
		gc = rs.Next()
	}
}

type PrintOptions struct {
	SkipNilCursor bool
}

type optionFn func(o *PrintOptions)

func SkipNilCursor() optionFn {
	return func(o *PrintOptions) {
		o.SkipNilCursor = true
	}
}

type indentWriter struct {
	l   int
	p   []byte
	wr  io.Writer
	bol bool
}

func ensureIndentWriter(wr io.Writer) *indentWriter {
	if iw, ok := wr.(*indentWriter); ok {
		return iw
	} else {
		return newIndentWriter(wr)
	}
}

func newIndentWriter(wr io.Writer) *indentWriter {
	return &indentWriter{
		wr:  wr,
		bol: true,
	}
}

func (w *indentWriter) Indent(n int) {
	w.l += n
	if w.l < 0 {
		panic("negative indent")
	}
	w.p = bytes.Repeat([]byte(" "), w.l)
}

func (w *indentWriter) Write(p []byte) (n int, err error) {
	for _, c := range p {
		if w.bol {
			_, err = w.wr.Write(w.p)
			if err != nil {
				break
			}
			w.bol = false
		}
		_, err = w.wr.Write([]byte{c})
		if err != nil {
			break
		}
		n++
		w.bol = c == '\n'
	}

	return n, err
}
