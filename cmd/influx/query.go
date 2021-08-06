package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/values"
	ihttp "github.com/influxdata/influxdb/v2/http"
	"github.com/spf13/cobra"
)

var queryFlags struct {
	org       organization
	file      string
	raw       bool
	profilers []string
}

func cmdQuery(f *globalFlags, opts genericCLIOpts) *cobra.Command {
	cmd := opts.newCmd("query [query literal or -f /path/to/query.flux]", fluxQueryF, true)
	cmd.Short = "Execute a Flux query"
	cmd.Long = `Execute a Flux query provided via the first argument or a file or stdin`
	cmd.Args = cobra.MaximumNArgs(1)

	f.registerFlags(opts.viper, cmd)
	queryFlags.org.register(opts.viper, cmd, true)
	cmd.Flags().StringVarP(&queryFlags.file, "file", "f", "", "Path to Flux query file")
	cmd.Flags().BoolVarP(&queryFlags.raw, "raw", "r", false, "Display raw query results")
	cmd.Flags().StringSliceVarP(&queryFlags.profilers, "profilers", "p", nil, "Names of Flux profilers to enable. Profiler information will be appended to query results")

	return cmd
}

// readFluxQuery returns first argument, file contents or stdin
func readFluxQuery(args []string, file string) (string, error) {
	// backward compatibility
	if len(args) > 0 {
		if strings.HasPrefix(args[0], "@") {
			file = args[0][1:]
			args = args[:0]
		} else if args[0] == "-" {
			file = ""
			args = args[:0]
		}
	}

	var query string
	switch {
	case len(args) > 0:
		query = args[0]
	case len(file) > 0:
		content, err := ioutil.ReadFile(file)
		if err != nil {
			return "", err
		}
		query = string(content)
	default:
		content, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return "", err
		}
		query = string(content)
	}
	return query, nil
}

func fluxQueryF(cmd *cobra.Command, args []string) error {
	if err := queryFlags.org.validOrgFlags(&flags); err != nil {
		return err
	}

	q, err := readFluxQuery(args, queryFlags.file)
	if err != nil {
		return fmt.Errorf("failed to load query: %v", err)
	}

	u, err := url.Parse(flags.config().Host)
	if err != nil {
		return fmt.Errorf("unable to parse host: %s", err)
	}

	if !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}
	u.Path += "api/v2/query"

	params := url.Values{}
	if queryFlags.org.id != "" {
		params.Set("orgID", queryFlags.org.id)
	} else {
		params.Set("org", queryFlags.org.name)
	}
	u.RawQuery = params.Encode()

	var body_map = map[string]interface{}{
		"query": q,
		"type":  "flux",
		"dialect": map[string]interface{}{
			"annotations": []string{"group", "datatype", "default"},
			"delimiter":   ",",
			"header":      true,
		},
	}

	if len(queryFlags.profilers) > 0 {
		body_map["extern"] = buildProfilersExtern(queryFlags.profilers)
	}

	body, _ := json.Marshal(body_map)

	req, _ := http.NewRequest("POST", u.String(), bytes.NewReader(body))
	req.Header.Set("Authorization", "Token "+flags.config().Token)
	req.Header.Set("Content-Type", "application/json")

	client := ihttp.NewClient(u.Scheme, flags.skipVerify)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if err := ihttp.CheckError(resp); err != nil {
		return err
	}

	if queryFlags.raw {
		io.Copy(os.Stdout, resp.Body)
		return nil
	}

	dec := csv.NewMultiResultDecoder(csv.ResultDecoderConfig{})
	results, err := dec.Decode(resp.Body)
	if err != nil {
		return fmt.Errorf("query decode error: %s", err)
	}
	defer results.Release()

	for results.More() {
		res := results.Next()
		fmt.Println("Result:", res.Name())

		if err := res.Tables().Do(func(tbl flux.Table) error {
			_, err := newFormatter(tbl).WriteTo(os.Stdout)
			return err
		}); err != nil {
			return err
		}
	}
	// It is safe and appropriate to call Release multiple times and must be
	// called before checking the error on the next line.
	results.Release()
	return results.Err()
}

// buildProfilersExtern constructs the AST representation of a Flux statement enabling
// the specified profilers in the query options.
//
// See the docs for more info: https://docs.influxdata.com/influxdb/cloud/reference/flux/stdlib/profiler/
func buildProfilersExtern(profilersToEnable []string) (profilersExtern map[string]interface{}) {
	elements := make([]interface{}, len(profilersToEnable))
	for i, profiler := range profilersToEnable {
		elements[i] = map[string]interface{}{
			"type":  "StringLiteral",
			"value": profiler,
		}
	}
	profilersExtern = map[string]interface{}{
		"type": "File",
		"imports": []interface{}{
			map[string]interface{}{
				"type": "ImportDeclaration",
				"path": map[string]interface{}{
					"type":  "StringLiteral",
					"value": "profiler",
				},
			},
		},
		"body": []interface{}{
			map[string]interface{}{
				"assignment": map[string]interface{}{
					"member": map[string]interface{}{
						"object": map[string]interface{}{
							"name": "profiler",
							"type": "Identifier",
						},
						"property": map[string]interface{}{
							"name": "enabledProfilers",
							"type": "Identifier",
						},
						"type": "MemberExpression",
					},
					"init": map[string]interface{}{
						"type":     "ArrayExpression",
						"elements": elements,
					},
					"type": "MemberAssignment",
				},
				"type": "OptionStatement",
			},
		},
	}
	return profilersExtern
}

// Below is a copy and trimmed version of the execute/format.go file from flux.
// It is copied here to avoid requiring a dependency on the execute package which
// may pull in the flux runtime as a dependency.
// In the future, the formatters and other primitives such as the csv parser should
// probably be separated out into user libraries anyway.

const fixedWidthTimeFmt = "2006-01-02T15:04:05.000000000Z"

// formatter writes a table to a Writer.
type formatter struct {
	tbl       flux.Table
	widths    []int
	maxWidth  int
	newWidths []int
	pad       []byte
	dash      []byte
	// fmtBuf is used to format values
	fmtBuf [64]byte

	cols orderedCols
}

var eol = []byte{'\n'}

// newFormatter creates a formatter for a given table.
func newFormatter(tbl flux.Table) *formatter {
	return &formatter{
		tbl: tbl,
	}
}

type writeToHelper struct {
	w   io.Writer
	n   int64
	err error
}

func (w *writeToHelper) write(data []byte) {
	if w.err != nil {
		return
	}
	n, err := w.w.Write(data)
	w.n += int64(n)
	w.err = err
}

var minWidthsByType = map[flux.ColType]int{
	flux.TBool:    12,
	flux.TInt:     26,
	flux.TUInt:    27,
	flux.TFloat:   28,
	flux.TString:  22,
	flux.TTime:    len(fixedWidthTimeFmt),
	flux.TInvalid: 10,
}

// WriteTo writes the formatted table data to w.
func (f *formatter) WriteTo(out io.Writer) (int64, error) {
	w := &writeToHelper{w: out}

	// Sort cols
	cols := f.tbl.Cols()
	f.cols = newOrderedCols(cols, f.tbl.Key())
	sort.Sort(f.cols)

	// Compute header widths
	f.widths = make([]int, len(cols))
	for j, c := range cols {
		// Column header is "<label>:<type>"
		l := len(c.Label) + len(c.Type.String()) + 1
		min := minWidthsByType[c.Type]
		if min > l {
			l = min
		}
		if l > f.widths[j] {
			f.widths[j] = l
		}
		if l > f.maxWidth {
			f.maxWidth = l
		}
	}

	// Write table header
	w.write([]byte("Table: keys: ["))
	labels := make([]string, len(f.tbl.Key().Cols()))
	for i, c := range f.tbl.Key().Cols() {
		labels[i] = c.Label
	}
	w.write([]byte(strings.Join(labels, ", ")))
	w.write([]byte("]"))
	w.write(eol)

	// Check err and return early
	if w.err != nil {
		return w.n, w.err
	}

	// Write rows
	r := 0
	w.err = f.tbl.Do(func(cr flux.ColReader) error {
		if r == 0 {
			l := cr.Len()
			for i := 0; i < l; i++ {
				for oj, c := range f.cols.cols {
					j := f.cols.Idx(oj)
					buf := f.valueBuf(i, j, c.Type, cr)
					l := len(buf)
					if l > f.widths[j] {
						f.widths[j] = l
					}
					if l > f.maxWidth {
						f.maxWidth = l
					}
				}
			}
			f.makePaddingBuffers()
			f.writeHeader(w)
			f.writeHeaderSeparator(w)
			f.newWidths = make([]int, len(f.widths))
			copy(f.newWidths, f.widths)
		}
		l := cr.Len()
		for i := 0; i < l; i++ {
			for oj, c := range f.cols.cols {
				j := f.cols.Idx(oj)
				buf := f.valueBuf(i, j, c.Type, cr)
				l := len(buf)
				padding := f.widths[j] - l
				if padding >= 0 {
					w.write(f.pad[:padding])
					w.write(buf)
				} else {
					//TODO make unicode friendly
					w.write(buf[:f.widths[j]-3])
					w.write([]byte{'.', '.', '.'})
				}
				w.write(f.pad[:2])
				if l > f.newWidths[j] {
					f.newWidths[j] = l
				}
				if l > f.maxWidth {
					f.maxWidth = l
				}
			}
			w.write(eol)
			r++
		}
		return w.err
	})
	return w.n, w.err
}

func (f *formatter) makePaddingBuffers() {
	if len(f.pad) != f.maxWidth {
		f.pad = make([]byte, f.maxWidth)
		for i := range f.pad {
			f.pad[i] = ' '
		}
	}
	if len(f.dash) != f.maxWidth {
		f.dash = make([]byte, f.maxWidth)
		for i := range f.dash {
			f.dash[i] = '-'
		}
	}
}

func (f *formatter) writeHeader(w *writeToHelper) {
	for oj, c := range f.cols.cols {
		j := f.cols.Idx(oj)
		buf := append(append([]byte(c.Label), ':'), []byte(c.Type.String())...)
		w.write(f.pad[:f.widths[j]-len(buf)])
		w.write(buf)
		w.write(f.pad[:2])
	}
	w.write(eol)
}

func (f *formatter) writeHeaderSeparator(w *writeToHelper) {
	for oj := range f.cols.cols {
		j := f.cols.Idx(oj)
		w.write(f.dash[:f.widths[j]])
		w.write(f.pad[:2])
	}
	w.write(eol)
}

func (f *formatter) valueBuf(i, j int, typ flux.ColType, cr flux.ColReader) []byte {
	buf := []byte("")
	switch typ {
	case flux.TBool:
		if cr.Bools(j).IsValid(i) {
			buf = strconv.AppendBool(f.fmtBuf[0:0], cr.Bools(j).Value(i))
		}
	case flux.TInt:
		if cr.Ints(j).IsValid(i) {
			buf = strconv.AppendInt(f.fmtBuf[0:0], cr.Ints(j).Value(i), 10)
		}
	case flux.TUInt:
		if cr.UInts(j).IsValid(i) {
			buf = strconv.AppendUint(f.fmtBuf[0:0], cr.UInts(j).Value(i), 10)
		}
	case flux.TFloat:
		if cr.Floats(j).IsValid(i) {
			// TODO allow specifying format and precision
			buf = strconv.AppendFloat(f.fmtBuf[0:0], cr.Floats(j).Value(i), 'f', -1, 64)
		}
	case flux.TString:
		if cr.Strings(j).IsValid(i) {
			buf = []byte(cr.Strings(j).Value(i))
		}
	case flux.TTime:
		if cr.Times(j).IsValid(i) {
			buf = []byte(values.Time(cr.Times(j).Value(i)).String())
		}
	}
	return buf
}

// orderedCols sorts a list of columns:
//
// * time
// * common tags sorted by label
// * other tags sorted by label
// * value
//
type orderedCols struct {
	indexMap []int
	cols     []flux.ColMeta
	key      flux.GroupKey
}

func newOrderedCols(cols []flux.ColMeta, key flux.GroupKey) orderedCols {
	indexMap := make([]int, len(cols))
	for i := range indexMap {
		indexMap[i] = i
	}
	cpy := make([]flux.ColMeta, len(cols))
	copy(cpy, cols)
	return orderedCols{
		indexMap: indexMap,
		cols:     cpy,
		key:      key,
	}
}

func (o orderedCols) Idx(oj int) int {
	return o.indexMap[oj]
}

func (o orderedCols) Len() int { return len(o.cols) }
func (o orderedCols) Swap(i int, j int) {
	o.cols[i], o.cols[j] = o.cols[j], o.cols[i]
	o.indexMap[i], o.indexMap[j] = o.indexMap[j], o.indexMap[i]
}

func (o orderedCols) Less(i int, j int) bool {
	ki := colIdx(o.cols[i].Label, o.key.Cols())
	kj := colIdx(o.cols[j].Label, o.key.Cols())
	if ki >= 0 && kj >= 0 {
		return ki < kj
	} else if ki >= 0 {
		return true
	} else if kj >= 0 {
		return false
	}

	return i < j
}

func colIdx(label string, cols []flux.ColMeta) int {
	for j, c := range cols {
		if c.Label == label {
			return j
		}
	}
	return -1
}
