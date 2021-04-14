package main

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"

	platform2 "github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/fujiwara/shapeio"
	platform "github.com/influxdata/influxdb/v2"
	ihttp "github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/kit/signals"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/pkg/csv2lp"
	"github.com/influxdata/influxdb/v2/write"
	"github.com/spf13/cobra"
)

const (
	inputFormatCsv          = "csv"
	inputFormatLineProtocol = "lp"
	inputCompressionNone    = "none"
	inputCompressionGzip    = "gzip"
)

type buildWriteSvcFn func(builder *writeFlagsBuilder) platform.WriteService

type writeFlagsBuilder struct {
	genericCLIOpts
	*globalFlags

	svcFn buildWriteSvcFn

	org                        organization
	BucketID                   string
	Bucket                     string
	Precision                  string
	Format                     string
	Headers                    []string
	Files                      []string
	URLs                       []string
	Debug                      bool
	SkipRowOnError             bool
	SkipHeader                 int
	MaxLineLength              int
	IgnoreDataTypeInColumnName bool
	Encoding                   string
	ErrorsFile                 string
	RateLimit                  string
	Compression                string
}

func newWriteFlagsBuilder(svcFn buildWriteSvcFn, f *globalFlags, opt genericCLIOpts) *writeFlagsBuilder {
	return &writeFlagsBuilder{
		genericCLIOpts: opt,
		globalFlags:    f,
		svcFn:          svcFn,
	}
}

func cmdWrite(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	builder := newWriteFlagsBuilder(newBatchingWriteService, f, opt)
	return builder.cmd()
}

func newBatchingWriteService(b *writeFlagsBuilder) platform.WriteService {
	ac := b.config()
	return &write.Batcher{
		Service: &ihttp.WriteService{
			Addr:               ac.Host,
			Token:              ac.Token,
			Precision:          b.Precision,
			InsecureSkipVerify: b.skipVerify,
		},
		MaxLineLength: b.MaxLineLength,
	}
}

func (b *writeFlagsBuilder) cmd() *cobra.Command {
	cmd := b.newCmd("write", b.writeRunE, true)
	cmd.Args = cobra.MaximumNArgs(1)
	cmd.Short = "Write points to InfluxDB"
	cmd.Long = `Write data to InfluxDB via stdin, or add an entire file specified with the -f flag`

	b.registerFlags(b.viper, cmd)
	b.org.register(b.viper, cmd, true)
	opts := flagOpts{
		{
			DestP:      &b.BucketID,
			Flag:       "bucket-id",
			Desc:       "The ID of destination bucket",
			Persistent: true,
		},
		{
			DestP:      &b.Bucket,
			Flag:       "bucket",
			Short:      'b',
			EnvVar:     "BUCKET_NAME",
			Desc:       "The name of destination bucket",
			Persistent: true,
		},
		{
			DestP:      &b.Precision,
			Flag:       "precision",
			Short:      'p',
			Default:    "ns",
			Desc:       "Precision of the timestamps of the lines",
			Persistent: true,
		},
	}
	opts.mustRegister(b.viper, cmd)
	cmd.PersistentFlags().StringVar(&b.Format, "format", "", "Input format, either lp (Line Protocol) or csv (Comma Separated Values). Defaults to lp unless '.csv' extension")
	cmd.PersistentFlags().StringArrayVar(&b.Headers, "header", []string{}, "Header prepends lines to input data; Example --header HEADER1 --header HEADER2")
	cmd.PersistentFlags().StringArrayVarP(&b.Files, "file", "f", []string{}, "The path to the file to import")
	cmd.PersistentFlags().StringArrayVarP(&b.URLs, "url", "u", []string{}, "The URL to import data from")
	cmd.PersistentFlags().BoolVar(&b.Debug, "debug", false, "Log CSV columns to stderr before reading data rows")
	cmd.PersistentFlags().BoolVar(&b.SkipRowOnError, "skipRowOnError", false, "Log CSV data errors to stderr and continue with CSV processing")
	cmd.PersistentFlags().IntVar(&b.SkipHeader, "skipHeader", 0, "Skip the first <n> rows from input data")
	cmd.PersistentFlags().IntVar(&b.MaxLineLength, "max-line-length", 16_000_000, "Specifies the maximum number of bytes that can be read for a single line")
	cmd.Flag("skipHeader").NoOptDefVal = "1" // skipHeader flag value is optional, skip the first header when unspecified
	cmd.PersistentFlags().BoolVar(&b.IgnoreDataTypeInColumnName, "xIgnoreDataTypeInColumnName", false, "Ignores dataType which could be specified after ':' in column name")
	cmd.PersistentFlags().MarkHidden("xIgnoreDataTypeInColumnName") // should be used only upon explicit advice
	cmd.PersistentFlags().StringVar(&b.Encoding, "encoding", "UTF-8", "Character encoding of input files or stdin")
	cmd.PersistentFlags().StringVar(&b.ErrorsFile, "errors-file", "", "The path to the file to write rejected rows to")
	cmd.PersistentFlags().StringVar(&b.RateLimit, "rate-limit", "", "Throttles write, examples: \"5 MB / 5 min\" , \"17kBs\". \"\" (default) disables throttling.")
	cmd.PersistentFlags().StringVar(&b.Compression, "compression", "", "Input compression, either 'none' or 'gzip'. Defaults to 'none' unless an input has a '.gz' extension")

	cmdDryRun := b.newCmd("dryrun", b.writeDryrunE, false)
	cmdDryRun.Args = cobra.MaximumNArgs(1)
	cmdDryRun.Short = "Write to stdout instead of InfluxDB"
	cmdDryRun.Long = `Write protocol lines to stdout instead of InfluxDB. Troubleshoot conversion from CSV to line protocol.`
	b.registerFlags(b.viper, cmdDryRun)
	cmd.AddCommand(cmdDryRun)
	return cmd
}

func (b *writeFlagsBuilder) dump(args []string) {
	if b.Debug {
		log.Printf("WriteFlags%+v args:%v", *b, args)
	}
}

// createLineReader uses writeFlags and cli arguments to create a reader that produces line protocol
func (b *writeFlagsBuilder) createLineReader(ctx context.Context, cmd *cobra.Command, args []string) (io.Reader, io.Closer, error) {
	files := b.Files
	if len(args) > 0 && len(args[0]) > 1 && args[0][0] == '@' {
		// backward compatibility: @ in arg denotes a file
		files = append(files, args[0][1:])
		args = args[:0]
	}

	readers := make([]io.Reader, 0, 2*len(b.Headers)+2*len(files)+2*len(b.URLs)+1)
	closers := make([]io.Closer, 0, len(files)+len(b.URLs))

	// validate input format
	if len(b.Format) > 0 && b.Format != inputFormatLineProtocol && b.Format != inputFormatCsv {
		return nil, csv2lp.MultiCloser(closers...), fmt.Errorf("unsupported input format: %s", b.Format)
	}
	// validate input compression
	if len(b.Compression) > 0 && b.Compression != inputCompressionNone && b.Compression != inputCompressionGzip {
		return nil, csv2lp.MultiCloser(closers...), fmt.Errorf("unsupported input compression: %s", b.Compression)
	}

	// validate and setup decoding of files/stdin if encoding is supplied
	decode, err := csv2lp.CreateDecoder(b.Encoding)
	if err != nil {
		return nil, csv2lp.MultiCloser(closers...), err
	}

	// utility to manage common steps used to decode / decompress input sources,
	// while tracking resources that must be cleaned-up after reading.
	addReader := func(r io.Reader, name string, compressed bool) error {
		if compressed {
			rcz, err := gzip.NewReader(r)
			if err != nil {
				return fmt.Errorf("failed to decompress %s: %w", name, err)
			}
			closers = append(closers, rcz)
			r = rcz
		}
		readers = append(readers, decode(r), strings.NewReader("\n"))
		return nil
	}

	// prepend header lines
	if len(b.Headers) > 0 {
		for _, header := range b.Headers {
			readers = append(readers, strings.NewReader(header), strings.NewReader("\n"))
		}
		if len(b.Format) == 0 {
			b.Format = inputFormatCsv
		}
	}

	// add files
	if len(files) > 0 {
		for _, file := range files {
			f, err := os.Open(file)
			if err != nil {
				return nil, csv2lp.MultiCloser(closers...), fmt.Errorf("failed to open %q: %v", file, err)
			}
			closers = append(closers, f)

			fname := file
			compressed := b.Compression == "gzip" || (len(b.Compression) == 0 && strings.HasSuffix(fname, ".gz"))
			if compressed {
				fname = strings.TrimSuffix(fname, ".gz")
			}
			if len(b.Format) == 0 && strings.HasSuffix(fname, ".csv") {
				b.Format = inputFormatCsv
			}

			if err = addReader(f, file, compressed); err != nil {
				return nil, csv2lp.MultiCloser(closers...), err
			}
		}
	}

	// #18349 allow URL data sources, a simple alternative to `curl -f -s http://... | influx write ...`
	if len(b.URLs) > 0 {
		client := http.DefaultClient
		for _, addr := range b.URLs {
			u, err := url.Parse(addr)
			if err != nil {
				return nil, csv2lp.MultiCloser(closers...), fmt.Errorf("failed to open %q: %v", addr, err)
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, addr, nil)
			if err != nil {
				return nil, csv2lp.MultiCloser(closers...), fmt.Errorf("failed to open %q: %v", addr, err)
			}
			req.Header.Set("Accept-Encoding", "gzip")
			resp, err := client.Do(req)
			if err != nil {
				return nil, csv2lp.MultiCloser(closers...), fmt.Errorf("failed to open %q: %v", addr, err)
			}
			closers = append(closers, resp.Body)
			if resp.StatusCode/100 != 2 {
				return nil, csv2lp.MultiCloser(closers...), fmt.Errorf("failed to open %q: response status_code=%d", addr, resp.StatusCode)
			}

			compressed := b.Compression == "gzip" ||
				resp.Header.Get("Content-Encoding") == "gzip" ||
				(len(b.Compression) == 0 && strings.HasSuffix(u.Path, ".gz"))
			if compressed {
				u.Path = strings.TrimSuffix(u.Path, ".gz")
			}
			if len(b.Format) == 0 &&
				(strings.HasSuffix(u.Path, ".csv") || strings.HasPrefix(resp.Header.Get("Content-Type"), "text/csv")) {
				b.Format = inputFormatCsv
			}

			if err = addReader(resp.Body, addr, compressed); err != nil {
				return nil, csv2lp.MultiCloser(closers...), err
			}
		}
	}

	// add stdin or a single argument
	switch {
	case len(args) == 0:
		// use also stdIn if it is a terminal
		if !isCharacterDevice(cmd.InOrStdin()) {
			if err = addReader(cmd.InOrStdin(), "stdin", b.Compression == "gzip"); err != nil {
				return nil, csv2lp.MultiCloser(closers...), err
			}
		}
	case args[0] == "-":
		// "-" also means stdin
		if err = addReader(cmd.InOrStdin(), "stdin", b.Compression == "gzip"); err != nil {
			return nil, csv2lp.MultiCloser(closers...), err
		}
	default:
		if err = addReader(strings.NewReader(args[0]), "arg 0", b.Compression == "gzip"); err != nil {
			return nil, csv2lp.MultiCloser(closers...), err
		}
	}

	// skipHeader lines when set
	if b.SkipHeader != 0 {
		// find the last non-string reader (stdin or file)
		for i := len(readers) - 1; i >= 0; i-- {
			_, stringReader := readers[i].(*strings.Reader)
			if !stringReader { // ignore headers and new lines
				readers[i] = csv2lp.SkipHeaderLinesReader(b.SkipHeader, readers[i])
				break
			}
		}
	}

	// create writer for errors-file, if supplied
	var errorsFile *csv.Writer
	var rowSkippedListener func(*csv2lp.CsvToLineReader, error, []string)
	if b.ErrorsFile != "" {
		writer, err := os.Create(b.ErrorsFile)
		if err != nil {
			return nil, csv2lp.MultiCloser(closers...), fmt.Errorf("failed to create %q: %v", b.ErrorsFile, err)
		}
		closers = append(closers, writer)
		errorsFile = csv.NewWriter(writer)
		rowSkippedListener = func(source *csv2lp.CsvToLineReader, lineError error, row []string) {
			log.Println(lineError)
			errorsFile.Comma = source.Comma()
			errorsFile.Write([]string{fmt.Sprintf("# error : %v", lineError)})
			if err := errorsFile.Write(row); err != nil {
				log.Printf("Unable to write to error-file: %v\n", err)
			}
			errorsFile.Flush() // flush is required
		}
	}

	// concatenate readers
	r := io.MultiReader(readers...)
	if b.Format == inputFormatCsv {
		csvReader := csv2lp.CsvToLineProtocol(r)
		csvReader.LogTableColumns(b.Debug)
		csvReader.SkipRowOnError(b.SkipRowOnError)
		csvReader.Table.IgnoreDataTypeInColumnName(b.IgnoreDataTypeInColumnName)
		// change LineNumber to report file/stdin line numbers properly
		csvReader.LineNumber = b.SkipHeader - len(b.Headers)
		csvReader.RowSkipped = rowSkippedListener
		r = csvReader
	}
	// throttle reader if requested
	rateLimit, err := ToBytesPerSecond(b.RateLimit)
	if err != nil {
		return nil, csv2lp.MultiCloser(closers...), err
	}
	if rateLimit > 0.0 {
		// LineReader ensures that original reader is consumed in the smallest possible
		// units (at most one protocol line) to avoid bigger pauses in throttling
		r = csv2lp.NewLineReader(r)
		throttledReader := shapeio.NewReaderWithContext(r, ctx)
		throttledReader.SetRateLimit(rateLimit)
		r = throttledReader
	}

	return r, csv2lp.MultiCloser(closers...), nil
}

func (b *writeFlagsBuilder) writeRunE(cmd *cobra.Command, args []string) error {
	b.dump(args) // print flags when in Debug mode
	// validate InfluxDB flags
	if err := b.org.validOrgFlags(b.globalFlags); err != nil {
		return err
	}

	if b.Bucket != "" && b.BucketID != "" {
		return fmt.Errorf("please specify one of bucket or bucket-id")
	}

	if !models.ValidPrecision(b.Precision) {
		return fmt.Errorf("invalid precision")
	}

	var (
		filter platform.BucketFilter
		err    error
	)

	if b.BucketID != "" {
		filter.ID, err = platform2.IDFromString(b.BucketID)
		if err != nil {
			return fmt.Errorf("failed to decode bucket-id: %v", err)
		}
	}
	if b.Bucket != "" {
		filter.Name = &b.Bucket
	}

	if b.org.id != "" {
		filter.OrganizationID, err = platform2.IDFromString(b.org.id)
		if err != nil {
			return fmt.Errorf("failed to decode org-id id: %v", err)
		}
	}
	if b.org.name != "" {
		filter.Org = &b.org.name
	}

	ctx := signals.WithStandardSignals(context.Background())

	// create line reader
	r, closer, err := b.createLineReader(ctx, cmd, args)
	if closer != nil {
		defer closer.Close()
	}
	if err != nil {
		return err
	}

	s := b.svcFn(b)
	if err := s.WriteTo(ctx, filter, r); err != nil && err != context.Canceled {
		return fmt.Errorf("failed to write data: %v", err)
	}

	return nil
}

func (b *writeFlagsBuilder) writeDryrunE(cmd *cobra.Command, args []string) error {
	b.dump(args) // print flags when in Debug mode
	// create line reader
	ctx := signals.WithStandardSignals(context.Background())
	r, closer, err := b.createLineReader(ctx, cmd, args)
	if closer != nil {
		defer closer.Close()
	}
	if err != nil {
		return err
	}
	// dry run
	_, err = io.Copy(cmd.OutOrStdout(), r)
	if err != nil {
		return fmt.Errorf("failed: %v", err)
	}
	return nil
}

// IsCharacterDevice returns true if the supplied reader is a character device (a terminal)
func isCharacterDevice(reader io.Reader) bool {
	file, isFile := reader.(*os.File)
	if !isFile {
		return false
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) == os.ModeCharDevice
}

var rateLimitRegexp = regexp.MustCompile(`^(\d*\.?\d*)(B|kB|MB)/?(\d*)?(s|sec|m|min)$`)
var bytesUnitMultiplier = map[string]float64{"B": 1, "kB": 1024, "MB": 1_048_576}
var timeUnitMultiplier = map[string]float64{"s": 1, "sec": 1, "m": 60, "min": 60}

// ToBytesPerSecond converts rate from string to number. The supplied string
// value format must be COUNT(B|kB|MB)/TIME(s|sec|m|min) with / and TIME being optional.
// All spaces are ignored, they can help with formatting. Examples: "5 MB / 5 min", 17kbs. 5.1MB5m.
func ToBytesPerSecond(rateLimit string) (float64, error) {
	// ignore all spaces
	strVal := strings.ReplaceAll(rateLimit, " ", "")
	if len(strVal) == 0 {
		return 0, nil
	}

	matches := rateLimitRegexp.FindStringSubmatch(strVal)
	if matches == nil {
		return 0, fmt.Errorf("invalid rate limit %q: it does not match format COUNT(B|kB|MB)/TIME(s|sec|m|min) with / and TIME being optional, rexpexp: %v", strVal, rateLimitRegexp)
	}
	bytes, err := strconv.ParseFloat(matches[1], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid rate limit %q: '%v' is not count of bytes: %v", strVal, matches[1], err)
	}
	bytes = bytes * bytesUnitMultiplier[matches[2]]
	var time float64
	if len(matches[3]) == 0 {
		time = 1 // number is not specified, for example 5kbs or 1Mb/s
	} else {
		int64Val, err := strconv.ParseUint(matches[3], 10, 32)
		if err != nil {
			return 0, fmt.Errorf("invalid rate limit %q: time is out of range: %v", strVal, err)
		}
		if int64Val <= 0 {
			return 0, fmt.Errorf("invalid rate limit %q: positive time expected but %v supplied", strVal, matches[3])
		}
		time = float64(int64Val)
	}
	time = time * timeUnitMultiplier[matches[4]]
	return bytes / time, nil
}
