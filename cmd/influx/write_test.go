package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

const logPrefix = "::PREFIX::"

func overrideLogging() (func(), *bytes.Buffer) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	oldFlags := log.Flags()
	log.SetFlags(0)
	oldPrefix := log.Prefix()
	log.SetPrefix(logPrefix)
	return func() {
		log.SetOutput(os.Stderr)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	}, &buf
}

var tempFiles []string

func removeTempFiles() {
	for _, tempFile := range tempFiles {
		os.Remove(tempFile)
	}
	tempFiles = tempFiles[:0]
}

func readLines(reader io.Reader) []string {
	scanner := bufio.NewScanner(reader)
	retVal := make([]string, 0, 3)
	for scanner.Scan() {
		retVal = append(retVal, scanner.Text())
	}
	return retVal
}

func createTempFile(t *testing.T, suffix string, contents []byte, compress bool) string {
	t.Helper()

	file, err := ioutil.TempFile("", "influx_writeTest*."+suffix)
	require.NoError(t, err)
	defer file.Close()

	fileName := file.Name()
	tempFiles = append(tempFiles, fileName)

	var writer io.Writer = file
	if compress {
		gzipWriter := gzip.NewWriter(writer)
		defer gzipWriter.Close()
		writer = gzipWriter
	}

	_, err = writer.Write(contents)
	require.NoError(t, err)
	return fileName
}

// Test_writeFlags_dump test that --debug flag will dump debugging info
func Test_writeFlags_dump(t *testing.T) {
	restoreLogging, log := overrideLogging()
	defer restoreLogging()
	flags := writeFlagsBuilder{}
	flags.dump([]string{})
	// no dump without --debug
	require.Empty(t, log.String())

	flags.Debug = true
	flags.dump([]string{})
	messages := strings.Count(log.String(), logPrefix)
	// dump with --debug
	require.Greater(t, messages, 0)
}

// Test_writeFlags_createLineReader validates the way of how headers, files, stdin and arguments
// are combined and transformed to provide a reader of protocol lines
func Test_writeFlags_createLineReader(t *testing.T) {
	defer removeTempFiles()

	gzipStdin := func(uncompressed string) io.Reader {
		contents := &bytes.Buffer{}
		writer := gzip.NewWriter(contents)
		_, err := writer.Write([]byte(uncompressed))
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		return contents
	}

	lpContents := "f1 b=f2,c=f3,d=f4"
	lpFile := createTempFile(t, "txt", []byte(lpContents), false)
	gzipLpFile := createTempFile(t, "txt.gz", []byte(lpContents), true)
	gzipLpFileNoExt := createTempFile(t, "lp", []byte(lpContents), true)
	stdInLpContents := "stdin3 i=stdin1,j=stdin2,k=stdin4"

	csvContents := "_measurement,b,c,d\nf1,f2,f3,f4"
	csvFile := createTempFile(t, "csv", []byte(csvContents), false)
	gzipCsvFile := createTempFile(t, "csv.gz", []byte(csvContents), true)
	gzipCsvFileNoExt := createTempFile(t, "csv", []byte(csvContents), true)
	stdInCsvContents := "i,j,_measurement,k\nstdin1,stdin2,stdin3,stdin4"

	// use a test HTTP server to provide CSV data
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// fmt.Println(req.URL.String())
		query := req.URL.Query()
		if contentType := query.Get("Content-Type"); contentType != "" {
			rw.Header().Set("Content-Type", contentType)
		}
		if encoding := query.Get("encoding"); encoding != "" {
			rw.Header().Set("Content-Encoding", encoding)
		}
		compress := query.Get("compress") != ""
		rw.WriteHeader(http.StatusOK)
		if data := query.Get("data"); data != "" {
			var writer io.Writer = rw
			if compress {
				gzw := gzip.NewWriter(writer)
				defer gzw.Close()
				writer = gzw
			}
			_, err := writer.Write([]byte(data))
			require.NoError(t, err)
		}
	}))
	defer server.Close()

	var tests = []struct {
		name string
		// input
		flags     writeFlagsBuilder
		stdIn     io.Reader
		arguments []string
		// output
		firstLineCorrection int // 0 unless shifted by prepended headers or skipped rows
		lines               []string
	}{
		{
			name: "read data from LP file",
			flags: writeFlagsBuilder{
				Files: []string{lpFile},
			},
			firstLineCorrection: 0,
			lines: []string{
				lpContents,
			},
		},
		{
			name: "read data from LP file using non-UTF encoding",
			flags: writeFlagsBuilder{
				Files:    []string{lpFile},
				Encoding: "ISO_8859-1",
			},
			firstLineCorrection: 0,
			lines: []string{
				lpContents,
			},
		},
		{
			name: "read compressed LP data from file",
			flags: writeFlagsBuilder{
				Files:       []string{gzipLpFileNoExt},
				Compression: inputCompressionGzip,
			},
			firstLineCorrection: 0,
			lines: []string{
				lpContents,
			},
		},
		{
			name: "read compressed data from LP file using non-UTF encoding",
			flags: writeFlagsBuilder{
				Files:       []string{gzipLpFileNoExt},
				Compression: inputCompressionGzip,
				Encoding:    "ISO_8859-1",
			},
			firstLineCorrection: 0,
			lines: []string{
				lpContents,
			},
		},
		{
			name: "read compressed LP data from file ending in .gz",
			flags: writeFlagsBuilder{
				Files: []string{gzipLpFile},
			},
			firstLineCorrection: 0,
			lines: []string{
				lpContents,
			},
		},
		{
			name: "read compressed and uncompressed LP data from file in the same call",
			flags: writeFlagsBuilder{
				Files: []string{gzipLpFile, lpFile},
			},
			firstLineCorrection: 0,
			lines: []string{
				lpContents,
				lpContents,
			},
		},
		{
			name:  "read LP data from stdin",
			flags: writeFlagsBuilder{},
			stdIn: strings.NewReader(stdInLpContents),
			lines: []string{
				stdInLpContents,
			},
		},
		{
			name: "read compressed LP data from stdin",
			flags: writeFlagsBuilder{
				Compression: inputCompressionGzip,
			},
			stdIn: gzipStdin(stdInLpContents),
			lines: []string{
				stdInLpContents,
			},
		},
		{
			name:      "read LP data from stdin using '-' argument",
			flags:     writeFlagsBuilder{},
			stdIn:     strings.NewReader(stdInLpContents),
			arguments: []string{"-"},
			lines: []string{
				stdInLpContents,
			},
		},
		{
			name: "read compressed LP data from stdin using '-' argument",
			flags: writeFlagsBuilder{
				Compression: inputCompressionGzip,
			},
			stdIn:     gzipStdin(stdInLpContents),
			arguments: []string{"-"},
			lines: []string{
				stdInLpContents,
			},
		},
		{
			name:      "read LP data from 1st argument",
			flags:     writeFlagsBuilder{},
			arguments: []string{stdInLpContents},
			lines: []string{
				stdInLpContents,
			},
		},
		{
			name: "read LP data from URL",
			flags: writeFlagsBuilder{
				URLs: []string{fmt.Sprintf("%s/a?data=%s", server.URL, url.QueryEscape(lpContents))},
			},
			lines: []string{
				lpContents,
			},
		},
		{
			name: "read compressed LP data from URL",
			flags: writeFlagsBuilder{
				URLs:        []string{fmt.Sprintf("%s/a?data=%s&compress=true", server.URL, url.QueryEscape(lpContents))},
				Compression: inputCompressionGzip,
			},
			lines: []string{
				lpContents,
			},
		},
		{
			name: "read compressed LP data from URL ending in .gz",
			flags: writeFlagsBuilder{
				URLs: []string{fmt.Sprintf("%s/a.gz?data=%s&compress=true", server.URL, url.QueryEscape(lpContents))},
			},
			lines: []string{
				lpContents,
			},
		},
		{
			name: "read compressed LP data from URL with gzip encoding",
			flags: writeFlagsBuilder{
				URLs: []string{fmt.Sprintf("%s/a?data=%s&compress=true&encoding=gzip", server.URL, url.QueryEscape(lpContents))},
			},
			lines: []string{
				lpContents,
			},
		},
		{
			name: "read data from CSV file + transform to line protocol",
			flags: writeFlagsBuilder{
				Files: []string{csvFile},
			},
			firstLineCorrection: 0, // no changes
			lines: []string{
				lpContents,
			},
		},
		{
			name: "read compressed CSV data from file + transform to line protocol",
			flags: writeFlagsBuilder{
				Files:       []string{gzipCsvFileNoExt},
				Compression: inputCompressionGzip,
			},
			firstLineCorrection: 0,
			lines: []string{
				lpContents,
			},
		},
		{
			name: "read compressed CSV data from file ending in .csv.gz + transform to line protocol",
			flags: writeFlagsBuilder{
				Files: []string{gzipCsvFile},
			},
			firstLineCorrection: 0,
			lines: []string{
				lpContents,
			},
		},
		{
			name: "read CSV data from --header and --file + transform to line protocol",
			flags: writeFlagsBuilder{
				Headers: []string{"x,_measurement,y,z"},
				Files:   []string{csvFile},
			},
			firstLineCorrection: -1, // shifted back by header line
			lines: []string{
				"b x=_measurement,y=c,z=d",
				"f2 x=f1,y=f3,z=f4",
			},
		},
		{
			name: "read CSV data from --header and @file argument with 1st row in file skipped + transform to line protocol",
			flags: writeFlagsBuilder{
				Headers:    []string{"x,_measurement,y,z"},
				SkipHeader: 1,
			},
			arguments:           []string{"@" + csvFile},
			firstLineCorrection: 0, // shifted (-1) back by header line, forward (+1) by skipHeader
			lines: []string{
				"f2 x=f1,y=f3,z=f4",
			},
		},
		{
			name: "read CSV data from --header and @file argument with 1st row in file skipped + transform to line protocol",
			flags: writeFlagsBuilder{
				Headers:    []string{"x,_measurement,y,z"},
				SkipHeader: 1,
			},
			arguments:           []string{"@" + csvFile},
			firstLineCorrection: 0, // shifted (-1) back by header line, forward (+1) by skipHeader
			lines: []string{
				"f2 x=f1,y=f3,z=f4",
			},
		},
		{
			name: "read CSV data from stdin + transform to line protocol",
			flags: writeFlagsBuilder{
				Format: inputFormatCsv,
			},
			stdIn: strings.NewReader(stdInCsvContents),
			lines: []string{
				stdInLpContents,
			},
		},
		{
			name: "read compressed CSV data from stdin + transform to line protocol",
			flags: writeFlagsBuilder{
				Format:      inputFormatCsv,
				Compression: inputCompressionGzip,
			},
			stdIn: gzipStdin(stdInCsvContents),
			lines: []string{
				stdInLpContents,
			},
		},
		{
			name: "read CSV data from stdin using '-' argument + transform to line protocol",
			flags: writeFlagsBuilder{
				Format: inputFormatCsv,
			},
			stdIn:     strings.NewReader(stdInCsvContents),
			arguments: []string{"-"},
			lines: []string{
				stdInLpContents,
			},
		},
		{
			name: "read compressed CSV data from stdin using '-' argument + transform to line protocol",
			flags: writeFlagsBuilder{
				Format:      inputFormatCsv,
				Compression: inputCompressionGzip,
			},
			stdIn:     gzipStdin(stdInCsvContents),
			arguments: []string{"-"},
			lines: []string{
				stdInLpContents,
			},
		},
		{
			name: "read CSV data from 1st argument + transform to line protocol",
			flags: writeFlagsBuilder{
				Format: inputFormatCsv,
			},
			arguments: []string{stdInCsvContents},
			lines: []string{
				stdInLpContents,
			},
		},
		{
			name: "read data from .csv URL + transform to line protocol",
			flags: writeFlagsBuilder{
				URLs: []string{fmt.Sprintf("%s/a.csv?data=%s", server.URL, url.QueryEscape(csvContents))},
			},
			lines: []string{
				lpContents,
			},
		},
		{
			name: "read compressed CSV data from URL + transform to line protocol",
			flags: writeFlagsBuilder{
				URLs:        []string{fmt.Sprintf("%s/a.csv?data=%s&compress=true", server.URL, url.QueryEscape(csvContents))},
				Compression: inputCompressionGzip,
			},
			lines: []string{
				lpContents,
			},
		},
		{
			name: "read compressed CSV data from URL ending in .csv.gz + transform to line protocol",
			flags: writeFlagsBuilder{
				URLs: []string{fmt.Sprintf("%s/a.csv.gz?data=%s&compress=true", server.URL, url.QueryEscape(csvContents))},
			},
			lines: []string{
				lpContents,
			},
		},
		{
			name: "read compressed CSV data from URL with gzip encoding + transform to line protocol",
			flags: writeFlagsBuilder{
				URLs: []string{fmt.Sprintf("%s/a.csv?data=%s&compress=true&encoding=gzip", server.URL, url.QueryEscape(csvContents))},
			},
			lines: []string{
				lpContents,
			},
		},
		{
			name: "read data from .csv URL + change header line + transform to line protocol",
			flags: writeFlagsBuilder{
				URLs:       []string{fmt.Sprintf("%s/a.csv?data=%s", server.URL, url.QueryEscape(csvContents))},
				Headers:    []string{"k,j,_measurement,i"},
				SkipHeader: 1,
			},
			lines: []string{
				"f3 k=f1,j=f2,i=f4",
			},
		},
		{
			name: "read data from URL with text/csv Content-Type + transform to line protocol",
			flags: writeFlagsBuilder{
				URLs: []string{fmt.Sprintf("%s/a?Content-Type=text/csv&data=%s", server.URL, url.QueryEscape(csvContents))},
			},
			lines: []string{
				lpContents,
			},
		},
		{
			name: "read compressed data from URL with text/csv Content-Type and gzip Content-Encoding + transform to line protocol",
			flags: writeFlagsBuilder{
				URLs: []string{fmt.Sprintf("%s/a?Content-Type=text/csv&data=%s&compress=true&encoding=gzip", server.URL, url.QueryEscape(csvContents))},
			},
			lines: []string{
				lpContents,
			},
		},
		{
			name: "read data from CSV file + transform to line protocol + throttle read to 1MB/min",
			flags: writeFlagsBuilder{
				Files:     []string{csvFile},
				RateLimit: "1MBs",
			},
			lines: []string{
				lpContents,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := cmdWrite(&globalFlags{}, genericCLIOpts{in: test.stdIn, viper: viper.New()})
			reader, closer, err := test.flags.createLineReader(context.Background(), command, test.arguments)
			require.NotNil(t, closer)
			defer closer.Close()
			require.Nil(t, err)
			require.NotNil(t, reader)
			lines := readLines(reader)
			require.Equal(t, test.lines, lines)
		})
	}
}

// Test_writeFlags_createLineReader_errors tests input validation
func Test_writeFlags_createLineReader_errors(t *testing.T) {
	defer removeTempFiles()
	csvFile1 := createTempFile(t, "csv", []byte("_measurement,b,c,d\nf1,f2,f3,f4"), false)
	// use a test HTTP server to server errors
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	var tests = []struct {
		name string
		// input
		flags writeFlagsBuilder
		// output
		message string
	}{
		{
			name: "unsupported format",
			flags: writeFlagsBuilder{
				Format: "wiki",
			},
			message: "unsupported",
		},
		{
			name: "unsupported encoding",
			flags: writeFlagsBuilder{
				Encoding: "green",
			},
			message: "https://www.iana.org/assignments/character-sets/character-sets.xhtml", // hint to available values
		},
		{
			name: "file not found",
			flags: writeFlagsBuilder{
				Files: []string{csvFile1 + "x"},
			},
			message: csvFile1,
		},
		{
			name: "unsupported URL",
			flags: writeFlagsBuilder{
				URLs: []string{"wit://whatever"},
			},
			message: "wit://whatever",
		},
		{
			name: "invalid URL",
			flags: writeFlagsBuilder{
				URLs: []string{"http://test%zy"}, // 2 hex digits after % expected
			},
			message: "http://test%zy",
		},
		{
			name: "URL with 500 status code",
			flags: writeFlagsBuilder{
				URLs: []string{server.URL},
			},
			message: server.URL,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := cmdWrite(&globalFlags{}, genericCLIOpts{in: strings.NewReader(""), viper: viper.New()})
			_, closer, err := test.flags.createLineReader(context.Background(), command, []string{})
			require.NotNil(t, closer)
			defer closer.Close()
			require.NotNil(t, err)
			require.Contains(t, fmt.Sprintf("%s", err), test.message)
		})
	}
}

func Test_writeDryrunE(t *testing.T) {
	t.Run("process and transform csv data without problems to stdout", func(t *testing.T) {
		stdInContents := "i,j,_measurement,k\nstdin1,stdin2,stdin3,stdin4"
		out := bytes.Buffer{}
		command := cmdWrite(&globalFlags{}, genericCLIOpts{in: strings.NewReader(stdInContents), w: bufio.NewWriter(&out), viper: viper.New()})
		command.SetArgs([]string{"dryrun", "--format", "csv"})
		err := command.Execute()
		require.Nil(t, err)
		require.Equal(t, "stdin3 i=stdin1,j=stdin2,k=stdin4", strings.Trim(out.String(), "\n"))
	})

	t.Run("dryrun fails on unsupported data format", func(t *testing.T) {
		stdInContents := "i,j,_measurement,k\nstdin1,stdin2,stdin3,stdin4"
		out := bytes.Buffer{}
		command := cmdWrite(&globalFlags{}, genericCLIOpts{in: strings.NewReader(stdInContents), w: bufio.NewWriter(&out), viper: viper.New()})
		command.SetArgs([]string{"dryrun", "--format", "csvx"})
		err := command.Execute()
		require.NotNil(t, err)
		require.Contains(t, fmt.Sprintf("%s", err), "unsupported") // unsupported format
	})

	t.Run("dryrun fails on malformed CSV data while reading them", func(t *testing.T) {
		stdInContents := "i,j,l,k\nstdin1,stdin2,stdin3,stdin4"
		out := bytes.Buffer{}
		command := cmdWrite(&globalFlags{}, genericCLIOpts{in: strings.NewReader(stdInContents), w: bufio.NewWriter(&out), viper: viper.New()})
		command.SetArgs([]string{"dryrun", "--format", "csv"})
		err := command.Execute()
		require.NotNil(t, err)
		require.Contains(t, fmt.Sprintf("%s", err), "measurement") // no measurement column
	})
}

func Test_writeRunE(t *testing.T) {
	t.Run("validates that --org or --org-id must be specified", func(t *testing.T) {
		command := cmdWrite(&globalFlags{}, genericCLIOpts{w: ioutil.Discard, viper: viper.New()})
		command.SetArgs([]string{"--format", "csv"})
		err := command.Execute()
		require.Contains(t, fmt.Sprintf("%s", err), "org")
	})

	t.Run("validates that either --bucket or --bucket-id must be specified", func(t *testing.T) {
		command := cmdWrite(&globalFlags{}, genericCLIOpts{w: ioutil.Discard, viper: viper.New()})
		command.SetArgs([]string{"--format", "csv", "--org", "my-org", "--bucket", "my-bucket", "--bucket-id", "my-bucket"})
		err := command.Execute()
		require.Contains(t, fmt.Sprintf("%s", err), "bucket") // bucket or bucket-id, but not both
	})

	t.Run("validates --precision", func(t *testing.T) {
		command := cmdWrite(&globalFlags{}, genericCLIOpts{w: ioutil.Discard, viper: viper.New()})
		command.SetArgs([]string{"--format", "csv", "--org", "my-org", "--bucket", "my-bucket", "--precision", "pikosec"})
		err := command.Execute()
		require.Contains(t, fmt.Sprintf("%s", err), "precision") // invalid precision
	})

	t.Run("validates decoding of bucket-id", func(t *testing.T) {
		command := cmdWrite(&globalFlags{}, genericCLIOpts{w: ioutil.Discard, viper: viper.New()})
		command.SetArgs([]string{"--format", "csv", "--org", "my-org", "--bucket-id", "my-bucket"})
		err := command.Execute()
		require.Contains(t, fmt.Sprintf("%s", err), "bucket-id")
	})

	t.Run("validates decoding of org-id", func(t *testing.T) {
		command := cmdWrite(&globalFlags{}, genericCLIOpts{w: ioutil.Discard, viper: viper.New()})
		command.SetArgs([]string{"--format", "csv", "--org-id", "my-org", "--bucket", "my-bucket"})
		err := command.Execute()
		require.Contains(t, fmt.Sprintf("%s", err), "org-id")
	})

	t.Run("validates unsupported line reader format", func(t *testing.T) {
		command := cmdWrite(&globalFlags{}, genericCLIOpts{w: ioutil.Discard, viper: viper.New()})
		command.SetArgs([]string{"--format", "csvx", "--org", "my-org", "--bucket-id", "4f14589c26df8286"})
		err := command.Execute()
		require.Contains(t, fmt.Sprintf("%s", err), "format")
	})

	t.Run("validates error during data read", func(t *testing.T) {
		command := cmdWrite(&globalFlags{}, genericCLIOpts{
			in:    strings.NewReader("a,b\nc,d"),
			w:     ioutil.Discard,
			viper: viper.New()})
		command.SetArgs([]string{"--format", "csv", "--org", "my-org", "--bucket-id", "4f14589c26df8286"})
		err := command.Execute()
		require.Contains(t, fmt.Sprintf("%s", err), "measurement") // no measurement found in CSV data
	})

	t.Run("read and send LP", func(t *testing.T) {
		lineData := bytes.Buffer{}
		writeSvc := &mock.WriteService{
			WriteToF: func(_ context.Context, _ influxdb.BucketFilter, reader io.Reader) error {
				if _, err := lineData.ReadFrom(reader); err != nil {
					return err
				}
				return nil
			},
		}
		svcBuilder := func(*writeFlagsBuilder) influxdb.WriteService { return writeSvc }

		// read data from CSV transformation, send them to server and validate the created protocol line
		cliOpts := genericCLIOpts{
			in:    strings.NewReader("stdin3 i=stdin1,j=stdin2,k=stdin4"),
			w:     ioutil.Discard,
			viper: viper.New(),
		}
		command := newWriteFlagsBuilder(svcBuilder, &globalFlags{}, cliOpts).cmd()
		command.SetArgs([]string{"--org", "my-org", "--bucket-id", "4f14589c26df8286"})
		err := command.Execute()
		require.NoError(t, err)
		require.Equal(t, "stdin3 i=stdin1,j=stdin2,k=stdin4", strings.Trim(lineData.String(), "\n"))
	})

	t.Run("report server-side error", func(t *testing.T) {
		writeSvc := &mock.WriteService{
			WriteToF: func(_ context.Context, _ influxdb.BucketFilter, _ io.Reader) error {
				return errors.New("i broke")
			},
		}
		svcBuilder := func(*writeFlagsBuilder) influxdb.WriteService { return writeSvc }

		// read data from CSV transformation, send them to server and validate the created protocol line
		cliOpts := genericCLIOpts{
			in:    strings.NewReader("stdin3 i=stdin1,j=stdin2,k=stdin4"),
			w:     ioutil.Discard,
			viper: viper.New(),
		}
		command := newWriteFlagsBuilder(svcBuilder, &globalFlags{}, cliOpts).cmd()
		command.SetArgs([]string{"--org", "my-org", "--bucket-id", "4f14589c26df8286"})
		err := command.Execute()
		require.Error(t, err)
		require.Contains(t, err.Error(), "i broke")
	})

	t.Run("read data from CSV and send lp", func(t *testing.T) {
		lineData := bytes.Buffer{}
		writeSvc := &mock.WriteService{
			WriteToF: func(_ context.Context, _ influxdb.BucketFilter, reader io.Reader) error {
				if _, err := lineData.ReadFrom(reader); err != nil {
					return err
				}
				return nil
			},
		}
		svcBuilder := func(*writeFlagsBuilder) influxdb.WriteService { return writeSvc }

		// read data from CSV transformation, send them to server and validate the created protocol line
		cliOpts := genericCLIOpts{
			in:    strings.NewReader("i,j,_measurement,k\nstdin1,stdin2,stdin3,stdin4"),
			w:     ioutil.Discard,
			viper: viper.New(),
		}
		command := newWriteFlagsBuilder(svcBuilder, &globalFlags{}, cliOpts).cmd()
		command.SetArgs([]string{"--format", "csv", "--org", "my-org", "--bucket-id", "4f14589c26df8286"})
		err := command.Execute()
		require.NoError(t, err)
		require.Equal(t, "stdin3 i=stdin1,j=stdin2,k=stdin4", strings.Trim(lineData.String(), "\n"))
	})
}

// Test_writeFlags_errorsFile tests that rejected rows are written to errors file
func Test_writeFlags_errorsFile(t *testing.T) {
	defer removeTempFiles()
	errorsFile := createTempFile(t, "errors", []byte{}, false)
	stdInContents := "_measurement,a|long:strict\nm,1\nm,1.1"
	out := bytes.Buffer{}
	command := cmdWrite(&globalFlags{}, genericCLIOpts{in: strings.NewReader(stdInContents), w: bufio.NewWriter(&out), viper: viper.New()})
	command.SetArgs([]string{"dryrun", "--format", "csv", "--errors-file", errorsFile})
	err := command.Execute()
	require.Nil(t, err)
	require.Equal(t, "m a=1i", strings.Trim(out.String(), "\n"))
	errorLines, err := ioutil.ReadFile(errorsFile)
	require.Nil(t, err)
	require.Equal(t, "# error : line 3: column 'a': '1.1' cannot fit into long data type\nm,1.1", strings.Trim(string(errorLines), "\n"))
}

func Test_ToBytesPerSecond(t *testing.T) {
	var tests = []struct {
		in    string
		out   float64
		error string
	}{
		{
			in:  "5 MB / 5 min",
			out: float64(5*1024*1024) / float64(5*60),
		},
		{
			in:  "17kBs",
			out: float64(17 * 1024),
		},
		{
			in:  "1B/m",
			out: float64(1) / float64(60),
		},
		{
			in:  "1B/2sec",
			out: float64(1) / float64(2),
		},
		{
			in:  "",
			out: 0,
		},
		{
			in:    "1B/munite",
			error: `invalid rate limit "1B/munite": it does not match format COUNT(B|kB|MB)/TIME(s|sec|m|min) with / and TIME being optional`,
		},
		{
			in:    ".B/s",
			error: `invalid rate limit ".B/s": '.' is not count of bytes:`,
		},
		{
			in:    "1B0s",
			error: `invalid rate limit "1B0s": positive time expected but 0 supplied`,
		},
		{
			in:    "1MB/42949672950s",
			error: `invalid rate limit "1MB/42949672950s": time is out of range`,
		},
	}
	for _, test := range tests {
		t.Run(test.in, func(t *testing.T) {
			bytesPerSec, err := ToBytesPerSecond(test.in)
			if len(test.error) == 0 {
				require.Equal(t, test.out, bytesPerSec)
				require.Nil(t, err)
			} else {
				require.NotNil(t, err)
				// contains is used, since the error messages contains root cause that may evolve with go versions
				require.Contains(t, fmt.Sprintf("%s", err), test.error)
			}
		})
	}
}
