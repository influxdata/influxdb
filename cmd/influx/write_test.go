package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
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

	"github.com/influxdata/influxdb/v2/pkg/csv2lp"
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

func createTempFile(suffix string, contents []byte) string {
	file, err := ioutil.TempFile("", "influx_writeTest*."+suffix)
	if err != nil {
		log.Fatal(err)
		return "unknown.file"
	}
	fileName := file.Name()
	tempFiles = append(tempFiles, fileName)
	err = ioutil.WriteFile(fileName, contents, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	return fileName
}

// Test_writeFlags_dump test that --debug flag will dump debugging info
func Test_writeFlags_dump(t *testing.T) {
	restoreLogging, log := overrideLogging()
	defer restoreLogging()
	flags := writeFlagsType{}
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
	fileContents := "_measurement,b,c,d\nf1,f2,f3,f4"
	csvFile1 := createTempFile("csv", []byte(fileContents))
	stdInContents := "i,j,_measurement,k\nstdin1,stdin2,stdin3,stdin4"

	// use a test HTTP server to provide CSV data
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// fmt.Println(req.URL.String())
		query := req.URL.Query()
		if contentType := query.Get("Content-Type"); contentType != "" {
			rw.Header().Set("Content-Type", contentType)
		}
		rw.WriteHeader(http.StatusOK)
		if data := query.Get("data"); data != "" {
			rw.Write([]byte(data))
		}
	}))
	defer server.Close()

	var tests = []struct {
		name string
		// input
		flags     writeFlagsType
		stdIn     io.Reader
		arguments []string
		// output
		firstLineCorrection int // 0 unless shifted by prepended headers or skipped rows
		lines               []string
		// lpData indicates the the data are line protocol data
		lpData bool
	}{
		{
			name: "read data from CSV file + transform to line protocol",
			flags: writeFlagsType{
				Files: []string{csvFile1},
			},
			firstLineCorrection: 0, // no changes
			lines: []string{
				"f1 b=f2,c=f3,d=f4",
			},
		},
		{
			name: "read CSV data from --header and --file + transform to line protocol",
			flags: writeFlagsType{
				Headers: []string{"x,_measurement,y,z"},
				Files:   []string{csvFile1},
			},
			firstLineCorrection: -1, // shifted back by header line
			lines: []string{
				"b x=_measurement,y=c,z=d",
				"f2 x=f1,y=f3,z=f4",
			},
		},
		{
			name: "read CSV data from --header and @file argument with 1st row in file skipped + transform to line protocol",
			flags: writeFlagsType{
				Headers:    []string{"x,_measurement,y,z"},
				SkipHeader: 1,
			},
			arguments:           []string{"@" + csvFile1},
			firstLineCorrection: 0, // shifted (-1) back by header line, forward (+1) by skipHeader
			lines: []string{
				"f2 x=f1,y=f3,z=f4",
			},
		},
		{
			name: "read CSV data from --header and @file argument with 1st row in file skipped + transform to line protocol",
			flags: writeFlagsType{
				Headers:    []string{"x,_measurement,y,z"},
				SkipHeader: 1,
			},
			arguments:           []string{"@" + csvFile1},
			firstLineCorrection: 0, // shifted (-1) back by header line, forward (+1) by skipHeader
			lines: []string{
				"f2 x=f1,y=f3,z=f4",
			},
		},
		{
			name: "read CSV data from stdin + transform to line protocol",
			flags: writeFlagsType{
				Format: inputFormatCsv,
			},
			stdIn: strings.NewReader(stdInContents),
			lines: []string{
				"stdin3 i=stdin1,j=stdin2,k=stdin4",
			},
		},
		{
			name: "read CSV data from stdin using '-' argument + transform to line protocol",
			flags: writeFlagsType{
				Format: inputFormatCsv,
			},
			stdIn:     strings.NewReader(stdInContents),
			arguments: []string{"-"},
			lines: []string{
				"stdin3 i=stdin1,j=stdin2,k=stdin4",
			},
		},
		{
			name: "read CSV data from 1st argument + transform to line protocol",
			flags: writeFlagsType{
				Format: inputFormatCsv,
			},
			arguments: []string{stdInContents},
			lines: []string{
				"stdin3 i=stdin1,j=stdin2,k=stdin4",
			},
		},
		{
			name: "read data from .csv URL + transform to line protocol",
			flags: writeFlagsType{
				URLs: []string{(server.URL + "/a.csv?data=" + url.QueryEscape(fileContents))},
			},
			lines: []string{
				"f1 b=f2,c=f3,d=f4",
			},
		},
		{
			name: "read data from .csv URL + change header line + transform to line protocol",
			flags: writeFlagsType{
				URLs:       []string{(server.URL + "/a.csv?data=" + url.QueryEscape(fileContents))},
				Headers:    []string{"k,j,_measurement,i"},
				SkipHeader: 1,
			},
			lines: []string{
				"f3 k=f1,j=f2,i=f4",
			},
		},
		{
			name: "read data from having text/csv URL resource + transform to line protocol",
			flags: writeFlagsType{
				URLs: []string{(server.URL + "/a?Content-Type=text/csv&data=" + url.QueryEscape(fileContents))},
			},
			lines: []string{
				"f1 b=f2,c=f3,d=f4",
			},
		},
		{
			name: "read line protocol data from URL",
			flags: writeFlagsType{
				URLs: []string{(server.URL + "/a?data=" + url.QueryEscape(fileContents))},
			},
			lines:  strings.Split(fileContents, "\n"),
			lpData: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := cmdWrite(&globalFlags{}, genericCLIOpts{in: test.stdIn})
			reader, closer, err := test.flags.createLineReader(context.Background(), command, test.arguments)
			require.NotNil(t, closer)
			defer closer.Close()
			require.Nil(t, err)
			require.NotNil(t, reader)
			if !test.lpData {
				csvToLineReader, ok := reader.(*csv2lp.CsvToLineReader)
				require.True(t, ok)
				require.Equal(t, csvToLineReader.LineNumber, test.firstLineCorrection)
			}
			lines := readLines(reader)
			require.Equal(t, test.lines, lines)
		})
	}
}

// Test_writeFlags_createLineReader_errors tests input validation
func Test_writeFlags_createLineReader_errors(t *testing.T) {
	defer removeTempFiles()
	csvFile1 := createTempFile("csv", []byte("_measurement,b,c,d\nf1,f2,f3,f4"))
	// use a test HTTP server to server errors
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	var tests = []struct {
		name string
		// input
		flags writeFlagsType
		// output
		message string
	}{
		{
			name: "unsupported format",
			flags: writeFlagsType{
				Format: "wiki",
			},
			message: "unsupported",
		},
		{
			name: "unsupported encoding",
			flags: writeFlagsType{
				Encoding: "green",
			},
			message: "https://www.iana.org/assignments/character-sets/character-sets.xhtml", // hint to available values
		},
		{
			name: "file not found",
			flags: writeFlagsType{
				Files: []string{csvFile1 + "x"},
			},
			message: csvFile1,
		},
		{
			name: "unsupported URL",
			flags: writeFlagsType{
				URLs: []string{"wit://whatever"},
			},
			message: "wit://whatever",
		},
		{
			name: "invalid URL",
			flags: writeFlagsType{
				URLs: []string{"http://test%zy"}, // 2 hex digits after % expected
			},
			message: "http://test%zy",
		},
		{
			name: "URL with 500 status code",
			flags: writeFlagsType{
				URLs: []string{server.URL},
			},
			message: server.URL,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := cmdWrite(&globalFlags{}, genericCLIOpts{in: strings.NewReader("")})
			_, closer, err := test.flags.createLineReader(context.Background(), command, []string{})
			require.NotNil(t, closer)
			defer closer.Close()
			require.NotNil(t, err)
			require.Contains(t, fmt.Sprintf("%s", err), test.message)
		})
	}
}

// Test_fluxWriteDryrunF tests dryrun functionality
func Test_fluxWriteDryrunF(t *testing.T) {
	t.Run("process and transform csv data without problems to stdout", func(t *testing.T) {
		stdInContents := "i,j,_measurement,k\nstdin1,stdin2,stdin3,stdin4"
		out := bytes.Buffer{}
		command := cmdWrite(&globalFlags{}, genericCLIOpts{in: strings.NewReader(stdInContents), w: bufio.NewWriter(&out)})
		command.SetArgs([]string{"dryrun", "--format", "csv"})
		err := command.Execute()
		require.Nil(t, err)
		require.Equal(t, "stdin3 i=stdin1,j=stdin2,k=stdin4", strings.Trim(out.String(), "\n"))
	})

	t.Run("dryrun fails on unsupported data format", func(t *testing.T) {
		stdInContents := "i,j,_measurement,k\nstdin1,stdin2,stdin3,stdin4"
		out := bytes.Buffer{}
		command := cmdWrite(&globalFlags{}, genericCLIOpts{in: strings.NewReader(stdInContents), w: bufio.NewWriter(&out)})
		command.SetArgs([]string{"dryrun", "--format", "csvx"})
		err := command.Execute()
		require.NotNil(t, err)
		require.Contains(t, fmt.Sprintf("%s", err), "unsupported") // unsupported format
	})

	t.Run("dryrun fails on malformed CSV data while reading them", func(t *testing.T) {
		stdInContents := "i,j,l,k\nstdin1,stdin2,stdin3,stdin4"
		out := bytes.Buffer{}
		command := cmdWrite(&globalFlags{}, genericCLIOpts{in: strings.NewReader(stdInContents), w: bufio.NewWriter(&out)})
		command.SetArgs([]string{"dryrun", "--format", "csv"})
		err := command.Execute()
		require.NotNil(t, err)
		require.Contains(t, fmt.Sprintf("%s", err), "measurement") // no measurement column
	})
}

// Test_fluxWriteF tests validation and processing of input flags in fluxWriteF
func Test_fluxWriteF(t *testing.T) {
	var lineData []byte // stores line data that the client writes
	// use a test HTTP server to mock response
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		url := req.URL.String()
		// fmt.Println(url)
		switch {
		case strings.Contains(url, "error"): // fail when error is in ULR
			rw.WriteHeader(http.StatusInternalServerError)
			rw.Write([]byte(`ERROR`))
			return
		case strings.Contains(url, "empty"): // return empty buckets response
			rw.WriteHeader(http.StatusOK)
			rw.Write([]byte(`{
				"links":{
					"self":"/api/v2/buckets?descending=false\u0026limit=20\u0026offset=0\u0026orgID=b112ec3528efa3b4"
				},
				"buckets":[]
			}`))
			return
		case strings.HasPrefix(url, "/api/v2/buckets"): // return example bucket
			rw.WriteHeader(http.StatusOK)
			rw.Write([]byte(`{
				"links":{
					"self":"/api/v2/buckets?descending=false\u0026limit=20\u0026offset=0\u0026orgID=b112ec3528efa3b4"
				},
				"buckets":[
					{"id":"4f14589c26df8286","orgID":"b112ec3528efa3b4","type":"user","name":"my-bucket","retentionRules":[],
						"createdAt":"2020-04-04T11:43:37.762325688Z","updatedAt":"2020-04-04T11:43:37.762325786Z",
						"links":{
							"labels":"/api/v2/buckets/4f14589c26df8286/labels",
							"logs":"/api/v2/buckets/4f14589c26df8286/logs",
							"members":"/api/v2/buckets/4f14589c26df8286/members",
							"org":"/api/v2/orgs/b112ec3528efa3b4",
							"owners":"/api/v2/buckets/4f14589c26df8286/owners",
							"self":"/api/v2/buckets/4f14589c26df8286",
							"write":"/api/v2/write?org=b112ec3528efa3b4\u0026bucket=4f14589c26df8286"
						},"labels":[]
					}
				]
			}`))
			return
		}
		// consume and remember request contents
		var requestData io.Reader = req.Body
		if h := req.Header["Content-Encoding"]; len(h) > 0 && strings.Contains(h[0], "gzip") {
			gzipReader, err := gzip.NewReader(req.Body)
			if err != nil {
				log.Fatal("Unable to create gzip reader", err)
				return
			}
			requestData = gzipReader
		}
		lineData, _ = ioutil.ReadAll(requestData)
		rw.Write([]byte(`OK`))
	}))
	defer server.Close()
	// setup flags to point to test server
	prevHost := flags.Host
	prevToken := flags.Token
	defer func() {
		flags.Host = prevHost
		flags.Token = prevToken
	}()
	useTestServer := func() {
		lineData = lineData[:0]
		flags.Token = "myToken"
		flags.Host = server.URL
	}

	t.Run("validates that --org or --org-id must be specified", func(t *testing.T) {
		t.Skip(`this test is hard coded to global variables and one small tweak causes a lot of downstream test failures changes else, skipping for now`)
		useTestServer()
		command := cmdWrite(&globalFlags{}, genericCLIOpts{w: ioutil.Discard})
		command.SetArgs([]string{"--format", "csv"})
		err := command.Execute()
		require.Contains(t, fmt.Sprintf("%s", err), "org")
	})

	t.Run("validates that either --bucket or --bucket-id must be specified", func(t *testing.T) {
		useTestServer()
		command := cmdWrite(&globalFlags{}, genericCLIOpts{w: ioutil.Discard})
		command.SetArgs([]string{"--format", "csv", "--org", "my-org", "--bucket", "my-bucket", "--bucket-id", "my-bucket"})
		err := command.Execute()
		require.Contains(t, fmt.Sprintf("%s", err), "bucket") // bucket or bucket-id, but not both
	})

	t.Run("validates --precision", func(t *testing.T) {
		useTestServer()
		command := cmdWrite(&globalFlags{}, genericCLIOpts{w: ioutil.Discard})
		command.SetArgs([]string{"--format", "csv", "--org", "my-org", "--bucket", "my-bucket", "--precision", "pikosec"})
		err := command.Execute()
		require.Contains(t, fmt.Sprintf("%s", err), "precision") // invalid precision
	})

	t.Run("validates --host must be supplied", func(t *testing.T) {
		useTestServer()
		flags.Host = ""
		command := cmdWrite(&flags, genericCLIOpts{w: ioutil.Discard})
		command.SetArgs([]string{"--format", "csv", "--org", "my-org", "--bucket", "my-bucket"})
		err := command.Execute()
		require.Contains(t, fmt.Sprintf("%s", err), "host")
	})

	t.Run("validates decoding of bucket-id", func(t *testing.T) {
		useTestServer()
		command := cmdWrite(&globalFlags{}, genericCLIOpts{w: ioutil.Discard})
		command.SetArgs([]string{"--format", "csv", "--org", "my-org", "--bucket-id", "my-bucket"})
		err := command.Execute()
		require.Contains(t, fmt.Sprintf("%s", err), "bucket-id")
	})

	t.Run("validates decoding of org-id", func(t *testing.T) {
		useTestServer()
		command := cmdWrite(&globalFlags{}, genericCLIOpts{w: ioutil.Discard})
		command.SetArgs([]string{"--format", "csv", "--org-id", "my-org", "--bucket", "my-bucket"})
		err := command.Execute()
		require.Contains(t, fmt.Sprintf("%s", err), "org-id")
	})

	t.Run("validates error when failed to retrive buckets", func(t *testing.T) {
		useTestServer()
		command := cmdWrite(&globalFlags{}, genericCLIOpts{w: ioutil.Discard})
		// note: my-error-bucket parameter causes the test server to fail
		command.SetArgs([]string{"--format", "csv", "--org", "my-org", "--bucket", "my-error-bucket"})
		err := command.Execute()
		require.Contains(t, fmt.Sprintf("%s", err), "bucket")
	})

	// validation: no such bucket found
	t.Run("validates no such bucket found", func(t *testing.T) {
		useTestServer()
		command := cmdWrite(&globalFlags{}, genericCLIOpts{w: ioutil.Discard})
		// note: my-empty-org parameter causes the test server to return no buckets
		command.SetArgs([]string{"--format", "csv", "--org", "my-empty-org", "--bucket", "my-bucket"})
		err := command.Execute()
		require.Contains(t, fmt.Sprintf("%s", err), "bucket")
	})

	// validation: no such bucket-id found
	t.Run("validates no such bucket-id found", func(t *testing.T) {
		useTestServer()
		command := cmdWrite(&globalFlags{}, genericCLIOpts{w: ioutil.Discard})
		// note: my-empty-org parameter causes the test server to return no buckets
		command.SetArgs([]string{"--format", "csv", "--org", "my-empty-org", "--bucket-id", "4f14589c26df8286"})
		err := command.Execute()
		require.Contains(t, fmt.Sprintf("%s", err), "id")
	})

	t.Run("validates unsupported line reader format", func(t *testing.T) {
		useTestServer()
		command := cmdWrite(&globalFlags{}, genericCLIOpts{w: ioutil.Discard})
		command.SetArgs([]string{"--format", "csvx", "--org", "my-org", "--bucket-id", "4f14589c26df8286"})
		err := command.Execute()
		require.Contains(t, fmt.Sprintf("%s", err), "format")
	})

	t.Run("validates error during data read", func(t *testing.T) {
		useTestServer()
		command := cmdWrite(&globalFlags{}, genericCLIOpts{
			in: strings.NewReader("a,b\nc,d"),
			w:  ioutil.Discard})
		command.SetArgs([]string{"--format", "csv", "--org", "my-org", "--bucket-id", "4f14589c26df8286"})
		err := command.Execute()
		require.Contains(t, fmt.Sprintf("%s", err), "measurement") // no measurement found in CSV data
	})

	t.Run("read data from CSV and send lp", func(t *testing.T) {
		// read data from CSV transformation, send them to server and validate the created protocol line
		useTestServer()
		command := cmdWrite(&globalFlags{}, genericCLIOpts{
			in: strings.NewReader("i,j,_measurement,k\nstdin1,stdin2,stdin3,stdin4"),
			w:  ioutil.Discard})
		command.SetArgs([]string{"--format", "csv", "--org", "my-org", "--bucket-id", "4f14589c26df8286"})
		err := command.Execute()
		require.Nil(t, err)
		require.Equal(t, "stdin3 i=stdin1,j=stdin2,k=stdin4", strings.Trim(string(lineData), "\n"))
	})
}
