package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/v2/write"
	"github.com/stretchr/testify/require"
)

var logPrefix string = "::PREFIX::"

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
	for len(tempFiles) > 0 {
		os.Remove(tempFiles[len(tempFiles)-1])
		tempFiles = tempFiles[0 : len(tempFiles)-1]
	}
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

func Test_writeFlags_dump(t *testing.T) {
	restoreLogging, log := overrideLogging()
	defer restoreLogging()
	flags := writeFlagsType{}
	flags.dump([]string{})
	require.Empty(t, log.String())
	flags.Debug = true
	flags.dump([]string{})
	messages := strings.Count(log.String(), logPrefix)

	require.Greater(t, messages, 0)
}

func Test_writeFlags_createLineReader(t *testing.T) {
	defer removeTempFiles()
	csvFile1 := createTempFile("csv", []byte("_measurement,b,c,d\nf1,f2,f3,f4"))
	stdInContents := "i,j,_measurement,k\nstdin1,stdin2,stdin3,stdin4"

	// from csv file with header
	flags := writeFlagsType{}
	flags.Headers = []string{"x,_measurement,y,z"}
	flags.Files = []string{csvFile1}
	command := cmdWrite(&globalFlags{}, genericCLIOpts{in: strings.NewReader(stdInContents)})
	reader, closer, err := flags.createLineReader(command, []string{})
	require.NotNil(t, closer)
	defer closer.Close()
	require.Nil(t, err)
	require.NotNil(t, reader)
	csvToLineReader, ok := reader.(*write.CsvToLineReader)
	require.Equal(t, csvToLineReader.LineNumber, -1) // -1 because of added header
	require.True(t, ok)
	csvReader := csv.NewReader(reader)
	rows, _ := csvReader.ReadAll()
	require.Equal(t, [][]string{
		{"b x=_measurement", "y=c", "z=d"},
		{"f2 x=f1", "y=f3", "z=f4"},
	}, rows)

	// import from csv file as @file + header + skipLines
	flags = writeFlagsType{}
	flags.Headers = []string{"x,_measurement,y,z"}
	flags.Files = []string{}
	flags.SkipHeader = 1
	command = cmdWrite(&globalFlags{}, genericCLIOpts{in: strings.NewReader(stdInContents)})
	reader, closer, err = flags.createLineReader(command, []string{"@" + csvFile1})
	require.NotNil(t, closer)
	defer closer.Close()
	require.Nil(t, err)
	require.NotNil(t, reader)
	csvToLineReader, ok = reader.(*write.CsvToLineReader)
	require.Equal(t, csvToLineReader.LineNumber, 0) // -1 because of added header, +1 because of skipLines
	require.True(t, ok)
	csvReader = csv.NewReader(reader)
	rows, _ = csvReader.ReadAll()
	require.Equal(t, [][]string{
		{"f2 x=f1", "y=f3", "z=f4"},
	}, rows)

	// import from csv file
	flags = writeFlagsType{}
	flags.Headers = []string{}
	flags.Files = []string{csvFile1}
	command = cmdWrite(&globalFlags{}, genericCLIOpts{in: strings.NewReader(stdInContents)})
	reader, closer, err = flags.createLineReader(command, []string{})
	require.NotNil(t, closer)
	defer closer.Close()
	require.Nil(t, err)
	require.NotNil(t, reader)
	csvToLineReader, ok = reader.(*write.CsvToLineReader)
	require.Equal(t, csvToLineReader.LineNumber, 0)
	require.True(t, ok)
	csvReader = csv.NewReader(reader)
	rows, _ = csvReader.ReadAll()
	require.Equal(t, [][]string{
		{"f1 b=f2", "c=f3", "d=f4"},
	}, rows)

	// three forms to read the same data
	for _, args := range [][]string{{}, {"-"}, {stdInContents}} {
		flags = writeFlagsType{}
		flags.Headers = []string{}
		flags.Files = []string{}
		flags.Format = inputFormatCsv
		command = cmdWrite(&globalFlags{}, genericCLIOpts{in: strings.NewReader(stdInContents)})
		reader, closer, err = flags.createLineReader(command, args)
		require.NotNil(t, closer)
		defer closer.Close()
		require.Nil(t, err)
		require.NotNil(t, reader)
		csvToLineReader, ok = reader.(*write.CsvToLineReader)
		require.Equal(t, csvToLineReader.LineNumber, 0)
		require.True(t, ok)
		csvReader = csv.NewReader(reader)
		rows, err = csvReader.ReadAll()
		require.Nil(t, err)
		require.Equal(t, [][]string{
			{"stdin3 i=stdin1", "j=stdin2", "k=stdin4"},
		}, rows)
	}
}

func Test_writeFlags_createLineReader_errors(t *testing.T) {
	defer removeTempFiles()
	csvFile1 := createTempFile("csv", []byte("_measurement,b,c,d\nf1,f2,f3,f4"))

	// fail on unsupported format
	flags := writeFlagsType{}
	flags.Format = "wiki"
	command := cmdWrite(&globalFlags{}, genericCLIOpts{in: strings.NewReader("")})
	_, closer, err := flags.createLineReader(command, []string{})
	require.NotNil(t, closer)
	defer closer.Close()
	require.NotNil(t, err)
	require.Contains(t, fmt.Sprintf("%s", err), "unsupported")

	// fail on unsupported encoding
	flags = writeFlagsType{}
	flags.Encoding = "green"
	command = cmdWrite(&globalFlags{}, genericCLIOpts{in: strings.NewReader("")})
	_, closer, err = flags.createLineReader(command, []string{})
	require.NotNil(t, closer)
	defer closer.Close()
	require.NotNil(t, err)
	require.Contains(t, fmt.Sprintf("%s", err), "https://www.iana.org/assignments/character-sets/character-sets.xhtml")

	// fail on file not found
	flags = writeFlagsType{}
	flags.Files = []string{csvFile1 + "x"}
	command = cmdWrite(&globalFlags{}, genericCLIOpts{in: strings.NewReader("")})
	_, closer, err = flags.createLineReader(command, []string{})
	require.NotNil(t, closer)
	defer closer.Close()
	require.NotNil(t, err)
	require.Contains(t, fmt.Sprintf("%s", err), csvFile1)
}

func Test_fluxWriteDryrunF(t *testing.T) {
	stdInContents := "i,j,_measurement,k\nstdin1,stdin2,stdin3,stdin4"
	out := bytes.Buffer{}
	command := cmdWrite(&globalFlags{}, genericCLIOpts{in: strings.NewReader(stdInContents), w: bufio.NewWriter(&out)})
	command.SetArgs([]string{"dryrun", "--format", "csv"})
	err := command.Execute()
	require.Nil(t, err)
	require.Equal(t, "stdin3 i=stdin1,j=stdin2,k=stdin4", strings.Trim(out.String(), "\n"))

	out = bytes.Buffer{}
	command = cmdWrite(&globalFlags{}, genericCLIOpts{in: strings.NewReader(stdInContents), w: bufio.NewWriter(&out)})
	command.SetArgs([]string{"dryrun", "--format", "csvx"})
	err = command.Execute()
	require.NotNil(t, err)
	require.Contains(t, fmt.Sprintf("%s", err), "unsupported") // unsupported format

	stdInContents = "i,j,l,k\nstdin1,stdin2,stdin3,stdin4"
	out = bytes.Buffer{}
	command = cmdWrite(&globalFlags{}, genericCLIOpts{in: strings.NewReader(stdInContents), w: bufio.NewWriter(&out)})
	command.SetArgs([]string{"dryrun", "--format", "csv"})
	err = command.Execute()
	require.NotNil(t, err)
	require.Contains(t, fmt.Sprintf("%s", err), "measurement") // no measurement column

}

func Test_fluxWriteF(t *testing.T) {
	var lineData []byte
	// use a local HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		url := req.URL.String()
		// fmt.Println(url)
		switch {
		case strings.Contains(url, "error"):
			rw.WriteHeader(http.StatusInternalServerError)
			rw.Write([]byte(`ERROR`))
			return
		case strings.Contains(url, "empty"):
			rw.WriteHeader(http.StatusOK)
			rw.Write([]byte(`{
				"links":{
					"self":"/api/v2/buckets?descending=false\u0026limit=20\u0026offset=0\u0026orgID=b112ec3528efa3b4"
				},
				"buckets":[]
			}`))
			return
		case strings.HasPrefix(url, "/api/v2/buckets"):
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
	stdInContents := "i,j,_measurement,k\nstdin1,stdin2,stdin3,stdin4"
	stdOut := bytes.Buffer{}

	lineData = lineData[:0]
	stdOut.Reset()
	command := cmdWrite(&globalFlags{}, genericCLIOpts{
		in: strings.NewReader(stdInContents),
		w:  bufio.NewWriter(&stdOut)})
	command.SetArgs([]string{"--format", "csv"})
	err := command.Execute()
	require.NotNil(t, err)
	require.Contains(t, fmt.Sprintf("%s", err), "org") // must specify org-id, or org name

	lineData = lineData[:0]
	stdOut.Reset()
	command = cmdWrite(&globalFlags{}, genericCLIOpts{
		in: strings.NewReader(stdInContents),
		w:  bufio.NewWriter(&stdOut)})
	command.SetArgs([]string{"--format", "csv", "--org", "my-org", "--bucket", "my-bucket", "--bucket-id", "my-bucket"})
	err = command.Execute()
	require.NotNil(t, err)
	require.Contains(t, fmt.Sprintf("%s", err), "bucket") // bucket or bucket-id, but not both

	lineData = lineData[:0]
	stdOut.Reset()
	command = cmdWrite(&globalFlags{}, genericCLIOpts{
		in: strings.NewReader(stdInContents),
		w:  bufio.NewWriter(&stdOut)})
	command.SetArgs([]string{"--format", "csv", "--org", "my-org", "--bucket", "my-bucket", "--precision", "pikosec"})
	err = command.Execute()
	require.NotNil(t, err)
	require.Contains(t, fmt.Sprintf("%s", err), "precision") // invalid precision

	prevHost := flags.Host
	prevToken := flags.Token
	flags.Host = ""
	flags.Token = "myToken"
	defer func() {
		flags.Host = prevHost
		flags.Token = prevToken
	}()

	lineData = lineData[:0]
	stdOut.Reset()
	command = cmdWrite(&globalFlags{}, genericCLIOpts{
		in: strings.NewReader(stdInContents),
		w:  bufio.NewWriter(&stdOut)})
	command.SetArgs([]string{"--format", "csv", "--org", "my-org", "--bucket", "my-bucket"})
	err = command.Execute()
	require.NotNil(t, err)
	require.Contains(t, fmt.Sprintf("%s", err), "host") // must provide a non empty host address

	lineData = lineData[:0]
	stdOut.Reset()
	flags.Host = server.URL
	command = cmdWrite(&globalFlags{}, genericCLIOpts{
		in: strings.NewReader(stdInContents),
		w:  bufio.NewWriter(&stdOut)})
	command.SetArgs([]string{"--format", "csv", "--org", "my-org", "--bucket-id", "my-bucket"})
	err = command.Execute()
	require.NotNil(t, err)
	require.Contains(t, fmt.Sprintf("%s", err), "bucket-id") // failed to decode bucket-id

	lineData = lineData[:0]
	stdOut.Reset()
	flags.Host = server.URL
	command = cmdWrite(&globalFlags{}, genericCLIOpts{
		in: strings.NewReader(stdInContents),
		w:  bufio.NewWriter(&stdOut)})
	command.SetArgs([]string{"--format", "csv", "--org-id", "my-org", "--bucket", "my-bucket"})
	err = command.Execute()
	require.NotNil(t, err)
	require.Contains(t, fmt.Sprintf("%s", err), "org-id") // failed to decode org-id

	lineData = lineData[:0]
	stdOut.Reset()
	flags.Host = server.URL
	command = cmdWrite(&globalFlags{}, genericCLIOpts{
		in: strings.NewReader(stdInContents),
		w:  bufio.NewWriter(&stdOut)})
	command.SetArgs([]string{"--format", "csv", "--org", "my-org", "--bucket", "my-error-bucket"})
	err = command.Execute()
	require.NotNil(t, err)
	require.Contains(t, fmt.Sprintf("%s", err), "bucket") // failed to retrieve buckets

	lineData = lineData[:0]
	stdOut.Reset()
	command = cmdWrite(&globalFlags{}, genericCLIOpts{
		in: strings.NewReader(stdInContents),
		w:  bufio.NewWriter(&stdOut)})
	command.SetArgs([]string{"--format", "csv", "--org", "my-empty-org", "--bucket", "my-bucket"})
	err = command.Execute()
	require.NotNil(t, err)
	require.Contains(t, fmt.Sprintf("%s", err), "bucket") // no such bucket found

	lineData = lineData[:0]
	stdOut.Reset()
	command = cmdWrite(&globalFlags{}, genericCLIOpts{
		in: strings.NewReader(stdInContents),
		w:  bufio.NewWriter(&stdOut)})
	command.SetArgs([]string{"--format", "csv", "--org", "my-empty-org", "--bucket-id", "4f14589c26df8286"})
	err = command.Execute()
	require.NotNil(t, err)
	require.Contains(t, fmt.Sprintf("%s", err), "id") // no such bucket id found

	lineData = lineData[:0]
	stdOut.Reset()
	command = cmdWrite(&globalFlags{}, genericCLIOpts{
		in: strings.NewReader("a,b\nc,d"),
		w:  bufio.NewWriter(&stdOut)})
	command.SetArgs([]string{"--format", "csvx", "--org", "my-org", "--bucket-id", "4f14589c26df8286"})
	err = command.Execute()
	require.NotNil(t, err)
	require.Contains(t, fmt.Sprintf("%s", err), "format") // unsupported format

	lineData = lineData[:0]
	stdOut.Reset()
	command = cmdWrite(&globalFlags{}, genericCLIOpts{
		in: strings.NewReader("a,b\nc,d"),
		w:  bufio.NewWriter(&stdOut)})
	command.SetArgs([]string{"--format", "csv", "--org", "my-org", "--bucket-id", "4f14589c26df8286"})
	err = command.Execute()
	require.NotNil(t, err)
	require.Contains(t, fmt.Sprintf("%s", err), "measurement") // no measurement found in CSV data

	lineData = lineData[:0]
	stdOut.Reset()
	command = cmdWrite(&globalFlags{}, genericCLIOpts{
		in: strings.NewReader(stdInContents),
		w:  bufio.NewWriter(&stdOut)})
	command.SetArgs([]string{"--format", "csv", "--org", "my-org", "--bucket-id", "4f14589c26df8286"})
	err = command.Execute()
	require.Nil(t, err)
	require.Equal(t, "stdin3 i=stdin1,j=stdin2,k=stdin4", strings.Trim(string(lineData), "\n"))
}
