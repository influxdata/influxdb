package v8

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/influxdata/influxdb/models"
)

func TestImporter_MultilineFieldsAcrossBatchBoundary(t *testing.T) {
	var (
		mu             sync.Mutex
		writeRequests  int
		importedPoints int
		parseErrors    []error
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ping":
			w.WriteHeader(http.StatusNoContent)
		case "/write":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			points, parseErr := models.ParsePoints(body)
			mu.Lock()
			writeRequests++
			if parseErr != nil {
				parseErrors = append(parseErrors, parseErr)
			} else {
				importedPoints += len(points)
			}
			mu.Unlock()

			if parseErr != nil {
				http.Error(w, "partial write: "+parseErr.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	input, err := os.CreateTemp(t.TempDir(), "multiline-import-*.lp")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintln(input, "# INFLUXDB EXPORT")
	fmt.Fprintln(input, "# DDL")
	fmt.Fprintln(input, "# DML")
	fmt.Fprintln(input, "# CONTEXT-DATABASE:testdb")
	fmt.Fprintln(input, "# CONTEXT-RETENTION-POLICY:default")
	value := models.EscapeStringField(`aaaaaaaaaa
bbbbbbbbbb
cccccccccc
dddddddddd
eeeeeeeeee
ffffffffff
gggggggggg
hhhhhhhhhh
iiiiiiiiii
jjjjjjjjjj
kkkkk"quote
lllll\slash`)
	for n := range 501 {
		fmt.Fprintf(input, "test,host=hosthost testvalue=\"%s\" %d\n", value, n)
	}
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	config := NewConfig()
	config.Path = input.Name()
	config.URL = *serverURL

	if err := NewImporter(config).Import(); err != nil {
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("import failed: %v; write parse errors: %v", err, parseErrors)
	}

	mu.Lock()
	defer mu.Unlock()
	if got, want := writeRequests, 1; got != want {
		t.Fatalf("write request count: got %d, want %d", got, want)
	}
	if got, want := importedPoints, 501; got != want {
		t.Fatalf("imported point count: got %d, want %d", got, want)
	}
}

func TestReadLineProtocolRecord_UnbalancedQuote(t *testing.T) {
	_, err := readLineProtocolRecord(
		bufio.NewReader(strings.NewReader("continued")),
		"test value=\"first line\n",
	)
	if err == nil || err.Error() != "unbalanced quotes" {
		t.Fatalf("got %v, want unbalanced quotes", err)
	}
}

func TestReadLineProtocolRecord_TrailingBackslash(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("next value=2\n"))
	record, err := readLineProtocolRecord(reader, "test value=first\\\n")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := record, "test value=first\\\n"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestReadLineProtocolRecord_SizeLimit(t *testing.T) {
	_, err := readLineProtocolRecord(
		bufio.NewReader(strings.NewReader(strings.Repeat("x", maxLineProtocolRecordSize))),
		"test value=\"first line\n",
	)
	want := fmt.Sprintf("line protocol record exceeds %d byte limit", maxLineProtocolRecordSize)
	if err == nil || err.Error() != want {
		t.Fatalf("got %v, want %s", err, want)
	}
}
