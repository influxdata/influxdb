package v8

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/models"
	th "github.com/influxdata/influxdb/pkg/testing/helper"
	"github.com/stretchr/testify/require"
)

func TestImporter_MultilineFieldsAcrossBatchBoundary(t *testing.T) {
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
	records := make([]string, 501)
	for n := range records {
		records[n] = fmt.Sprintf("test,host=hosthost testvalue=\"%s\" %d", value, n)
	}

	testImportRecords(t, records, true)
}

func TestImporter_UnterminatedFinalPoint(t *testing.T) {
	testImportRecords(t, []string{"test value=1i 1"}, false)
}

func testImportRecords(t *testing.T, records []string, trailingNewline bool) {
	t.Helper()

	var writeRequests int
	var receivedPoints []string
	var parseErrors, serverErrors []error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ping":
			w.WriteHeader(http.StatusNoContent)
		case "/write":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				serverErrors = append(serverErrors, err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			points, parseErr := models.ParsePoints(body)
			writeRequests++
			if parseErr != nil {
				parseErrors = append(parseErrors, parseErr)
			} else {
				for _, point := range points {
					receivedPoints = append(receivedPoints, point.String())
				}
			}

			if parseErr != nil {
				http.Error(w, "partial write: "+parseErr.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	var importContent strings.Builder
	importContent.WriteString("# INFLUXDB EXPORT\n# DDL\n# DML\n")
	importContent.WriteString("# CONTEXT-DATABASE:testdb\n")
	importContent.WriteString("# CONTEXT-RETENTION-POLICY:default\n")
	importContent.WriteString(strings.Join(records, "\n"))
	if trailingNewline {
		importContent.WriteByte('\n')
	}

	importPath := filepath.Join(t.TempDir(), "import.lp")
	importFile, err := os.Create(importPath)
	require.NoError(t, err)
	closeImportFile := th.CheckedCloseOnce(t, importFile)
	defer closeImportFile()
	_, err = io.WriteString(importFile, importContent.String())
	require.NoError(t, err)
	closeImportFile()

	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)
	config := NewConfig()
	config.Path = importPath
	config.URL = *serverURL

	importErr := NewImporter(config).Import()
	require.NoErrorf(t, importErr, "server errors: %v; write parse errors: %v", serverErrors, parseErrors)
	require.Empty(t, serverErrors)
	require.Empty(t, parseErrors)
	require.Equal(t, 1, writeRequests)
	require.Equal(t, records, receivedPoints)
}
