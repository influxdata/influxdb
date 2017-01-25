package server_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/chronograf/server"
)

var prefixerTests = []struct {
	name      string
	subject   string
	expected  string
	shouldErr bool
}{
	{
		`One script tag`,
		`<script type="text/javascript" src="/loljavascript.min.js">`,
		`<script type="text/javascript" src="/arbitraryprefix/loljavascript.min.js">`,
		false,
	},
	{
		`Two script tags`,
		`<script type="text/javascript" src="/loljavascript.min.js"><script type="text/javascript" src="/anotherscript.min.js">`,
		`<script type="text/javascript" src="/arbitraryprefix/loljavascript.min.js"><script type="text/javascript" src="/arbitraryprefix/anotherscript.min.js">`,
		false,
	},
}

func Test_Server_Prefixer_RewritesURLs(t *testing.T) {
	t.Parallel()

	for _, test := range prefixerTests {
		subject := test.subject
		expected := test.expected

		backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, subject)
		})

		pfx := &server.URLPrefixer{Prefix: "/arbitraryprefix", Next: backend}

		ts := httptest.NewServer(pfx)
		defer ts.Close()

		res, err := http.Get(ts.URL)
		if err != nil {
			t.Error("Unexpected error fetching from prefixer: err:", err)
		}

		actual, err := ioutil.ReadAll(res.Body)
		if err != nil {
			t.Error("Unable to read prefixed body: err:", err)
		}

		if string(actual) != expected+"\n" {
			t.Error(test.name, ":\n Unsuccessful prefixing.\n\tWant:", expected, "\n\tGot:", string(actual))
		}
	}
}
