package server_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/platform/chronograf/mocks"
	"github.com/influxdata/platform/chronograf/server"
)

var prefixerTests = []struct {
	name      string
	subject   string
	expected  string
	shouldErr bool
	attrs     [][]byte
}{
	{
		`One script tag`,
		`<script type="text/javascript" src="/loljavascript.min.js">`,
		`<script type="text/javascript" src="/arbitraryprefix/loljavascript.min.js">`,
		false,
		[][]byte{
			[]byte(`src="`),
		},
	},
	{
		`Two script tags`,
		`<script type="text/javascript" src="/loljavascript.min.js"><script type="text/javascript" src="/anotherscript.min.js">`,
		`<script type="text/javascript" src="/arbitraryprefix/loljavascript.min.js"><script type="text/javascript" src="/arbitraryprefix/anotherscript.min.js">`,
		false,
		[][]byte{
			[]byte(`src="`),
		},
	},
	{
		`Link href`,
		`<link rel="shortcut icon" href="/favicon.ico">`,
		`<link rel="shortcut icon" href="/arbitraryprefix/favicon.ico">`,
		false,
		[][]byte{
			[]byte(`src="`),
			[]byte(`href="`),
		},
	},
	{
		`Trailing HTML`,
		`<!DOCTYPE html>
		<html>
			<head>
				<meta http-equiv="Content-type" content="text/html; charset=utf-8"/>
				<title>Chronograf</title>
			<link rel="shortcut icon" href="/favicon.ico"><link href="/chronograf.css" rel="stylesheet"></head>
			<body>
				<div id='react-root'></div>
			<script type="text/javascript" src="/manifest.7489452b099f9581ca1b.dev.js"></script><script type="text/javascript" src="/vendor.568c0101d870a13ecff9.dev.js"></script><script type="text/javascript" src="/app.13d0ce0b33609be3802b.dev.js"></script></body>
		</html>`,
		`<!DOCTYPE html>
		<html>
			<head>
				<meta http-equiv="Content-type" content="text/html; charset=utf-8"/>
				<title>Chronograf</title>
			<link rel="shortcut icon" href="/arbitraryprefix/favicon.ico"><link href="/arbitraryprefix/chronograf.css" rel="stylesheet"></head>
			<body>
				<div id='react-root'></div>
			<script type="text/javascript" src="/arbitraryprefix/manifest.7489452b099f9581ca1b.dev.js"></script><script type="text/javascript" src="/arbitraryprefix/vendor.568c0101d870a13ecff9.dev.js"></script><script type="text/javascript" src="/arbitraryprefix/app.13d0ce0b33609be3802b.dev.js"></script></body>
		</html>`,
		false,
		[][]byte{
			[]byte(`src="`),
			[]byte(`href="`),
		},
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

		pfx := &server.URLPrefixer{Prefix: "/arbitraryprefix", Next: backend, Attrs: test.attrs}

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
			t.Error(test.name, ":\n Unsuccessful prefixing.\n\tWant:", fmt.Sprintf("%+q", expected), "\n\tGot: ", fmt.Sprintf("%+q", string(actual)))
		}
	}
}

// clogger is an http.ResponseWriter that is not an http.Flusher. It is used
// for testing the behavior of handlers that may rely on specific behavior of
// http.Flusher
type clogger struct {
	next http.ResponseWriter
}

func (c *clogger) Header() http.Header {
	return c.next.Header()
}

func (c *clogger) Write(bytes []byte) (int, error) {
	return c.next.Write(bytes)
}

func (c *clogger) WriteHeader(code int) {
	c.next.WriteHeader(code)
}

func Test_Server_Prefixer_NoPrefixingWithoutFlusther(t *testing.T) {
	backend := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(rw, "<a href=\"/valley\">Hill Valley Preservation Society</a>")
	})

	wrapFunc := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			clog := &clogger{rw}
			next.ServeHTTP(clog, r)
		})
	}

	tl := &mocks.TestLogger{}
	pfx := &server.URLPrefixer{
		Prefix: "/hill",
		Next:   backend,
		Logger: tl,
		Attrs: [][]byte{
			[]byte("href=\""),
		},
	}

	ts := httptest.NewServer(wrapFunc(pfx))
	defer ts.Close()

	res, err := http.Get(ts.URL)
	if err != nil {
		t.Fatal("Unexpected error fetching from prefixer: err:", err)
	}

	actual, err := ioutil.ReadAll(res.Body)
	if err != nil {
		t.Fatal("Unable to read prefixed body: err:", err)
	}

	unexpected := "<a href=\"/hill/valley\">Hill Valley Preservation Society</a>"
	expected := "<a href=\"/valley\">Hill Valley Preservation Society</a>"
	if string(actual) == unexpected {
		t.Error("No Flusher", ":\n Prefixing occurred without an http.Flusher")
	}

	if string(actual) != expected {
		t.Error("No Flusher", ":\n\tPrefixing failed to output without an http.Flusher\n\t\tWant:\n", expected, "\n\t\tGot:\n", string(actual))
	}

	if !tl.HasMessage("info", server.ErrNotFlusher) {
		t.Error("No Flusher", ":\n Expected Error Message: \"", server.ErrNotFlusher, "\" but saw none. Msgs:", tl.Messages)
	}
}
