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
