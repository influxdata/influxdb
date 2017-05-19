package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

var prefixingRedirectTests = []struct {
	CaseName       string
	RedirectTarget string
	Prefix         string
	Expected       string
}{
	{
		"ChronografBasepath",
		"/chronograf/v1/",
		"/chronograf",
		"/chronograf/chronograf/v1/",
	},
	{
		"DifferentBasepath",
		"/chronograf/v1/",
		"/delorean",
		"/delorean/chronograf/v1/",
	},
	{
		"TrailingSlashPrefix",
		"/chronograf/v1/",
		"/delorean/",
		"/delorean/chronograf/v1/",
	},
	{
		"NoPrefix",
		"/chronograf/v1/",
		"",
		"/chronograf/v1/",
	},
	{
		"SlashPrefix",
		"/chronograf/v1/",
		"/",
		"/chronograf/v1/",
	},
	{
		"AlreadyPrefixed",
		"/chronograf/chronograf/v1/",
		"/chronograf",
		"/chronograf/chronograf/v1/",
	},
}

func Test_PrefixingRedirector(t *testing.T) {
	t.Parallel()
	for _, p := range prefixingRedirectTests {
		t.Run(p.CaseName, func(subt *testing.T) {
			hf := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				rw.Header().Set("Location", p.RedirectTarget)
				rw.WriteHeader(http.StatusTemporaryRedirect)
			})
			pr := PrefixedRedirect(p.Prefix, hf)

			ts := httptest.NewServer(pr)
			defer ts.Close()

			hc := http.Client{
				CheckRedirect: func(req *http.Request, via []*http.Request) error {
					return http.ErrUseLastResponse
				},
			}

			mockBody := strings.NewReader("")
			req, _ := http.NewRequest("GET", ts.URL, mockBody)

			resp, err := hc.Do(req)
			if err != nil {
				subt.Fatal("Unexpected http err:", err)
			}

			expected := p.Expected
			if loc := resp.Header.Get("Location"); loc != expected {
				subt.Fatal("Unexpected redirected location. Expected:", expected, "Actual:", loc)
			}
		})
	}
}
