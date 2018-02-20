package oauth2

import (
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	clog "github.com/influxdata/chronograf/log"
)

var testTime = time.Date(1985, time.October, 25, 18, 0, 0, 0, time.UTC)

// setupMuxTest produces an http.Client and an httptest.Server configured to
// use a particular http.Handler selected from a AuthMux. As this selection is
// done during the setup process, this configuration is performed by providing
// a function, and returning the desired handler. Cleanup is still the
// responsibility of the test writer, so the httptest.Server's Close() method
// should be deferred.
func setupMuxTest(selector func(*AuthMux) http.Handler, body string) (*http.Client, *httptest.Server, *httptest.Server) {
	provider := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if body != "" {
			rw.Header().Set("Content-Type", "application/json")
		}
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte(body))
	}))

	now := func() time.Time {
		return testTime
	}
	mp := &MockProvider{
		Email:       "biff@example.com",
		ProviderURL: provider.URL,
		Orgs:        "",
	}
	mt := &YesManTokenizer{}
	auth := &cookie{
		Name:       DefaultCookieName,
		Lifespan:   1 * time.Hour,
		Inactivity: DefaultInactivityDuration,
		Now:        now,
		Tokens:     mt,
	}

	jm := NewAuthMux(mp, auth, mt, "", clog.New(clog.ParseLevel("debug")))
	ts := httptest.NewServer(selector(jm))
	jar, _ := cookiejar.New(nil)
	hc := http.Client{
		Jar: jar,
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return &hc, ts, provider
}

// teardownMuxTest cleans up any resources created by setupMuxTest. This should
// be deferred in your test after setupMuxTest is called
func teardownMuxTest(hc *http.Client, backend *httptest.Server, provider *httptest.Server) {
	provider.Close()
	backend.Close()
}

func Test_AuthMux_Logout_DeletesSessionCookie(t *testing.T) {
	t.Parallel()

	hc, ts, prov := setupMuxTest(func(j *AuthMux) http.Handler {
		return j.Logout()
	}, "")
	defer teardownMuxTest(hc, ts, prov)

	tsURL, _ := url.Parse(ts.URL)

	hc.Jar.SetCookies(tsURL, []*http.Cookie{
		&http.Cookie{
			Name:  DefaultCookieName,
			Value: "",
		},
	})

	resp, err := hc.Get(ts.URL)
	if err != nil {
		t.Fatal("Error communicating with Logout() handler: err:", err)
	}

	if resp.StatusCode < 300 || resp.StatusCode >= 400 {
		t.Fatal("Expected to be redirected, but received status code", resp.StatusCode)
	}

	cookies := resp.Cookies()
	if len(cookies) != 1 {
		t.Fatal("Expected that cookie would be present but wasn't")
	}

	c := cookies[0]
	if c.Name != DefaultCookieName || c.Expires != testTime.Add(-1*time.Hour) {
		t.Fatal("Expected cookie to be expired but wasn't")
	}
}

func Test_AuthMux_Login_RedirectsToCorrectURL(t *testing.T) {
	t.Parallel()

	hc, ts, prov := setupMuxTest(func(j *AuthMux) http.Handler {
		return j.Login() // Use Login handler for httptest server.
	}, "")
	defer teardownMuxTest(hc, ts, prov)

	resp, err := hc.Get(ts.URL)
	if err != nil {
		t.Fatal("Error communicating with Login() handler: err:", err)
	}

	// Ensure we were redirected
	if resp.StatusCode < 300 || resp.StatusCode >= 400 {
		t.Fatal("Expected to be redirected, but received status code", resp.StatusCode)
	}

	loc, err := resp.Location()
	if err != nil {
		t.Fatal("Expected a location to be redirected to, but wasn't present")
	}

	if state := loc.Query().Get("state"); state != "HELLO?!MCFLY?!ANYONEINTHERE?!" {
		t.Fatalf("Expected state to be %s set but was %s", "HELLO?!MCFLY?!ANYONEINTHERE?!", state)
	}
}

func Test_AuthMux_Callback_SetsCookie(t *testing.T) {
	hc, ts, prov := setupMuxTest(func(j *AuthMux) http.Handler {
		return j.Callback()
	}, "")
	defer teardownMuxTest(hc, ts, prov)

	tsURL, _ := url.Parse(ts.URL)

	v := url.Values{
		"code":  {"4815162342"},
		"state": {"foobar"},
	}

	tsURL.RawQuery = v.Encode()

	resp, err := hc.Get(tsURL.String())
	if err != nil {
		t.Fatal("Error communicating with Callback() handler: err", err)
	}

	// Ensure we were redirected
	if resp.StatusCode < 300 || resp.StatusCode >= 400 {
		t.Fatal("Expected to be redirected, but received status code", resp.StatusCode)
	}

	// Check that cookie was set
	cookies := resp.Cookies()
	if count := len(cookies); count != 1 {
		t.Fatal("Expected exactly one cookie to be set but found", count)
	}

	c := cookies[0]

	if c.Name != DefaultCookieName {
		t.Fatal("Expected cookie to be named", DefaultCookieName, "but was", c.Name)
	}
}

func Test_AuthMux_Callback_HandlesIdToken(t *testing.T) {
	// body taken from ADFS4
	body := `{"access_token":"eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IllEQlVocWRXa3NLWGRHdVgwc3l0amFVdXhoQSJ9.eyJhdWQiOiJ1cm46bWljcm9zb2Z0OnVzZXJpbmZvIiwiaXNzIjoiaHR0cDovL2RzdGNpbWFhZDFwLmRzdC1pdHMuZGUvYWRmcy9zZXJ2aWNlcy90cnVzdCIsImlhdCI6MTUxNTcwMDU2NSwiZXhwIjoxNTE1NzA0MTY1LCJhcHB0eXBlIjoiQ29uZmlkZW50aWFsIiwiYXBwaWQiOiJjaHJvbm9ncmFmIiwiYXV0aG1ldGhvZCI6InVybjpvYXNpczpuYW1lczp0YzpTQU1MOjIuMDphYzpjbGFzc2VzOlBhc3N3b3JkUHJvdGVjdGVkVHJhbnNwb3J0IiwiYXV0aF90aW1lIjoiMjAxOC0wMS0xMVQxOTo1MToyNS44MDZaIiwidmVyIjoiMS4wIiwic2NwIjoib3BlbmlkIiwic3ViIjoiZVlWM2pkbGROeUZZMXFGRkg0b0FkQnZERmZiVmZudUcyOUhpSGtTdWp3az0ifQ.sf1qJys9LMUp2S232IRK2aTXiPCE93O-cUdYQQz7kg2woyD46KLwwKIYJVqMaqLspTn3OmaIhKtgx5ZXyAEtihODB1GOBK7DBNRBYCS1iqY_v2-Qwjf7hgaNaCqBjs0DZJspfp5G9MTykvD1FOtQNjPOcBW-i2bblG9L9jlmMbOZ3F7wrZMrroTSkiSn_gRiw2SnN8K7w8WrMEXNK2_jg9ZJ7aSHeUSBwkRNFRds2QNho3HWHg-zcsZFdZ4UGSt-6Az_0LY3yENMLj5us5Rl6Qzk_Re2dhFrlnlXlY1v1DEp3icCvvjkv6AeZWjTfW4qETZaCXUKtSyZ7d5_V1CRDQ","token_type":"bearer","expires_in":3600,"resource":"urn:microsoft:userinfo","refresh_token":"X9ZGO4H1bMk2bFeOfpv18BzAuFBzUPKQNfOEfdp60FkAAQAALPEBfj23FPEzajle-hm4DrDXp8-Kj53OqoVGalyZeuR-lfJzxpQXQhRAXZOUTuuQ8AQycByh9AylQYDA0jdMFW4FL4WL_6JhNh2JrtXCv2HQ9ozbUq9F7u_O0cY7u0P2pfNujQfk3ckYn-CMVjXbuwJTve6bXUR0JDp5c195bAVA5eFWyI-2uh432t7viyaIjAVbWxQF4fvimcpF1Et9cGodZHVsrZzGxKRnzwjYkWHsqm9go4KOeSKN6MlcWbjvS1UdMjQXSvoqSI00JnSMC3hxJZFn5JcmAPB1AMnJf4VvXZ5b-aOnwdX09YT8KayWkWekAsuZqTAsFwhZPVCRGWAFAADy0e2fTe6l-U6Cj_2bWsq6Snm1QEpWHXuwOJKWZJH-9yQn8KK3KzRowSzRuACzEIpZS5skrqXs_-2aOaZibNpjCEVyw8fF8GTw3VRLufsSrMQ5pD0KL7TppTGFpaqgwIH1yq6T8aRY4DeyoJkNpnO9cw1wuqnY7oGF-J25sfZ4XNWhk6o5e9A45PXhTilClyDKDLqTfdoIsG1Koc2ywqTIb-XI_EbWR3e4ijy8Kmlehw1kU9_xAG0MmmD2HTyGHZCBRgrskYCcHd-UNgCMrNAb5dZQ8NwpKtEL46qIq4R0lheTRRK8sOWzzuJXmvDEoJiIxqSR3Ma4MOISi-vsIsAuiEL9G1aMOkDRj-kDVmqrdKRAwYnN78AWY5EFfkQJyVBbiG882wBh9S0q3HUUCxzFerOvl4eDlVn6m18rRMz7CVZYBBltGtHRhEOQ4gumICR5JRrXAC50aBmUlhDiiMdbEIwJrvWrkhKE0oAJznqC7gleP0E4EOEh9r6CEGZ7Oj8X9Cdzjbuq2G1JGBm_yUvkhAcV61DjOiIQl35BpOfshveNZf_caUtNMa2i07BBmezve17-2kWGzRunr1BD1vMTz41z-H62fy4McR47WJjdDJnuy4DH5AZYQ6ooVxWCtEqeqRPYpzO0XdOdJGXFqXs9JzDKVXTgnHU443hZBC5H-BJkZDuuJ_ZWNKXf03JhouWkxXcdaMbuaQYOZJsUySVyJ5X4usrBFjW4udZAzy7mua-nJncbvcwoyVXiFlRfZiySXolQ9865N7XUnEk_2PijMLoVDATDbA09XuRySvngNsdsQ27p21dPxChXdtpD5ofNqKJ2FBzFKmxCkuX7L01N1nDpWQTuxhHF0JfxSKG5m3jcTx8Bd7Un94mTuAB7RuglDqkdQB9o4X9NHNGSdqGQaK-xeKoNCFWevk3VZoDoY9w2NqSNV2VIuqhy7SxtDSMjZKC5kiQi5EfGeTYZAvTwMYwaXb7K4WWtscy_ZE15EOCVeYi0hM1Ma8iFFTANkSRyX83Ju4SRphxRKnpKcJ2pPYH784I5HOm5sclhUL3aLeAA161QgxRBSa9YVIZfyXHyWQTcbNucNdhmdUZnKfRv1xtXcS9VAx2yAkoKFehZivEINX0Y500-WZ1eT_RXp0BfCKmJQ8Fu50oTaI-c5h2Q3Gp_LTSODNnMrjJiJxCLD_LD1fd1e8jTYDV3NroGlpWTuTdjMUm-Z1SMXaaJzQGEnNT6F8b6un9228L6YrDC_3MJ5J80VAHL5EO1GesdEWblugCL7AQDtFjNXq0lK8Aoo8X9_hlvDwgfdR16l8QALPT1HJVzlHPG8G3dRe50TKZnl3obU0WXN1KYG1EC4Qa3LyaVCIuGJYOeFqjMINrf7PoM368nS9yhrY08nnoHZbQ7IeA1KsNq2kANeH1doCNfWrXDwn8KxjYxZPEnzvlQ5M1RIzArOqzWL8NbftW1q2yCZZ4RVg0vOTVXsqWFnQIvWK-mkELa7bvByFzbtVHOJpc_2EKBKBNv6IYUENRCu2TOf6w7u42yvng7ccoXRTiUFUlKgVmswf9FzISxFd-YKgrzp3bMhC3gReGqcJuqEwnXPvOAY_BAkVMSd_ZaCFuyclRjFvUxrAg1T_cqOvRIlJ2Qq7z4u7W3BAo9BtFdj8QNLKJXtvvzXTprglRPDNP_QEPAkwZ_Uxa13vdYFcG18WCx4GbWQXchl5B7DnISobcdCH34M-I0xDZN98VWQVmLAfPniDUD30C8pfiYF7tW_EVy958Eg_JWVy0SstYEhV-y-adrJ1Oimjv0ptsWv-yErKBUD14aex9A_QqdnTXZUg.tqMb72eWAkAIvInuLp57NDyGxfYvms3NnhN-mllkYb7Xpd8gVbQFc2mYdzOOhtnfGuakyXYF4rZdJonQwzBO6C9KYuARciUU1Ms4bWPC-aeNO5t-aO_bDZbwC9qMPmq5ZuxG633BARGaw26fr0Z7qhcJMiou_EuaIehYTKkPB-mxtRAhxxyX91qqe0-PJnCHWoxizC4hDCUwp9Jb54tNf34BG3vtkXFX-kUARNfGucgKUkh6RYkhWiMBsMVoyWmkFXB5fYxmCAH5c5wDW6srKdyIDEWZInliuKbYR0p66vg1FfoSi4bBfrsm5NtCtLKG9V6Q0FEIA6tRRgHmKUGpkw","refresh_token_expires_in":28519,"scope":"openid","id_token":"eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IllEQlVocWRXa3NLWGRHdVgwc3l0amFVdXhoQSIsImtpZCI6IllEQlVocWRXa3NLWGRHdVgwc3l0amFVdXhoQSJ9.eyJhdWQiOiJjaHJvbm9ncmFmIiwiaXNzIjoiaHR0cHM6Ly9kc3RjaW1hYWQxcC5kc3QtaXRzLmRlL2FkZnMiLCJpYXQiOjE1MTU3MDA1NjUsImV4cCI6MTUxNTcwNDE2NSwiYXV0aF90aW1lIjoxNTE1NzAwMjg1LCJzdWIiOiJlWVYzamRsZE55RlkxcUZGSDRvQWRCdkRGZmJWZm51RzI5SGlIa1N1andrPSIsInVwbiI6ImJzY0Bkc3QtaXRzLmRlIiwidW5pcXVlX25hbWUiOiJEU1RcXGJzYyIsInNpZCI6IlMtMS01LTIxLTI1MDUxNTEzOTgtMjY2MTAyODEwOS0zNzU0MjY1ODIwLTExMDQifQ.XD873K6NVRTJY1700NsflLJGZKFHJfNBjB81SlADVdAHbhnq7wkAZbGEEm8wFqvTKKysUl9EALzmDa2tR9nzohVvmHftIYBO0E-wPBzdzWWX0coEgpVAc-SysP-eIQWLsj8EaodaMkCgKO0FbTWOf4GaGIBZGklrr9EEk8VRSdbXbm6Sv9WVphezEzxq6JJBRBlCVibCnZjR5OYh1Vw_7E7P38ESPbpLY3hYYl2hz4y6dQJqCwGr7YP8KrDlYtbosZYgT7ayxokEJI1udEbX5PbAq5G6mj5rLfSOl85rMg-psZiivoM8dn9lEl2P7oT8rAvMWvQp-FIRQQHwqf9cxw"}`
	hc, ts, prov := setupMuxTest(func(j *AuthMux) http.Handler {
		return j.Callback()
	}, body)
	defer teardownMuxTest(hc, ts, prov)

	tsURL, _ := url.Parse(ts.URL)

	v := url.Values{
		"code":  {"4815162342"},
		"state": {"foobar"},
	}

	tsURL.RawQuery = v.Encode()

	resp, err := hc.Get(tsURL.String())
	if err != nil {
		t.Fatal("Error communicating with Callback() handler: err", err)
	}

	// Ensure we were redirected
	if resp.StatusCode < 300 || resp.StatusCode >= 400 {
		t.Fatal("Expected to be redirected, but received status code", resp.StatusCode)
	}

	// Check that cookie was set
	cookies := resp.Cookies()
	if count := len(cookies); count != 1 {
		t.Fatal("Expected exactly one cookie to be set but found", count)
	}

	c := cookies[0]

	if c.Name != DefaultCookieName {
		t.Fatal("Expected cookie to be named", DefaultCookieName, "but was", c.Name)
	}
}
