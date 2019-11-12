package testttp

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Req is a request builder.
type Req struct {
	addr    string
	method  string
	body    io.Reader
	headers []string
}

// HTTP runs creates a request for an http call.
func HTTP(method, addr string, body io.Reader) *Req {
	return &Req{
		addr:   addr,
		method: method,
		body:   body,
	}
}

// Delete creates a DELETE request.
func Delete(addr string) *Req {
	return HTTP(http.MethodDelete, addr, nil)
}

// Get creates a GET request.
func Get(addr string) *Req {
	return HTTP(http.MethodGet, addr, nil)
}

// Patch creates a PATCH request.
func Patch(addr string, body io.Reader) *Req {
	return HTTP(http.MethodPatch, addr, body)
}

// Post creates a POST request.
func Post(addr string, body io.Reader) *Req {
	return HTTP(http.MethodPost, addr, body)
}

// Put creates a PUT request.
func Put(addr string, body io.Reader) *Req {
	return HTTP(http.MethodPut, addr, body)
}

// Headers allows the user to set headers on the http request.
func (r *Req) Headers(k, v string, rest ...string) *Req {
	r.headers = append(r.headers, k, v)
	r.headers = append(r.headers, rest...)
	return r
}

// Do runs the request against the provided handler.
func (r *Req) Do(handler http.Handler) *Resp {
	req := httptest.NewRequest(r.method, r.addr, r.body)
	rec := httptest.NewRecorder()

	for i := 0; i < len(r.headers); i += 2 {
		if i+1 >= len(r.headers) {
			break
		}
		k, v := r.headers[i], r.headers[i+1]
		req.Header.Add(k, v)
	}

	handler.ServeHTTP(rec, req)

	return &Resp{
		Req: req,
		Rec: rec,
	}
}

// Resp is a http recorder wrapper.
type Resp struct {
	Req *http.Request
	Rec *httptest.ResponseRecorder
}

// Expect allows the assertions against the raw Resp.
func (r *Resp) Expect(fn func(*Resp)) *Resp {
	fn(r)
	return r
}

// ExpectStatus compares the expected status code against the recorded status code.
func (r *Resp) ExpectStatus(t *testing.T, code int) *Resp {
	t.Helper()

	if r.Rec.Code != code {
		t.Errorf("unexpected status code: expected=%d got=%d", code, r.Rec.Code)
	}
	return r
}

// ExpectBody provides an assertion against the recorder body.
func (r *Resp) ExpectBody(fn func(*bytes.Buffer)) *Resp {
	fn(r.Rec.Body)
	return r
}

// ExpectHeader asserts that the header is in the recorder.
func (r *Resp) ExpectHeader(t *testing.T, k, v string) *Resp {
	t.Helper()

	vals, ok := r.Rec.Header()[k]
	if !ok {
		t.Errorf("did not find expected header: %q", k)
		return r
	}

	for _, vv := range vals {
		if vv == v {
			return r
		}
	}
	t.Errorf("did not find expected value for header %q; got: %v", k, vals)

	return r
}
