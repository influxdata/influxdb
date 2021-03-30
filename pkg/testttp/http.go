package testttp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// Req is a request builder.
type Req struct {
	t testing.TB

	req *http.Request
}

// HTTP runs creates a request for an http call.
func HTTP(t testing.TB, method, addr string, body io.Reader) *Req {
	return &Req{
		t:   t,
		req: httptest.NewRequest(method, addr, body),
	}
}

// Delete creates a DELETE request.
func Delete(t testing.TB, addr string) *Req {
	return HTTP(t, http.MethodDelete, addr, nil)
}

// Get creates a GET request.
func Get(t testing.TB, addr string) *Req {
	return HTTP(t, http.MethodGet, addr, nil)
}

// Patch creates a PATCH request.
func Patch(t testing.TB, addr string, body io.Reader) *Req {
	return HTTP(t, http.MethodPatch, addr, body)
}

// PatchJSON creates a PATCH request with a json encoded body.
func PatchJSON(t testing.TB, addr string, v interface{}) *Req {
	return HTTP(t, http.MethodPatch, addr, mustEncodeJSON(t, v))
}

// Post creates a POST request.
func Post(t testing.TB, addr string, body io.Reader) *Req {
	return HTTP(t, http.MethodPost, addr, body)
}

// PostJSON returns a POST request with a json encoded body.
func PostJSON(t testing.TB, addr string, v interface{}) *Req {
	return Post(t, addr, mustEncodeJSON(t, v))
}

// Put creates a PUT request.
func Put(t testing.TB, addr string, body io.Reader) *Req {
	return HTTP(t, http.MethodPut, addr, body)
}

// PutJSON creates a PUT request with a json encoded body.
func PutJSON(t testing.TB, addr string, v interface{}) *Req {
	return HTTP(t, http.MethodPut, addr, mustEncodeJSON(t, v))
}

// Do runs the request against the provided handler.
func (r *Req) Do(handler http.Handler) *Resp {
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, r.req)

	return &Resp{
		t:     r.t,
		debug: true,
		Req:   r.req,
		Rec:   rec,
	}
}

func (r *Req) SetFormValue(k, v string) *Req {
	if r.req.Form == nil {
		r.req.Form = make(url.Values)
	}
	r.req.Form.Set(k, v)
	return r
}

// Headers allows the user to set headers on the http request.
func (r *Req) Headers(k, v string, rest ...string) *Req {
	headers := append(rest, k, v)
	for i := 0; i < len(headers); i += 2 {
		if i+1 >= len(headers) {
			break
		}
		k, v := headers[i], headers[i+1]
		r.req.Header.Add(k, v)
	}
	return r
}

// WithCtx sets the ctx on the request.
func (r *Req) WithCtx(ctx context.Context) *Req {
	r.req = r.req.WithContext(ctx)
	return r
}

// WrapCtx provides means to wrap a request context. This is useful for stuffing in the
// auth stuffs that are required at times.
func (r *Req) WrapCtx(fn func(ctx context.Context) context.Context) *Req {
	return r.WithCtx(fn(r.req.Context()))
}

// Resp is a http recorder wrapper.
type Resp struct {
	t testing.TB

	debug bool

	Req *http.Request
	Rec *httptest.ResponseRecorder
}

// Debug sets the debugger. If true, the debugger will print the body of the response
// when the expected status is not received.
func (r *Resp) Debug(b bool) *Resp {
	r.debug = b
	return r
}

// Expect allows the assertions against the raw Resp.
func (r *Resp) Expect(fn func(*Resp)) *Resp {
	fn(r)
	return r
}

// ExpectStatus compares the expected status code against the recorded status code.
func (r *Resp) ExpectStatus(code int) *Resp {
	r.t.Helper()

	if r.Rec.Code != code {
		r.t.Errorf("unexpected status code: expected=%d got=%d", code, r.Rec.Code)
		if r.debug {
			r.t.Logf("body: %v", r.Rec.Body.String())
		}
	}
	return r
}

// ExpectBody provides an assertion against the recorder body.
func (r *Resp) ExpectBody(fn func(body *bytes.Buffer)) *Resp {
	fn(r.Rec.Body)
	return r
}

// ExpectHeaders asserts that multiple headers with values exist in the recorder.
func (r *Resp) ExpectHeaders(h map[string]string) *Resp {
	for k, v := range h {
		r.ExpectHeader(k, v)
	}

	return r
}

// ExpectHeader asserts that the header is in the recorder.
func (r *Resp) ExpectHeader(k, v string) *Resp {
	r.t.Helper()

	vals, ok := r.Rec.Header()[k]
	if !ok {
		r.t.Errorf("did not find expected header: %q", k)
		return r
	}

	for _, vv := range vals {
		if vv == v {
			return r
		}
	}
	r.t.Errorf("did not find expected value for header %q; got: %v", k, vals)

	return r
}

func mustEncodeJSON(t testing.TB, v interface{}) *bytes.Buffer {
	t.Helper()

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		t.Fatal(err)
	}
	return &buf
}
