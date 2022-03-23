package httpc

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	newClient := func(t *testing.T, addr string, opts ...ClientOptFn) *Client {
		t.Helper()
		client, err := New(append(opts, WithAddr(addr))...)
		require.NoError(t, err)
		return client
	}

	type (
		respFn func(status int, req *http.Request) (resp *http.Response, err error)

		authFn func(status int, respFn respFn, opts ...ClientOptFn) (*Client, *fakeDoer)

		newReqFn func(*Client, string, reqBody) *Req

		testCase struct {
			method      string
			status      int
			clientOpts  []ClientOptFn
			reqFn       newReqFn
			queryParams [][2]string
			reqBody     reqBody
		}
	)

	tokenAuthClient := func(status int, respFn respFn, opts ...ClientOptFn) (*Client, *fakeDoer) {
		const token = "secrettoken"
		fakeDoer := &fakeDoer{
			doFn: func(r *http.Request) (*http.Response, error) {
				if r.Header.Get("Authorization") != "Token "+token {
					return nil, errors.New("unauthed token")
				}
				return respFn(status, r)
			},
		}
		client := newClient(t, "http://example.com", append(opts, WithAuthToken(token))...)
		client.doer = fakeDoer
		return client, fakeDoer
	}

	cookieAuthClient := func(status int, respFn respFn, opts ...ClientOptFn) (*Client, *fakeDoer) {
		const session = "secret"
		fakeDoer := &fakeDoer{
			doFn: func(r *http.Request) (*http.Response, error) {
				cookie, err := r.Cookie("session")
				if err != nil {
					return nil, errors.New("session cookie not found")
				}
				if cookie.Value != session {
					return nil, errors.New("unauthed cookie")
				}
				return respFn(status, r)
			},
		}
		client := newClient(t, "http://example.com", append(opts, WithSessionCookie(session))...)
		client.doer = fakeDoer
		return client, fakeDoer
	}

	noAuthClient := func(status int, respFn respFn, opts ...ClientOptFn) (*Client, *fakeDoer) {
		fakeDoer := &fakeDoer{
			doFn: func(r *http.Request) (*http.Response, error) {
				return respFn(status, r)
			},
		}
		client := newClient(t, "http://example.com", opts...)
		client.doer = fakeDoer
		return client, fakeDoer
	}

	authTests := []struct {
		name     string
		clientFn authFn
	}{
		{
			name:     "no auth",
			clientFn: noAuthClient,
		},
		{
			name:     "token auth",
			clientFn: tokenAuthClient,
		},
		{
			name:     "cookie auth",
			clientFn: cookieAuthClient,
		},
	}

	encodingTests := []struct {
		name     string
		respFn   respFn
		decodeFn func(v interface{}) func(r *Req) *Req
	}{
		{
			name:   "json response",
			respFn: stubRespNJSONBody,
			decodeFn: func(v interface{}) func(r *Req) *Req {
				return func(r *Req) *Req { return r.DecodeJSON(v) }
			},
		},
		{
			name:   "gzipped json response",
			respFn: stubRespNGZippedJSON,
			decodeFn: func(v interface{}) func(r *Req) *Req {
				return func(r *Req) *Req { return r.DecodeJSON(v) }
			},
		},
		{
			name:   "gob response",
			respFn: stubRespNGobBody,
			decodeFn: func(v interface{}) func(r *Req) *Req {
				return func(r *Req) *Req { return r.DecodeGob(v) }
			},
		},
	}

	testWithRespBody := func(tt testCase) func(t *testing.T) {
		return func(t *testing.T) {
			t.Helper()

			for _, encTest := range encodingTests {
				t.Run(encTest.name, func(t *testing.T) {
					t.Helper()

					for _, authTest := range authTests {
						fn := func(t *testing.T) {
							t.Helper()
							client, fakeDoer := authTest.clientFn(tt.status, encTest.respFn, tt.clientOpts...)

							req := tt.reqFn(client, "/new/path/heres", tt.reqBody).
								Accept("application/json").
								Header("X-Code", "Code").
								QueryParams(tt.queryParams...).
								StatusFn(StatusIn(tt.status))

							var actual echoResp
							req = encTest.decodeFn(&actual)(req)

							err := req.Do(context.TODO())
							require.NoError(t, err)

							expectedResp := echoResp{
								Method:  tt.method,
								Scheme:  "http",
								Host:    "example.com",
								Path:    "/new/path/heres",
								Queries: tt.queryParams,
								ReqBody: tt.reqBody,
							}
							assert.Equal(t, expectedResp, actual)
							require.Len(t, fakeDoer.args, 1)
							assert.Equal(t, "application/json", fakeDoer.args[0].Header.Get("Accept"))
							assert.Equal(t, "Code", fakeDoer.args[0].Header.Get("X-Code"))
						}
						t.Run(authTest.name, fn)
					}
				})
			}
		}
	}

	newGet := func(client *Client, urlPath string, _ reqBody) *Req {
		return client.Get(urlPath)
	}

	t.Run("Delete", func(t *testing.T) {
		for _, authTest := range authTests {
			fn := func(t *testing.T) {
				client, fakeDoer := authTest.clientFn(204, stubResp)

				err := client.Delete("/new/path/heres").
					Header("X-Code", "Code").
					StatusFn(StatusIn(204)).
					Do(context.TODO())
				require.NoError(t, err)

				require.Len(t, fakeDoer.args, 1)
				assert.Equal(t, "Code", fakeDoer.args[0].Header.Get("X-Code"))
			}
			t.Run(authTest.name, fn)
		}
	})

	t.Run("Get", func(t *testing.T) {
		tests := []struct {
			name string
			testCase
		}{
			{
				name: "handles basic call",
				testCase: testCase{
					status: 200,
				},
			},
			{
				name: "handles query values",
				testCase: testCase{
					queryParams: [][2]string{{"q1", "v1"}, {"q2", "v2"}},
					status:      202,
				},
			},
		}

		for _, tt := range tests {
			tt.method = "GET"
			tt.reqFn = newGet

			t.Run(tt.name, testWithRespBody(tt.testCase))
		}
	})

	t.Run("Patch Post Put with request bodies", func(t *testing.T) {
		methods := []struct {
			name         string
			methodCallFn func(client *Client, urlPath string, bFn BodyFn) *Req
		}{
			{
				name: "PATCH",
				methodCallFn: func(client *Client, urlPath string, bFn BodyFn) *Req {
					return client.Patch(bFn, urlPath)
				},
			},
			{
				name: "POST",
				methodCallFn: func(client *Client, urlPath string, bFn BodyFn) *Req {
					return client.Post(bFn, urlPath)
				},
			},
			{
				name: "PUT",
				methodCallFn: func(client *Client, urlPath string, bFn BodyFn) *Req {
					return client.Put(bFn, urlPath)
				},
			},
		}

		for _, method := range methods {
			t.Run(method.name, func(t *testing.T) {
				tests := []struct {
					name string
					testCase
				}{
					{
						name: "handles json req body",
						testCase: testCase{
							status: 200,
							reqFn: func(client *Client, urlPath string, body reqBody) *Req {
								return method.methodCallFn(client, urlPath, BodyJSON(body))
							},
							reqBody: reqBody{
								Foo: "foo 1",
								Bar: 31,
							},
						},
					},
					{
						name: "handles gob req body",
						testCase: testCase{
							status: 201,
							reqFn: func(client *Client, urlPath string, body reqBody) *Req {
								return method.methodCallFn(client, urlPath, BodyGob(body))
							},
							reqBody: reqBody{
								Foo: "foo 1",
								Bar: 31,
							},
						},
					},
					{
						name: "handles gzipped json req body",
						testCase: testCase{
							status:     201,
							clientOpts: []ClientOptFn{WithWriterGZIP()},
							reqFn: func(client *Client, urlPath string, body reqBody) *Req {
								return method.methodCallFn(client, urlPath, BodyJSON(body))
							},
							reqBody: reqBody{
								Foo: "foo",
								Bar: 31,
							},
						},
					},
				}

				for _, tt := range tests {
					tt.method = method.name

					t.Run(tt.name, testWithRespBody(tt.testCase))
				}
			})
		}
	})

	t.Run("PatchJSON PostJSON PutJSON with request bodies", func(t *testing.T) {
		methods := []struct {
			name         string
			methodCallFn func(client *Client, urlPath string, v interface{}) *Req
		}{
			{
				name: "PATCH",
				methodCallFn: func(client *Client, urlPath string, v interface{}) *Req {
					return client.PatchJSON(v, urlPath)
				},
			},
			{
				name: "POST",
				methodCallFn: func(client *Client, urlPath string, v interface{}) *Req {
					return client.PostJSON(v, urlPath)
				},
			},
			{
				name: "PUT",
				methodCallFn: func(client *Client, urlPath string, v interface{}) *Req {
					return client.PutJSON(v, urlPath)
				},
			},
		}

		for _, method := range methods {
			t.Run(method.name, func(t *testing.T) {
				tests := []struct {
					name string
					testCase
				}{
					{
						name: "handles json req body",
						testCase: testCase{
							status: 200,
							reqFn: func(client *Client, urlPath string, body reqBody) *Req {
								return method.methodCallFn(client, urlPath, body)
							},
							reqBody: reqBody{
								Foo: "foo 1",
								Bar: 31,
							},
						},
					},
				}

				for _, tt := range tests {
					tt.method = method.name

					t.Run(tt.name, testWithRespBody(tt.testCase))
				}
			})
		}
	})
}

type fakeDoer struct {
	doFn      func(*http.Request) (*http.Response, error)
	args      []*http.Request
	callCount int
}

func (f *fakeDoer) Do(r *http.Request) (*http.Response, error) {
	f.callCount++
	f.args = append(f.args, r)
	return f.doFn(r)
}

func stubResp(status int, _ *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(new(bytes.Buffer)),
	}, nil
}

func stubRespNGZippedJSON(status int, r *http.Request) (*http.Response, error) {
	e, err := decodeFromContentType(r)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	defer w.Close()
	if err := json.NewEncoder(w).Encode(e); err != nil {
		return nil, err
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}

	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(&buf),
		Header: http.Header{
			"Content-Encoding": []string{"gzip"},
			headerContentType:  []string{"application/json"},
		},
	}, nil
}

func stubRespNJSONBody(status int, r *http.Request) (*http.Response, error) {
	e, err := decodeFromContentType(r)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(e); err != nil {
		return nil, err
	}

	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(&buf),
		Header:     http.Header{headerContentType: []string{"application/json"}},
	}, nil
}

func stubRespNGobBody(status int, r *http.Request) (*http.Response, error) {
	e, err := decodeFromContentType(r)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(e); err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(&buf),
		Header:     http.Header{headerContentEncoding: []string{"application/gob"}},
	}, nil
}

type (
	reqBody struct {
		Foo string
		Bar int
	}

	echoResp struct {
		Method  string
		Scheme  string
		Host    string
		Path    string
		Queries [][2]string

		ReqBody reqBody
	}
)

func decodeFromContentType(r *http.Request) (echoResp, error) {
	e := echoResp{
		Method: r.Method,
		Scheme: r.URL.Scheme,
		Host:   r.URL.Host,
		Path:   r.URL.Path,
	}
	for key, vals := range r.URL.Query() {
		for _, v := range vals {
			e.Queries = append(e.Queries, [2]string{key, v})
		}
	}
	sort.Slice(e.Queries, func(i, j int) bool {
		qi, qj := e.Queries[i], e.Queries[j]
		if qi[0] == qj[0] {
			return qi[1] < qj[1]
		}
		return qi[0] < qj[0]
	})

	var reader io.Reader = r.Body
	if r.Header.Get(headerContentEncoding) == "gzip" {
		gr, err := gzip.NewReader(reader)
		if err != nil {
			return echoResp{}, err
		}
		reader = gr
	}

	if r.Header.Get(headerContentEncoding) == "application/gob" {
		return e, gob.NewDecoder(reader).Decode(&e.ReqBody)
	}

	if r.Header.Get(headerContentType) == "application/json" {
		return e, json.NewDecoder(reader).Decode(&e.ReqBody)
	}

	return e, nil
}
