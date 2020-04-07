package pkger_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb/v2"
	pcontext "github.com/influxdata/influxdb/v2/context"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/pkg/testttp"
	"github.com/influxdata/influxdb/v2/pkger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

func TestPkgerHTTPServer(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		b, err := ioutil.ReadFile(strings.TrimPrefix(r.URL.Path, "/"))
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Write(b)
	})
	filesvr := httptest.NewServer(mux)
	defer filesvr.Close()

	newPkgURL := func(t *testing.T, svrURL string, pkgPath string) string {
		t.Helper()

		u, err := url.Parse(svrURL)
		require.NoError(t, err)
		u.Path = path.Join(u.Path, pkgPath)
		return u.String()
	}

	strPtr := func(s string) *string {
		return &s
	}

	t.Run("create pkg", func(t *testing.T) {
		t.Run("should successfully return with valid req body", func(t *testing.T) {
			fakeLabelSVC := mock.NewLabelService()
			fakeLabelSVC.FindLabelByIDFn = func(ctx context.Context, id influxdb.ID) (*influxdb.Label, error) {
				return &influxdb.Label{
					ID: id,
				}, nil
			}
			svc := pkger.NewService(pkger.WithLabelSVC(fakeLabelSVC))
			pkgHandler := pkger.NewHTTPServer(zap.NewNop(), svc)
			svr := newMountedHandler(pkgHandler, 1)

			testttp.
				PostJSON(t, "/api/v2/packages", pkger.ReqCreatePkg{
					Resources: []pkger.ResourceToClone{
						{
							Kind: pkger.KindLabel,
							ID:   1,
							Name: "new name",
						},
					},
				}).
				Headers("Content-Type", "application/json").
				Do(svr).
				ExpectStatus(http.StatusOK).
				ExpectBody(func(buf *bytes.Buffer) {
					pkg, err := pkger.Parse(pkger.EncodingJSON, pkger.FromReader(buf))
					require.NoError(t, err)

					require.NotNil(t, pkg)
					require.NoError(t, pkg.Validate())

					assert.Len(t, pkg.Objects, 1)
					assert.Len(t, pkg.Summary().Labels, 1)
				})

		})

		t.Run("should be invalid if not org ids or resources provided", func(t *testing.T) {
			pkgHandler := pkger.NewHTTPServer(zap.NewNop(), nil)
			svr := newMountedHandler(pkgHandler, 1)

			testttp.
				PostJSON(t, "/api/v2/packages", pkger.ReqCreatePkg{}).
				Headers("Content-Type", "application/json").
				Do(svr).
				ExpectStatus(http.StatusUnprocessableEntity)

		})
	})

	t.Run("dry run pkg", func(t *testing.T) {
		t.Run("json", func(t *testing.T) {
			tests := []struct {
				name        string
				contentType string
				reqBody     pkger.ReqApplyPkg
			}{
				{
					name:        "app json",
					contentType: "application/json",
					reqBody: pkger.ReqApplyPkg{
						DryRun: true,
						OrgID:  influxdb.ID(9000).String(),
						RawPkg: bucketPkgKinds(t, pkger.EncodingJSON),
					},
				},
				{
					name: "defaults json when no content type",
					reqBody: pkger.ReqApplyPkg{
						DryRun: true,
						OrgID:  influxdb.ID(9000).String(),
						RawPkg: bucketPkgKinds(t, pkger.EncodingJSON),
					},
				},
				{
					name: "retrieves package from a URL",
					reqBody: pkger.ReqApplyPkg{
						DryRun: true,
						OrgID:  influxdb.ID(9000).String(),
						Remotes: []pkger.PkgRemote{{
							URL: newPkgURL(t, filesvr.URL, "testdata/remote_bucket.json"),
						}},
					},
				},
				{
					name:        "app jsonnet",
					contentType: "application/x-jsonnet",
					reqBody: pkger.ReqApplyPkg{
						DryRun: true,
						OrgID:  influxdb.ID(9000).String(),
						RawPkg: bucketPkgKinds(t, pkger.EncodingJsonnet),
					},
				},
			}

			for _, tt := range tests {
				fn := func(t *testing.T) {
					svc := &fakeSVC{
						dryRunFn: func(ctx context.Context, orgID, userID influxdb.ID, pkg *pkger.Pkg, opts ...pkger.ApplyOptFn) (pkger.Summary, pkger.Diff, error) {
							if err := pkg.Validate(); err != nil {
								return pkger.Summary{}, pkger.Diff{}, err
							}
							sum := pkg.Summary()
							var diff pkger.Diff
							for _, b := range sum.Buckets {
								diff.Buckets = append(diff.Buckets, pkger.DiffBucket{
									PkgName: b.Name,
								})
							}
							return sum, diff, nil
						},
					}

					pkgHandler := pkger.NewHTTPServer(zap.NewNop(), svc)
					svr := newMountedHandler(pkgHandler, 1)

					testttp.
						PostJSON(t, "/api/v2/packages/apply", tt.reqBody).
						Headers("Content-Type", tt.contentType).
						Do(svr).
						ExpectStatus(http.StatusOK).
						ExpectBody(func(buf *bytes.Buffer) {
							var resp pkger.RespApplyPkg
							decodeBody(t, buf, &resp)

							assert.Len(t, resp.Summary.Buckets, 1)
							assert.Len(t, resp.Diff.Buckets, 1)
						})
				}

				t.Run(tt.name, fn)
			}
		})

		t.Run("yml", func(t *testing.T) {
			tests := []struct {
				name        string
				contentType string
			}{
				{
					name:        "app yml",
					contentType: "application/x-yaml",
				},
				{
					name:        "text yml",
					contentType: "text/yml",
				},
			}

			for _, tt := range tests {
				fn := func(t *testing.T) {
					svc := &fakeSVC{
						dryRunFn: func(ctx context.Context, orgID, userID influxdb.ID, pkg *pkger.Pkg, opts ...pkger.ApplyOptFn) (pkger.Summary, pkger.Diff, error) {
							if err := pkg.Validate(); err != nil {
								return pkger.Summary{}, pkger.Diff{}, err
							}
							sum := pkg.Summary()
							var diff pkger.Diff
							for _, b := range sum.Buckets {
								diff.Buckets = append(diff.Buckets, pkger.DiffBucket{
									PkgName: b.Name,
								})
							}
							return sum, diff, nil
						},
					}

					pkgHandler := pkger.NewHTTPServer(zap.NewNop(), svc)
					svr := newMountedHandler(pkgHandler, 1)

					body := newReqApplyYMLBody(t, influxdb.ID(9000), true)

					testttp.
						Post(t, "/api/v2/packages/apply", body).
						Headers("Content-Type", tt.contentType).
						Do(svr).
						ExpectStatus(http.StatusOK).
						ExpectBody(func(buf *bytes.Buffer) {
							var resp pkger.RespApplyPkg
							decodeBody(t, buf, &resp)

							assert.Len(t, resp.Summary.Buckets, 1)
							assert.Len(t, resp.Diff.Buckets, 1)
						})
				}

				t.Run(tt.name, fn)
			}
		})

		t.Run("with multiple pkgs", func(t *testing.T) {
			newBktPkg := func(t *testing.T, bktName string) json.RawMessage {
				t.Helper()

				pkgStr := fmt.Sprintf(`[
  {
    "apiVersion": "%[1]s",
    "kind": "Bucket",
    "metadata": {
      "name": %q
    },
    "spec": {}
  }
]`, pkger.APIVersion, bktName)

				pkg, err := pkger.Parse(pkger.EncodingJSON, pkger.FromString(pkgStr))
				require.NoError(t, err)

				pkgBytes, err := pkg.Encode(pkger.EncodingJSON)
				require.NoError(t, err)
				return pkgBytes
			}

			tests := []struct {
				name         string
				reqBody      pkger.ReqApplyPkg
				expectedBkts []string
			}{
				{
					name: "retrieves package from a URL and raw pkgs",
					reqBody: pkger.ReqApplyPkg{
						DryRun: true,
						OrgID:  influxdb.ID(9000).String(),
						Remotes: []pkger.PkgRemote{{
							ContentType: "json",
							URL:         newPkgURL(t, filesvr.URL, "testdata/remote_bucket.json"),
						}},
						RawPkgs: []json.RawMessage{
							newBktPkg(t, "bkt1"),
							newBktPkg(t, "bkt2"),
							newBktPkg(t, "bkt3"),
						},
					},
					expectedBkts: []string{"bkt1", "bkt2", "bkt3", "rucket_11"},
				},
				{
					name: "retrieves packages from raw single and list",
					reqBody: pkger.ReqApplyPkg{
						DryRun: true,
						OrgID:  influxdb.ID(9000).String(),
						RawPkg: newBktPkg(t, "bkt4"),
						RawPkgs: []json.RawMessage{
							newBktPkg(t, "bkt1"),
							newBktPkg(t, "bkt2"),
							newBktPkg(t, "bkt3"),
						},
					},
					expectedBkts: []string{"bkt1", "bkt2", "bkt3", "bkt4"},
				},
			}

			for _, tt := range tests {
				fn := func(t *testing.T) {
					svc := &fakeSVC{
						dryRunFn: func(ctx context.Context, orgID, userID influxdb.ID, pkg *pkger.Pkg, opts ...pkger.ApplyOptFn) (pkger.Summary, pkger.Diff, error) {
							if err := pkg.Validate(); err != nil {
								return pkger.Summary{}, pkger.Diff{}, err
							}
							sum := pkg.Summary()
							var diff pkger.Diff
							for _, b := range sum.Buckets {
								diff.Buckets = append(diff.Buckets, pkger.DiffBucket{
									PkgName: b.Name,
								})
							}
							return sum, diff, nil
						},
					}

					pkgHandler := pkger.NewHTTPServer(zap.NewNop(), svc)
					svr := newMountedHandler(pkgHandler, 1)

					testttp.
						PostJSON(t, "/api/v2/packages/apply", tt.reqBody).
						Do(svr).
						ExpectStatus(http.StatusOK).
						ExpectBody(func(buf *bytes.Buffer) {
							var resp pkger.RespApplyPkg
							decodeBody(t, buf, &resp)

							require.Len(t, resp.Summary.Buckets, len(tt.expectedBkts))
							for i, expected := range tt.expectedBkts {
								assert.Equal(t, expected, resp.Summary.Buckets[i].Name)
							}
						})
				}

				t.Run(tt.name, fn)
			}
		})

		t.Run("validation failures", func(t *testing.T) {
			tests := []struct {
				name               string
				contentType        string
				reqBody            pkger.ReqApplyPkg
				expectedStatusCode int
			}{
				{
					name:        "invalid org id",
					contentType: "application/json",
					reqBody: pkger.ReqApplyPkg{
						DryRun: true,
						OrgID:  "bad org id",
						RawPkg: bucketPkgKinds(t, pkger.EncodingJSON),
					},
					expectedStatusCode: http.StatusBadRequest,
				},
				{
					name:        "invalid stack id",
					contentType: "application/json",
					reqBody: pkger.ReqApplyPkg{
						DryRun:  true,
						OrgID:   influxdb.ID(9000).String(),
						StackID: strPtr("invalid stack id"),
						RawPkg:  bucketPkgKinds(t, pkger.EncodingJSON),
					},
					expectedStatusCode: http.StatusBadRequest,
				},
			}

			for _, tt := range tests {
				fn := func(t *testing.T) {
					svc := &fakeSVC{
						dryRunFn: func(ctx context.Context, orgID, userID influxdb.ID, pkg *pkger.Pkg, opts ...pkger.ApplyOptFn) (pkger.Summary, pkger.Diff, error) {
							return pkg.Summary(), pkger.Diff{}, nil
						},
					}

					pkgHandler := pkger.NewHTTPServer(zap.NewNop(), svc)
					svr := newMountedHandler(pkgHandler, 1)

					testttp.
						PostJSON(t, "/api/v2/packages/apply", tt.reqBody).
						Headers("Content-Type", tt.contentType).
						Do(svr).
						ExpectStatus(tt.expectedStatusCode)
				}

				t.Run(tt.name, fn)
			}
		})
	})

	t.Run("apply a pkg", func(t *testing.T) {
		svc := &fakeSVC{
			dryRunFn: func(ctx context.Context, orgID, userID influxdb.ID, pkg *pkger.Pkg, opts ...pkger.ApplyOptFn) (pkger.Summary, pkger.Diff, error) {
				if err := pkg.Validate(); err != nil {
					return pkger.Summary{}, pkger.Diff{}, err
				}
				sum := pkg.Summary()
				var diff pkger.Diff
				for _, b := range sum.Buckets {
					diff.Buckets = append(diff.Buckets, pkger.DiffBucket{
						PkgName: b.Name,
					})
				}
				return sum, diff, nil
			},
			applyFn: func(ctx context.Context, orgID, userID influxdb.ID, pkg *pkger.Pkg, opts ...pkger.ApplyOptFn) (pkger.Summary, error) {
				var opt pkger.ApplyOpt
				for _, o := range opts {
					o(&opt)
				}
				sum := pkg.Summary()
				for key := range opt.MissingSecrets {
					sum.MissingSecrets = append(sum.MissingSecrets, key)
				}
				return sum, nil
			},
		}

		pkgHandler := pkger.NewHTTPServer(zap.NewNop(), svc)
		svr := newMountedHandler(pkgHandler, 1)

		testttp.
			PostJSON(t, "/api/v2/packages/apply", pkger.ReqApplyPkg{
				OrgID:   influxdb.ID(9000).String(),
				Secrets: map[string]string{"secret1": "val1"},
				RawPkg:  bucketPkgKinds(t, pkger.EncodingJSON),
			}).
			Do(svr).
			ExpectStatus(http.StatusCreated).
			ExpectBody(func(buf *bytes.Buffer) {
				var resp pkger.RespApplyPkg
				decodeBody(t, buf, &resp)

				assert.Len(t, resp.Summary.Buckets, 1)
				assert.Len(t, resp.Diff.Buckets, 1)
				assert.Equal(t, []string{"secret1"}, resp.Summary.MissingSecrets)
				assert.Nil(t, resp.Errors)
			})
	})

	t.Run("create a stack", func(t *testing.T) {
		t.Run("should successfully return with valid req body", func(t *testing.T) {
			svc := &fakeSVC{
				initStack: func(ctx context.Context, userID influxdb.ID, stack pkger.Stack) (pkger.Stack, error) {
					stack.ID = 3
					stack.CreatedAt = time.Now()
					stack.UpdatedAt = time.Now()
					return stack, nil
				},
			}
			pkgHandler := pkger.NewHTTPServer(zap.NewNop(), svc)
			svr := newMountedHandler(pkgHandler, 1)

			reqBody := pkger.ReqCreateStack{
				OrgID:       influxdb.ID(3).String(),
				Name:        "threeve",
				Description: "desc",
				URLs:        []string{"http://example.com"},
			}

			testttp.
				PostJSON(t, "/api/v2/packages/stacks", reqBody).
				Headers("Content-Type", "application/json").
				Do(svr).
				ExpectStatus(http.StatusCreated).
				ExpectBody(func(buf *bytes.Buffer) {
					var resp pkger.RespCreateStack
					decodeBody(t, buf, &resp)

					assert.NotZero(t, resp.ID)
					assert.Equal(t, reqBody.OrgID, resp.OrgID)
					assert.Equal(t, reqBody.Name, resp.Name)
					assert.Equal(t, reqBody.Description, resp.Description)
					assert.Equal(t, reqBody.URLs, resp.URLs)
					assert.NotZero(t, resp.CRUDLog)
				})

		})

		t.Run("error cases", func(t *testing.T) {
			tests := []struct {
				name           string
				reqBody        pkger.ReqCreateStack
				expectedStatus int
				svc            pkger.SVC
			}{
				{
					name: "bad org id",
					reqBody: pkger.ReqCreateStack{
						OrgID: "invalid id",
					},
					expectedStatus: http.StatusBadRequest,
				},
				{
					name: "bad url",
					reqBody: pkger.ReqCreateStack{
						OrgID: influxdb.ID(3).String(),
						URLs:  []string{"invalid @% url"},
					},
					expectedStatus: http.StatusBadRequest,
				},
				{
					name:    "translates svc conflict error",
					reqBody: pkger.ReqCreateStack{OrgID: influxdb.ID(3).String()},
					svc: &fakeSVC{
						initStack: func(ctx context.Context, userID influxdb.ID, stack pkger.Stack) (pkger.Stack, error) {
							return pkger.Stack{}, &influxdb.Error{Code: influxdb.EConflict}
						},
					},
					expectedStatus: http.StatusUnprocessableEntity,
				},
				{
					name:    "translates svc internal error",
					reqBody: pkger.ReqCreateStack{OrgID: influxdb.ID(3).String()},
					svc: &fakeSVC{
						initStack: func(ctx context.Context, userID influxdb.ID, stack pkger.Stack) (pkger.Stack, error) {
							return pkger.Stack{}, &influxdb.Error{Code: influxdb.EInternal}
						},
					},
					expectedStatus: http.StatusInternalServerError,
				},
			}

			for _, tt := range tests {
				fn := func(t *testing.T) {
					svc := tt.svc
					if svc == nil {
						svc = &fakeSVC{
							initStack: func(ctx context.Context, userID influxdb.ID, stack pkger.Stack) (pkger.Stack, error) {
								return stack, nil
							},
						}
					}

					pkgHandler := pkger.NewHTTPServer(zap.NewNop(), svc)
					svr := newMountedHandler(pkgHandler, 1)

					testttp.
						PostJSON(t, "/api/v2/packages/stacks", tt.reqBody).
						Headers("Content-Type", "application/json").
						Do(svr).
						ExpectStatus(tt.expectedStatus)
				}

				t.Run(tt.name, fn)
			}
		})
	})
}

func bucketPkgKinds(t *testing.T, encoding pkger.Encoding) []byte {
	t.Helper()

	var pkgStr string
	switch encoding {
	case pkger.EncodingJsonnet:
		pkgStr = `
local Bucket(name, desc) = {
    apiVersion: '%[1]s',
    kind: 'Bucket',
    metadata: {
        name: name
    },
    spec: {
        description: desc
    }
};

[
  Bucket(name="rucket_1", desc="bucket 1 description"),
]
`
	case pkger.EncodingJSON:
		pkgStr = `[
  {
    "apiVersion": "%[1]s",
    "kind": "Bucket",
    "metadata": {
      "name": "rucket_11"
    },
    "spec": {
      "description": "bucket 1 description"
    }
  }
]
`
	case pkger.EncodingYAML:
		pkgStr = `apiVersion: %[1]s
kind: Bucket
metadata:
  name:  rucket_11
spec:
  description: bucket 1 description
`
	default:
		require.FailNow(t, "invalid encoding provided: "+encoding.String())
	}

	pkg, err := pkger.Parse(encoding, pkger.FromString(fmt.Sprintf(pkgStr, pkger.APIVersion)))
	require.NoError(t, err)

	b, err := pkg.Encode(encoding)
	require.NoError(t, err)
	return b
}

func newReqApplyYMLBody(t *testing.T, orgID influxdb.ID, dryRun bool) *bytes.Buffer {
	t.Helper()

	var buf bytes.Buffer
	err := yaml.NewEncoder(&buf).Encode(pkger.ReqApplyPkg{
		DryRun: dryRun,
		OrgID:  orgID.String(),
		RawPkg: bucketPkgKinds(t, pkger.EncodingYAML),
	})
	require.NoError(t, err)
	return &buf
}

func decodeBody(t *testing.T, r io.Reader, v interface{}) {
	t.Helper()

	if err := json.NewDecoder(r).Decode(v); err != nil {
		require.FailNow(t, err.Error())
	}
}

type fakeSVC struct {
	initStack func(ctx context.Context, userID influxdb.ID, stack pkger.Stack) (pkger.Stack, error)
	dryRunFn  func(ctx context.Context, orgID, userID influxdb.ID, pkg *pkger.Pkg, opts ...pkger.ApplyOptFn) (pkger.Summary, pkger.Diff, error)
	applyFn   func(ctx context.Context, orgID, userID influxdb.ID, pkg *pkger.Pkg, opts ...pkger.ApplyOptFn) (pkger.Summary, error)
}

var _ pkger.SVC = (*fakeSVC)(nil)

func (f *fakeSVC) InitStack(ctx context.Context, userID influxdb.ID, stack pkger.Stack) (pkger.Stack, error) {
	if f.initStack == nil {
		panic("not implemented")
	}
	return f.initStack(ctx, userID, stack)
}

func (f *fakeSVC) CreatePkg(ctx context.Context, setters ...pkger.CreatePkgSetFn) (*pkger.Pkg, error) {
	panic("not implemented")
}

func (f *fakeSVC) DryRun(ctx context.Context, orgID, userID influxdb.ID, pkg *pkger.Pkg, opts ...pkger.ApplyOptFn) (pkger.Summary, pkger.Diff, error) {
	if f.dryRunFn == nil {
		panic("not implemented")
	}

	return f.dryRunFn(ctx, orgID, userID, pkg, opts...)
}

func (f *fakeSVC) Apply(ctx context.Context, orgID, userID influxdb.ID, pkg *pkger.Pkg, opts ...pkger.ApplyOptFn) (pkger.Summary, error) {
	if f.applyFn == nil {
		panic("not implemented")
	}
	return f.applyFn(ctx, orgID, userID, pkg, opts...)
}

func newMountedHandler(rh kithttp.ResourceHandler, userID influxdb.ID) chi.Router {
	r := chi.NewRouter()
	r.Mount(rh.Prefix(), authMW(userID)(rh))
	return r
}

func authMW(userID influxdb.ID) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			r = r.WithContext(pcontext.SetAuthorizer(r.Context(), &influxdb.Session{UserID: userID}))
			next.ServeHTTP(w, r)
		}
		return http.HandlerFunc(fn)
	}
}
