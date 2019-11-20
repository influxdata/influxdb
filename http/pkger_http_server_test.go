package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/go-chi/chi"
	"github.com/influxdata/influxdb"
	fluxTTP "github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/pkg/testttp"
	"github.com/influxdata/influxdb/pkger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

func TestPkgerHTTPServer(t *testing.T) {
	t.Run("create pkg", func(t *testing.T) {
		t.Run("should successfully return with valid req body", func(t *testing.T) {
			fakeLabelSVC := mock.NewLabelService()
			fakeLabelSVC.FindLabelByIDFn = func(ctx context.Context, id influxdb.ID) (*influxdb.Label, error) {
				return &influxdb.Label{
					ID: id,
				}, nil
			}
			svc := pkger.NewService(zap.NewNop(), pkger.WithLabelSVC(fakeLabelSVC))
			pkgHandler := fluxTTP.NewHandlerPkg(fluxTTP.ErrorHandler(0), svc)
			svr := newMountedHandler(pkgHandler)

			body := newReqBody(t, fluxTTP.ReqCreatePkg{
				PkgName:        "name1",
				PkgDescription: "desc1",
				PkgVersion:     "v1",
				Resources: []pkger.ResourceToClone{
					{
						Kind: pkger.KindLabel,
						ID:   1,
						Name: "new name",
					},
				},
			})

			testttp.Post("/api/v2/packages", body).
				Headers("Content-Type", "application/json").
				Do(svr).
				ExpectStatus(t, http.StatusOK).
				ExpectBody(func(buf *bytes.Buffer) {
					var resp fluxTTP.RespCreatePkg
					decodeBody(t, buf, &resp)

					pkg := resp.Pkg
					require.NoError(t, pkg.Validate())
					assert.Equal(t, pkger.APIVersion, pkg.APIVersion)
					assert.Equal(t, pkger.KindPackage, pkg.Kind)

					meta := pkg.Metadata
					assert.Equal(t, "name1", meta.Name)
					assert.Equal(t, "desc1", meta.Description)
					assert.Equal(t, "v1", meta.Version)

					assert.Len(t, pkg.Spec.Resources, 1)
					assert.Len(t, pkg.Summary().Labels, 1)
				})

		})
	})

	t.Run("dry run pkg", func(t *testing.T) {
		t.Run("json", func(t *testing.T) {
			tests := []struct {
				name        string
				contentType string
			}{
				{
					name:        "app json",
					contentType: "application/json",
				},
				{
					name: "defaults json when no content type",
				},
			}

			for _, tt := range tests {
				fn := func(t *testing.T) {
					svc := &fakeSVC{
						DryRunFn: func(ctx context.Context, orgID influxdb.ID, pkg *pkger.Pkg) (pkger.Summary, pkger.Diff, error) {
							if err := pkg.Validate(); err != nil {
								return pkger.Summary{}, pkger.Diff{}, err
							}
							sum := pkg.Summary()
							var diff pkger.Diff
							for _, b := range sum.Buckets {
								diff.Buckets = append(diff.Buckets, pkger.DiffBucket{
									Name: b.Name,
								})
							}
							return sum, diff, nil
						},
					}

					pkgHandler := fluxTTP.NewHandlerPkg(fluxTTP.ErrorHandler(0), svc)
					svr := newMountedHandler(pkgHandler)

					body := newReqBody(t, fluxTTP.ReqApplyPkg{
						DryRun: true,
						OrgID:  influxdb.ID(9000).String(),
						Pkg:    bucketPkg(t, pkger.EncodingJSON),
					})

					testttp.Post("/api/v2/packages/apply", body).
						Headers("Content-Type", tt.contentType).
						Do(svr).
						ExpectStatus(t, http.StatusOK).
						ExpectBody(func(buf *bytes.Buffer) {
							var resp fluxTTP.RespApplyPkg
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
						DryRunFn: func(ctx context.Context, orgID influxdb.ID, pkg *pkger.Pkg) (pkger.Summary, pkger.Diff, error) {
							if err := pkg.Validate(); err != nil {
								return pkger.Summary{}, pkger.Diff{}, err
							}
							sum := pkg.Summary()
							var diff pkger.Diff
							for _, b := range sum.Buckets {
								diff.Buckets = append(diff.Buckets, pkger.DiffBucket{
									Name: b.Name,
								})
							}
							return sum, diff, nil
						},
					}

					pkgHandler := fluxTTP.NewHandlerPkg(fluxTTP.ErrorHandler(0), svc)
					svr := newMountedHandler(pkgHandler)

					body := newReqApplyYMLBody(t, influxdb.ID(9000), true)

					testttp.Post("/api/v2/packages/apply", body).
						Headers("Content-Type", tt.contentType).
						Do(svr).
						ExpectStatus(t, http.StatusOK).
						ExpectBody(func(buf *bytes.Buffer) {
							var resp fluxTTP.RespApplyPkg
							decodeBody(t, buf, &resp)

							assert.Len(t, resp.Summary.Buckets, 1)
							assert.Len(t, resp.Diff.Buckets, 1)
						})
				}

				t.Run(tt.name, fn)
			}
		})
	})

	t.Run("apply a pkg", func(t *testing.T) {
		svc := &fakeSVC{
			DryRunFn: func(ctx context.Context, orgID influxdb.ID, pkg *pkger.Pkg) (pkger.Summary, pkger.Diff, error) {
				if err := pkg.Validate(); err != nil {
					return pkger.Summary{}, pkger.Diff{}, err
				}
				sum := pkg.Summary()
				var diff pkger.Diff
				for _, b := range sum.Buckets {
					diff.Buckets = append(diff.Buckets, pkger.DiffBucket{
						Name: b.Name,
					})
				}
				return sum, diff, nil
			},
			ApplyFn: func(ctx context.Context, orgID influxdb.ID, pkg *pkger.Pkg) (pkger.Summary, error) {
				return pkg.Summary(), nil
			},
		}

		pkgHandler := fluxTTP.NewHandlerPkg(fluxTTP.ErrorHandler(0), svc)
		svr := newMountedHandler(pkgHandler)

		body := newReqBody(t, fluxTTP.ReqApplyPkg{
			OrgID: influxdb.ID(9000).String(),
			Pkg:   bucketPkg(t, pkger.EncodingJSON),
		})

		testttp.Post("/api/v2/packages/apply", body).
			Do(svr).
			ExpectStatus(t, http.StatusCreated).
			ExpectBody(func(buf *bytes.Buffer) {
				var resp fluxTTP.RespApplyPkg
				decodeBody(t, buf, &resp)

				assert.Len(t, resp.Summary.Buckets, 1)
				assert.Len(t, resp.Diff.Buckets, 1)
				assert.Nil(t, resp.Errors)
			})
	})
}

func bucketPkg(t *testing.T, encoding pkger.Encoding) *pkger.Pkg {
	t.Helper()

	var pkgStr string
	switch encoding {
	case pkger.EncodingJSON:
		pkgStr = `
{
  "apiVersion": "0.1.0",
  "kind": "Package",
  "meta": {
    "pkgName": "pkg_name",
    "pkgVersion": "1",
    "description": "pack description"
  },
  "spec": {
    "resources": [
      {
        "kind": "Bucket",
        "name": "rucket_11",
        "retention_period": "1h",
        "description": "bucket 1 description"
      }
    ]
  }
}
`
	case pkger.EncodingYAML:
		pkgStr = `apiVersion: 0.1.0
kind: Package
meta:
  pkgName:      pkg_name
  pkgVersion:   1
  description:  pack description
spec:
  resources:
    - kind: Bucket
      name: rucket_11
      retention_period: 1h
      description: bucket 1 description
`
	default:
		require.FailNow(t, "invalid encoding provided: "+encoding.String())
	}

	pkg, err := pkger.Parse(encoding, pkger.FromString(pkgStr))
	require.NoError(t, err)
	return pkg
}

func newReqApplyYMLBody(t *testing.T, orgID influxdb.ID, dryRun bool) *bytes.Buffer {
	t.Helper()

	var buf bytes.Buffer
	err := yaml.NewEncoder(&buf).Encode(fluxTTP.ReqApplyPkg{
		DryRun: dryRun,
		OrgID:  orgID.String(),
		Pkg:    bucketPkg(t, pkger.EncodingYAML),
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

func newReqBody(t *testing.T, v interface{}) *bytes.Buffer {
	t.Helper()

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		require.FailNow(t, "unexpected json encoding error", err)
	}
	return &buf
}

type fakeSVC struct {
	DryRunFn func(ctx context.Context, orgID influxdb.ID, pkg *pkger.Pkg) (pkger.Summary, pkger.Diff, error)
	ApplyFn  func(ctx context.Context, orgID influxdb.ID, pkg *pkger.Pkg) (pkger.Summary, error)
}

func (f *fakeSVC) CreatePkg(ctx context.Context, setters ...pkger.CreatePkgSetFn) (*pkger.Pkg, error) {
	panic("not implemented")
}

func (f *fakeSVC) DryRun(ctx context.Context, orgID influxdb.ID, pkg *pkger.Pkg) (pkger.Summary, pkger.Diff, error) {
	if f.DryRunFn == nil {
		panic("not implemented")
	}

	return f.DryRunFn(ctx, orgID, pkg)
}

func (f *fakeSVC) Apply(ctx context.Context, orgID influxdb.ID, pkg *pkger.Pkg) (pkger.Summary, error) {
	if f.ApplyFn == nil {
		panic("not implemented")
	}
	return f.ApplyFn(ctx, orgID, pkg)
}

func newMountedHandler(rh fluxTTP.ResourceHandler) chi.Router {
	r := chi.NewRouter()
	r.Mount(rh.Prefix(), rh)
	return r
}
