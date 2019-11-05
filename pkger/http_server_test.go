package pkger_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/influxdata/influxdb"
	fluxTTP "github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/pkger"
	"github.com/jsteenb2/testttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestHTTPServer(t *testing.T) {
	t.Run("create pkg", func(t *testing.T) {
		t.Run("should successfully return with valid req body", func(t *testing.T) {
			svr := pkger.NewHTTPServer(fluxTTP.ErrorHandler(0), new(pkger.Service))

			body := newReqBody(t, pkger.ReqCreatePkg{
				PkgName:        "name1",
				PkgDescription: "desc1",
				PkgVersion:     "v1",
			})

			testttp.Post("/api/v2/packages", body).
				Headers("Content-Type", "application/json").
				Do(svr).
				ExpectStatus(t, http.StatusOK).
				ExpectBody(func(buf *bytes.Buffer) {
					var resp pkger.RespCreatePkg
					decodeBody(t, buf, &resp)

					pkg := resp.Package
					assert.Equal(t, pkger.APIVersion, pkg.APIVersion)
					assert.Equal(t, "package", pkg.Kind)

					meta := pkg.Metadata
					assert.Equal(t, "name1", meta.Name)
					assert.Equal(t, "desc1", meta.Description)
					assert.Equal(t, "v1", meta.Version)

					assert.NotNil(t, pkg.Spec.Resources)
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

					svr := pkger.NewHTTPServer(fluxTTP.ErrorHandler(0), svc)

					body := newReqBody(t, pkger.ReqApplyPkg{
						DryRun: true,
						OrgID:  influxdb.ID(9000).String(),
						Pkg:    bucketPkg(t, pkger.EncodingJSON),
					})

					testttp.Post("/api/v2/packages/apply", body).
						Headers("Content-Type", tt.contentType).
						Do(svr).
						ExpectStatus(t, http.StatusOK).
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
						DryRunFn: func(ctx context.Context, orgID influxdb.ID, pkg *pkger.Pkg) (pkger.Summary, pkger.Diff, error) {
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

					svr := pkger.NewHTTPServer(fluxTTP.ErrorHandler(0), svc)

					body := newReqApplyYMLBody(t, influxdb.ID(9000), true)

					testttp.Post("/api/v2/packages/apply", body).
						Headers("Content-Type", tt.contentType).
						Do(svr).
						ExpectStatus(t, http.StatusOK).
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
	})

	t.Run("apply a pkg", func(t *testing.T) {
		svc := &fakeSVC{
			DryRunFn: func(ctx context.Context, orgID influxdb.ID, pkg *pkger.Pkg) (pkger.Summary, pkger.Diff, error) {
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

		svr := pkger.NewHTTPServer(fluxTTP.ErrorHandler(0), svc)

		body := newReqBody(t, pkger.ReqApplyPkg{
			OrgID: influxdb.ID(9000).String(),
			Pkg:   bucketPkg(t, pkger.EncodingJSON),
		})

		testttp.Post("/api/v2/packages/apply", body).
			Do(svr).
			ExpectStatus(t, http.StatusCreated).
			ExpectBody(func(buf *bytes.Buffer) {
				var resp pkger.RespApplyPkg
				decodeBody(t, buf, &resp)

				assert.Len(t, resp.Summary.Buckets, 1)
				assert.Len(t, resp.Diff.Buckets, 1)
			})
	})
}

func bucketPkg(t *testing.T, encoding pkger.Encoding) *pkger.Pkg {
	t.Helper()

	var ext string
	switch encoding {
	case pkger.EncodingJSON:
		ext = ".json"
	case pkger.EncodingYAML:
		ext = ".yml"
	default:
		require.FailNow(t, "invalid encoding provided: "+encoding.String())
	}

	pkg, err := pkger.Parse(encoding, pkger.FromFile("testdata/bucket"+ext))
	require.NoError(t, err)
	return pkg
}

func newReqApplyYMLBody(t *testing.T, orgID influxdb.ID, dryRun bool) *bytes.Buffer {
	t.Helper()

	var buf bytes.Buffer
	err := yaml.NewEncoder(&buf).Encode(pkger.ReqApplyPkg{
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
