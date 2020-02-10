package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/kit/errors"
	"github.com/influxdata/influxdb/mock"
	"github.com/influxdata/influxdb/pkger"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCmdPkg(t *testing.T) {
	fakeSVCFn := func(svc pkger.SVC) pkgSVCsFn {
		return func() (pkger.SVC, influxdb.OrganizationService, error) {
			return svc, &mock.OrganizationService{
				FindOrganizationF: func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
					return &influxdb.Organization{ID: influxdb.ID(9000), Name: "influxdata"}, nil
				},
			}, nil
		}
	}

	setViperOptions()

	t.Run("export all", func(t *testing.T) {
		expectedOrgID := influxdb.ID(9000)

		tests := []struct {
			pkgFileArgs
		}{
			{
				pkgFileArgs: pkgFileArgs{
					name:     "yaml out with org id",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					flags: []flagArg{
						{name: "org-id", val: expectedOrgID.String()},
					},
				},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "yaml out with org name",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					flags: []flagArg{
						{name: "org", val: "influxdata"},
					},
				},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "yaml out with org name env var",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					envVars:  map[string]string{"INFLUX_ORG": "influxdata"},
				},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "yaml out with org id env var",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					envVars:  map[string]string{"INFLUX_ORG_ID": expectedOrgID.String()},
				},
			},
		}

		cmdFn := func() *cobra.Command {
			pkgSVC := &fakePkgSVC{
				createFn: func(_ context.Context, opts ...pkger.CreatePkgSetFn) (*pkger.Pkg, error) {
					opt := pkger.CreateOpt{}
					for _, o := range opts {
						if err := o(&opt); err != nil {
							return nil, err
						}
					}
					if !opt.OrgIDs[expectedOrgID] {
						return nil, errors.New("did not provide expected orgID")
					}

					var pkg pkger.Pkg
					pkg.Objects = append(pkg.Objects, pkger.Object{
						APIVersion: pkger.APIVersion,
						Type:       pkger.KindBucket,
						Metadata:   pkger.Resource{"name": "bucket1"},
					})
					return &pkg, nil
				},
			}
			builder := newCmdPkgBuilder(fakeSVCFn(pkgSVC), in(new(bytes.Buffer)))
			return builder.cmdPkgExportAll()
		}
		for _, tt := range tests {
			testPkgWrites(t, cmdFn, tt.pkgFileArgs, func(t *testing.T, pkg *pkger.Pkg) {
				sum := pkg.Summary()

				require.Len(t, sum.Buckets, 1)
				assert.Equal(t, "bucket1", sum.Buckets[0].Name)
			})
		}
	})

	t.Run("export resources", func(t *testing.T) {
		tests := []struct {
			name string
			pkgFileArgs
			bucketIDs   []influxdb.ID
			dashIDs     []influxdb.ID
			endpointIDs []influxdb.ID
			labelIDs    []influxdb.ID
			ruleIDs     []influxdb.ID
			taskIDs     []influxdb.ID
			telegrafIDs []influxdb.ID
			varIDs      []influxdb.ID
		}{
			{
				pkgFileArgs: pkgFileArgs{
					name:     "buckets",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
				},
				bucketIDs: []influxdb.ID{1, 2},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "dashboards",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
				},
				dashIDs: []influxdb.ID{1, 2},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "endpoints",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
				},
				endpointIDs: []influxdb.ID{1, 2},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "labels",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
				},
				labelIDs: []influxdb.ID{1, 2},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "rules",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
				},
				ruleIDs: []influxdb.ID{1, 2},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "tasks",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
				},
				taskIDs: []influxdb.ID{1, 2},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "telegrafs",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
				},
				telegrafIDs: []influxdb.ID{1, 2},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "variables",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
				},
				varIDs: []influxdb.ID{1, 2},
			},
		}

		cmdFn := func() *cobra.Command {
			pkgSVC := &fakePkgSVC{
				createFn: func(_ context.Context, opts ...pkger.CreatePkgSetFn) (*pkger.Pkg, error) {
					var opt pkger.CreateOpt
					for _, o := range opts {
						if err := o(&opt); err != nil {
							return nil, err
						}
					}

					var pkg pkger.Pkg
					for _, rc := range opt.Resources {
						if rc.Kind == pkger.KindNotificationEndpoint {
							rc.Kind = pkger.KindNotificationEndpointHTTP
						}
						name := rc.Kind.String() + strconv.Itoa(int(rc.ID))
						pkg.Objects = append(pkg.Objects, pkger.Object{
							APIVersion: pkger.APIVersion,
							Type:       rc.Kind,
							Metadata:   pkger.Resource{"name": name},
						})
					}

					return &pkg, nil
				},
			}
			builder := newCmdPkgBuilder(fakeSVCFn(pkgSVC), in(new(bytes.Buffer)))
			return builder.cmdPkgExport()
		}
		for _, tt := range tests {
			tt.flags = append(tt.flags,
				flagArg{"buckets", idsStr(tt.bucketIDs...)},
				flagArg{"endpoints", idsStr(tt.endpointIDs...)},
				flagArg{"dashboards", idsStr(tt.dashIDs...)},
				flagArg{"labels", idsStr(tt.labelIDs...)},
				flagArg{"rules", idsStr(tt.ruleIDs...)},
				flagArg{"tasks", idsStr(tt.taskIDs...)},
				flagArg{"telegraf-configs", idsStr(tt.telegrafIDs...)},
				flagArg{"variables", idsStr(tt.varIDs...)},
			)

			testPkgWrites(t, cmdFn, tt.pkgFileArgs, func(t *testing.T, pkg *pkger.Pkg) {
				sum := pkg.Summary()

				require.Len(t, sum.Buckets, len(tt.bucketIDs))
				for i, id := range tt.bucketIDs {
					actual := sum.Buckets[i]
					assert.Equal(t, pkger.KindBucket.String()+strconv.Itoa(int(id)), actual.Name)
				}
				require.Len(t, sum.Dashboards, len(tt.dashIDs))
				for i, id := range tt.dashIDs {
					actual := sum.Dashboards[i]
					assert.Equal(t, pkger.KindDashboard.String()+strconv.Itoa(int(id)), actual.Name)
				}
				require.Len(t, sum.NotificationEndpoints, len(tt.endpointIDs))
				for i, id := range tt.endpointIDs {
					actual := sum.NotificationEndpoints[i]
					assert.Equal(t, pkger.KindNotificationEndpointHTTP.String()+strconv.Itoa(int(id)), actual.NotificationEndpoint.GetName())
				}
				require.Len(t, sum.Labels, len(tt.labelIDs))
				for i, id := range tt.labelIDs {
					actual := sum.Labels[i]
					assert.Equal(t, pkger.KindLabel.String()+strconv.Itoa(int(id)), actual.Name)
				}
				require.Len(t, sum.NotificationRules, len(tt.ruleIDs))
				for i, id := range tt.ruleIDs {
					actual := sum.NotificationRules[i]
					assert.Equal(t, pkger.KindNotificationRule.String()+strconv.Itoa(int(id)), actual.Name)
				}
				require.Len(t, sum.Tasks, len(tt.taskIDs))
				for i, id := range tt.taskIDs {
					actual := sum.Tasks[i]
					assert.Equal(t, pkger.KindTask.String()+strconv.Itoa(int(id)), actual.Name)
				}
				require.Len(t, sum.TelegrafConfigs, len(tt.telegrafIDs))
				for i, id := range tt.telegrafIDs {
					actual := sum.TelegrafConfigs[i]
					assert.Equal(t, pkger.KindTelegraf.String()+strconv.Itoa(int(id)), actual.TelegrafConfig.Name)
				}
				require.Len(t, sum.Variables, len(tt.varIDs))
				for i, id := range tt.varIDs {
					actual := sum.Variables[i]
					assert.Equal(t, pkger.KindVariable.String()+strconv.Itoa(int(id)), actual.Name)
				}
			})
		}
	})

	t.Run("validate", func(t *testing.T) {
		t.Run("pkg is valid returns no error", func(t *testing.T) {
			cmd := newCmdPkgBuilder(fakeSVCFn(new(fakePkgSVC))).cmdPkgValidate()
			cmd.SetArgs([]string{
				"--file=../../pkger/testdata/bucket.yml",
				"-f=../../pkger/testdata/label.yml",
			})
			require.NoError(t, cmd.Execute())
		})

		t.Run("pkg is invalid returns error", func(t *testing.T) {
			// pkgYml is invalid because it is missing a name and wrong apiVersion
			const pkgYml = `apiVersion: 0.1.0
kind: Bucket
metadata:
`
			b := newCmdPkgBuilder(fakeSVCFn(new(fakePkgSVC)), in(strings.NewReader(pkgYml)), out(ioutil.Discard))
			cmd := b.cmdPkgValidate()
			require.Error(t, cmd.Execute())
		})
	})
}

type flagArg struct{ name, val string }

func (s flagArg) String() string {
	return fmt.Sprintf("--%s=%s", s.name, s.val)
}

func flagArgs(fArgs ...flagArg) []string {
	var args []string
	for _, f := range fArgs {
		args = append(args, f.String())
	}
	return args
}

type pkgFileArgs struct {
	name     string
	filename string
	encoding pkger.Encoding
	flags    []flagArg
	envVars  map[string]string
}

func testPkgWrites(t *testing.T, newCmdFn func() *cobra.Command, args pkgFileArgs, assertFn func(t *testing.T, pkg *pkger.Pkg)) {
	t.Helper()

	defer addEnvVars(t, args.envVars)()

	wrappedCmdFn := func() *cobra.Command {
		cmd := newCmdFn()
		cmd.SetArgs([]string{}) // clears mess from test runner coming into cobra cli via stdin
		return cmd
	}

	t.Run(path.Join(args.name, "file"), testPkgWritesFile(wrappedCmdFn, args, assertFn))
	t.Run(path.Join(args.name, "buffer"), testPkgWritesToBuffer(wrappedCmdFn, args, assertFn))
}

func testPkgWritesFile(newCmdFn func() *cobra.Command, args pkgFileArgs, assertFn func(t *testing.T, pkg *pkger.Pkg)) func(t *testing.T) {
	return func(t *testing.T) {
		t.Helper()

		tempDir := newTempDir(t)
		defer os.RemoveAll(tempDir)

		pathToFile := filepath.Join(tempDir, args.filename)

		cmd := newCmdFn()
		cmd.SetArgs(flagArgs(append(args.flags, flagArg{name: "file", val: pathToFile})...))

		require.NoError(t, cmd.Execute())

		pkg, err := pkger.Parse(args.encoding, pkger.FromFile(pathToFile), pkger.ValidWithoutResources(), pkger.ValidSkipParseError())
		require.NoError(t, err)

		assertFn(t, pkg)
	}
}

func testPkgWritesToBuffer(newCmdFn func() *cobra.Command, args pkgFileArgs, assertFn func(t *testing.T, pkg *pkger.Pkg)) func(t *testing.T) {
	return func(t *testing.T) {
		t.Helper()

		var buf bytes.Buffer
		cmd := newCmdFn()
		cmd.SetOut(&buf)
		cmd.SetArgs(flagArgs(args.flags...))

		require.NoError(t, cmd.Execute())

		pkg, err := pkger.Parse(pkger.EncodingYAML, pkger.FromReader(&buf), pkger.ValidWithoutResources(), pkger.ValidSkipParseError())
		require.NoError(t, err)

		assertFn(t, pkg)
	}
}

type fakePkgSVC struct {
	createFn func(ctx context.Context, setters ...pkger.CreatePkgSetFn) (*pkger.Pkg, error)
	dryRunFn func(ctx context.Context, orgID, userID influxdb.ID, pkg *pkger.Pkg) (pkger.Summary, pkger.Diff, error)
	applyFn  func(ctx context.Context, orgID, userID influxdb.ID, pkg *pkger.Pkg, opts ...pkger.ApplyOptFn) (pkger.Summary, error)
}

func (f *fakePkgSVC) CreatePkg(ctx context.Context, setters ...pkger.CreatePkgSetFn) (*pkger.Pkg, error) {
	if f.createFn != nil {
		return f.createFn(ctx, setters...)
	}
	panic("not implemented")
}

func (f *fakePkgSVC) DryRun(ctx context.Context, orgID, userID influxdb.ID, pkg *pkger.Pkg, opts ...pkger.ApplyOptFn) (pkger.Summary, pkger.Diff, error) {
	if f.dryRunFn != nil {
		return f.dryRunFn(ctx, orgID, userID, pkg)
	}
	panic("not implemented")
}

func (f *fakePkgSVC) Apply(ctx context.Context, orgID, userID influxdb.ID, pkg *pkger.Pkg, opts ...pkger.ApplyOptFn) (pkger.Summary, error) {
	if f.applyFn != nil {
		return f.applyFn(ctx, orgID, userID, pkg, opts...)
	}
	panic("not implemented")
}

func newTempDir(t *testing.T) string {
	t.Helper()

	tempDir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	return tempDir
}

func idsStr(ids ...influxdb.ID) string {
	var idStrs []string
	for _, id := range ids {
		idStrs = append(idStrs, id.String())
	}
	return strings.Join(idStrs, ",")
}
