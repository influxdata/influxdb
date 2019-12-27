package main

import (
	"bytes"
	"context"
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

func Test_Pkg(t *testing.T) {
	fakeSVCFn := func(svc pkger.SVC) pkgSVCsFn {
		return func() (pkger.SVC, influxdb.OrganizationService, error) {
			return svc, &mock.OrganizationService{
				FindOrganizationF: func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
					return &influxdb.Organization{ID: influxdb.ID(9000), Name: "influxdata"}, nil
				},
			}, nil
		}
	}

	hasNotZeroDefault := func(t *testing.T, expected, actual string) {
		t.Helper()
		if expected == "" {
			assert.NotZero(t, actual)
			return
		}
		assert.Equal(t, expected, actual)
	}

	t.Run("new", func(t *testing.T) {
		tests := []struct {
			expectedMeta pkger.Metadata
			args         pkgFileArgs
		}{
			{
				args: pkgFileArgs{
					name:     "yaml out",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					flags: []flagArg{
						{name: "name", val: "new name"},
						{name: "description", val: "new desc"},
						{name: "version", val: "new version"},
					},
				},
				expectedMeta: pkger.Metadata{
					Name:        "new name",
					Description: "new desc",
					Version:     "new version",
				},
			},
			{
				args: pkgFileArgs{
					name:     "json out",
					encoding: pkger.EncodingJSON,
					filename: "pkg_1.json",
					flags: []flagArg{
						{name: "name", val: "new name"},
						{name: "description", val: "new desc"},
						{name: "version", val: "new version"},
					},
				},
				expectedMeta: pkger.Metadata{
					Name:        "new name",
					Description: "new desc",
					Version:     "new version",
				},
			},
			{
				args: pkgFileArgs{
					name:     "quiet mode",
					encoding: pkger.EncodingJSON,
					filename: "pkg_1.json",
					flags: []flagArg{
						{name: "quiet", val: "true"},
					},
				},
			},
		}

		cmdFn := func() *cobra.Command {
			builder := newCmdPkgBuilder(fakeSVCFn(pkger.NewService()), in(new(bytes.Buffer)))
			cmd := builder.cmdPkgNew()
			return cmd
		}

		for _, tt := range tests {
			testPkgWrites(t, cmdFn, tt.args, func(t *testing.T, pkg *pkger.Pkg) {
				assert.Equal(t, tt.expectedMeta.Description, pkg.Metadata.Description)
				hasNotZeroDefault(t, tt.expectedMeta.Name, pkg.Metadata.Name)
				hasNotZeroDefault(t, tt.expectedMeta.Version, pkg.Metadata.Version)
			})
		}
	})

	t.Run("export all", func(t *testing.T) {
		expectedOrgID := influxdb.ID(9000)

		tests := []struct {
			pkgFileArgs
			expectedMeta pkger.Metadata
		}{
			{
				pkgFileArgs: pkgFileArgs{
					name:     "yaml out with org id",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					flags: []flagArg{
						{name: "name", val: "new name"},
						{name: "description", val: "new desc"},
						{name: "version", val: "new version"},
						{name: "org-id", val: expectedOrgID.String()},
					},
				},
				expectedMeta: pkger.Metadata{
					Name:        "new name",
					Description: "new desc",
					Version:     "new version",
				},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "yaml out with org name",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					flags: []flagArg{
						{name: "name", val: "new name"},
						{name: "description", val: "new desc"},
						{name: "version", val: "new version"},
						{name: "org", val: "influxdata"},
					},
				},
				expectedMeta: pkger.Metadata{
					Name:        "new name",
					Description: "new desc",
					Version:     "new version",
				},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "yaml out with org name env var",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					flags: []flagArg{
						{name: "name", val: "new name"},
						{name: "description", val: "new desc"},
						{name: "version", val: "new version"},
					},
					envVars: []struct{ key, val string }{{key: "ORG", val: "influxdata"}},
				},
				expectedMeta: pkger.Metadata{
					Name:        "new name",
					Description: "new desc",
					Version:     "new version",
				},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "yaml out with org id env var",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					flags: []flagArg{
						{name: "name", val: "new name"},
						{name: "description", val: "new desc"},
						{name: "version", val: "new version"},
					},
					envVars: []struct{ key, val string }{{key: "ORG_ID", val: expectedOrgID.String()}},
				},
				expectedMeta: pkger.Metadata{
					Name:        "new name",
					Description: "new desc",
					Version:     "new version",
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

					pkg := pkger.Pkg{
						APIVersion: pkger.APIVersion,
						Kind:       pkger.KindPackage,
						Metadata:   opt.Metadata,
					}
					pkg.Spec.Resources = append(pkg.Spec.Resources, pkger.Resource{
						"name": "bucket1",
						"kind": pkger.KindBucket.String(),
					})
					return &pkg, nil
				},
			}
			builder := newCmdPkgBuilder(fakeSVCFn(pkgSVC), in(new(bytes.Buffer)))
			return builder.cmdPkgExportAll()
		}
		for _, tt := range tests {
			testPkgWrites(t, cmdFn, tt.pkgFileArgs, func(t *testing.T, pkg *pkger.Pkg) {
				assert.Equal(t, tt.expectedMeta.Description, pkg.Metadata.Description)
				hasNotZeroDefault(t, tt.expectedMeta.Name, pkg.Metadata.Name)
				hasNotZeroDefault(t, tt.expectedMeta.Version, pkg.Metadata.Version)

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
			bucketIDs    []influxdb.ID
			dashIDs      []influxdb.ID
			endpointIDs  []influxdb.ID
			labelIDs     []influxdb.ID
			ruleIDs      []influxdb.ID
			taskIDs      []influxdb.ID
			telegrafIDs  []influxdb.ID
			varIDs       []influxdb.ID
			expectedMeta pkger.Metadata
		}{
			{
				pkgFileArgs: pkgFileArgs{
					name:     "buckets",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					flags: []flagArg{
						{name: "name", val: "new name"},
						{name: "description", val: "new desc"},
						{name: "version", val: "new version"},
					},
				},
				bucketIDs: []influxdb.ID{1, 2},
				expectedMeta: pkger.Metadata{
					Name:        "new name",
					Description: "new desc",
					Version:     "new version",
				},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "dashboards",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					flags: []flagArg{
						{name: "name", val: "new name"},
						{name: "description", val: "new desc"},
						{name: "version", val: "new version"},
					},
				},
				dashIDs: []influxdb.ID{1, 2},
				expectedMeta: pkger.Metadata{
					Name:        "new name",
					Description: "new desc",
					Version:     "new version",
				},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "endpoints",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					flags: []flagArg{
						{name: "name", val: "new name"},
						{name: "description", val: "new desc"},
						{name: "version", val: "new version"},
					},
				},
				endpointIDs: []influxdb.ID{1, 2},
				expectedMeta: pkger.Metadata{
					Name:        "new name",
					Description: "new desc",
					Version:     "new version",
				},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "labels",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					flags: []flagArg{
						{name: "name", val: "new name"},
						{name: "description", val: "new desc"},
						{name: "version", val: "new version"},
					},
				},
				labelIDs: []influxdb.ID{1, 2},
				expectedMeta: pkger.Metadata{
					Name:        "new name",
					Description: "new desc",
					Version:     "new version",
				},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "rules",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					flags: []flagArg{
						{name: "name", val: "new name"},
						{name: "description", val: "new desc"},
						{name: "version", val: "new version"},
					},
				},
				ruleIDs: []influxdb.ID{1, 2},
				expectedMeta: pkger.Metadata{
					Name:        "new name",
					Description: "new desc",
					Version:     "new version",
				},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "tasks",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					flags: []flagArg{
						{name: "name", val: "new name"},
						{name: "description", val: "new desc"},
						{name: "version", val: "new version"},
					},
				},
				taskIDs: []influxdb.ID{1, 2},
				expectedMeta: pkger.Metadata{
					Name:        "new name",
					Description: "new desc",
					Version:     "new version",
				},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "telegrafs",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					flags: []flagArg{
						{name: "name", val: "new name"},
						{name: "description", val: "new desc"},
						{name: "version", val: "new version"},
					},
				},
				telegrafIDs: []influxdb.ID{1, 2},
				expectedMeta: pkger.Metadata{
					Name:        "new name",
					Description: "new desc",
					Version:     "new version",
				},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "variables",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					flags: []flagArg{
						{name: "name", val: "new name"},
						{name: "description", val: "new desc"},
						{name: "version", val: "new version"},
					},
				},
				varIDs: []influxdb.ID{1, 2},
				expectedMeta: pkger.Metadata{
					Name:        "new name",
					Description: "new desc",
					Version:     "new version",
				},
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

					pkg := pkger.Pkg{
						APIVersion: pkger.APIVersion,
						Kind:       pkger.KindPackage,
						Metadata:   opt.Metadata,
					}
					for _, rc := range opt.Resources {
						if rc.Kind == pkger.KindNotificationEndpoint {
							rc.Kind = pkger.KindNotificationEndpointHTTP
						}
						name := rc.Kind.String() + strconv.Itoa(int(rc.ID))
						pkg.Spec.Resources = append(pkg.Spec.Resources, pkger.Resource{
							"kind": rc.Kind,
							"name": name,
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
				assert.Equal(t, tt.expectedMeta.Description, pkg.Metadata.Description)
				hasNotZeroDefault(t, tt.expectedMeta.Name, pkg.Metadata.Name)
				hasNotZeroDefault(t, tt.expectedMeta.Version, pkg.Metadata.Version)

				sum := pkg.Summary()

				require.Len(t, sum.Buckets, len(tt.bucketIDs))
				for i, id := range tt.bucketIDs {
					actual := sum.Buckets[i]
					assert.Equal(t, "bucket"+strconv.Itoa(int(id)), actual.Name)
				}
				require.Len(t, sum.Dashboards, len(tt.dashIDs))
				for i, id := range tt.dashIDs {
					actual := sum.Dashboards[i]
					assert.Equal(t, "dashboard"+strconv.Itoa(int(id)), actual.Name)
				}
				require.Len(t, sum.NotificationEndpoints, len(tt.endpointIDs))
				for i, id := range tt.endpointIDs {
					actual := sum.NotificationEndpoints[i]
					assert.Equal(t, "notification_endpoint_http"+strconv.Itoa(int(id)), actual.NotificationEndpoint.GetName())
				}
				require.Len(t, sum.Labels, len(tt.labelIDs))
				for i, id := range tt.labelIDs {
					actual := sum.Labels[i]
					assert.Equal(t, "label"+strconv.Itoa(int(id)), actual.Name)
				}
				require.Len(t, sum.NotificationRules, len(tt.ruleIDs))
				for i, id := range tt.ruleIDs {
					actual := sum.NotificationRules[i]
					assert.Equal(t, "notification_rule"+strconv.Itoa(int(id)), actual.Name)
				}
				require.Len(t, sum.Tasks, len(tt.taskIDs))
				for i, id := range tt.taskIDs {
					actual := sum.Tasks[i]
					assert.Equal(t, "task"+strconv.Itoa(int(id)), actual.Name)
				}
				require.Len(t, sum.TelegrafConfigs, len(tt.telegrafIDs))
				for i, id := range tt.telegrafIDs {
					actual := sum.TelegrafConfigs[i]
					assert.Equal(t, "telegraf"+strconv.Itoa(int(id)), actual.TelegrafConfig.Name)
				}
				require.Len(t, sum.Variables, len(tt.varIDs))
				for i, id := range tt.varIDs {
					actual := sum.Variables[i]
					assert.Equal(t, "variable"+strconv.Itoa(int(id)), actual.Name)
				}
			})
		}
	})

	t.Run("validate", func(t *testing.T) {
		t.Run("pkg is valid returns no error", func(t *testing.T) {
			cmd := newCmdPkgBuilder(fakeSVCFn(new(fakePkgSVC))).cmdPkgValidate()
			require.NoError(t, cmd.Flags().Set("file", "../../pkger/testdata/bucket.yml"))
			require.NoError(t, cmd.Execute())
		})

		t.Run("pkg is invalid returns error", func(t *testing.T) {
			// pkgYml is invalid because it is missing a name
			const pkgYml = `apiVersion: 0.1.0
kind: Package
meta:
  pkgName:      pkg_name
  pkgVersion:   1
spec:
  resources:
    - kind: Bucket`

			b := newCmdPkgBuilder(fakeSVCFn(new(fakePkgSVC)), in(strings.NewReader(pkgYml)), out(ioutil.Discard))
			cmd := b.cmdPkgValidate()
			require.Error(t, cmd.Execute())
		})
	})
}

type flagArg struct{ name, val string }

type pkgFileArgs struct {
	name     string
	filename string
	encoding pkger.Encoding
	flags    []flagArg
	envVars  []struct {
		key, val string
	}
}

func testPkgWrites(t *testing.T, newCmdFn func() *cobra.Command, args pkgFileArgs, assertFn func(t *testing.T, pkg *pkger.Pkg)) {
	wrappedCmdFn := func() *cobra.Command {
		cmd := newCmdFn()
		cmd.SetArgs([]string{}) // clears mess from test runner coming into cobra cli via stdin
		return cmd
	}

	var initialEnvVars []struct{ key, val string }
	for _, envVar := range args.envVars {
		if k := os.Getenv(envVar.key); k != "" {
			initialEnvVars = append(initialEnvVars, struct{ key, val string }{
				key: envVar.key,
				val: k,
			})
		}

		require.NoError(t, os.Setenv(envVar.key, envVar.val))
	}

	defer func() {
		for _, envVar := range args.envVars {
			require.NoError(t, os.Unsetenv(envVar.key))
		}

		for _, envVar := range initialEnvVars {
			require.NoError(t, os.Setenv(envVar.key, envVar.val))
		}
	}()
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
		require.NoError(t, cmd.Flags().Set("file", pathToFile))
		for _, f := range args.flags {
			require.NoError(t, cmd.Flags().Set(f.name, f.val), "cmd="+cmd.Name())
		}

		require.NoError(t, cmd.Execute())

		pkg, err := pkger.Parse(args.encoding, pkger.FromFile(pathToFile), pkger.ValidWithoutResources(), pkger.ValidSkipParseError())
		require.NoError(t, err)

		require.Equal(t, pkger.KindPackage, pkg.Kind)

		assertFn(t, pkg)
	}
}

func testPkgWritesToBuffer(newCmdFn func() *cobra.Command, args pkgFileArgs, assertFn func(t *testing.T, pkg *pkger.Pkg)) func(t *testing.T) {
	return func(t *testing.T) {
		t.Helper()

		var buf bytes.Buffer
		cmd := newCmdFn()
		cmd.SetOutput(&buf)
		for _, f := range args.flags {
			require.NoError(t, cmd.Flags().Set(f.name, f.val))
		}

		require.NoError(t, cmd.Execute())

		pkg, err := pkger.Parse(pkger.EncodingYAML, pkger.FromReader(&buf), pkger.ValidWithoutResources(), pkger.ValidSkipParseError())
		require.NoError(t, err)

		require.Equal(t, pkger.KindPackage, pkg.Kind)

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

func (f *fakePkgSVC) DryRun(ctx context.Context, orgID, userID influxdb.ID, pkg *pkger.Pkg) (pkger.Summary, pkger.Diff, error) {
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
