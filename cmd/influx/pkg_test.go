package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/pkger"
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

	t.Run("export all", func(t *testing.T) {
		defaultAssertFn := func(t *testing.T, pkg *pkger.Pkg) {
			t.Helper()
			sum := pkg.Summary()

			require.Len(t, sum.Buckets, 1)
			assert.Equal(t, "bucket1", sum.Buckets[0].Name)
		}

		expectedOrgID := influxdb.ID(9000)

		tests := []struct {
			pkgFileArgs
			assertFn func(t *testing.T, pkg *pkger.Pkg)
		}{
			{
				pkgFileArgs: pkgFileArgs{
					name:     "yaml out with org id",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					args:     []string{"--org-id=" + expectedOrgID.String()},
				},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "yaml out with org name",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					args:     []string{"--org=influxdata"},
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
			{
				pkgFileArgs: pkgFileArgs{
					name:     "with labelName filter",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					args: []string{
						"--org-id=" + expectedOrgID.String(),
						"--filter=labelName=foo",
					},
				},
				assertFn: func(t *testing.T, pkg *pkger.Pkg) {
					defaultAssertFn(t, pkg)

					sum := pkg.Summary()

					require.Len(t, sum.Labels, 1)
					assert.Equal(t, "foo", sum.Labels[0].Name)
				},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "with multiple labelName filters",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					args: []string{
						"--org-id=" + expectedOrgID.String(),
						"--filter=labelName=foo",
						"--filter=labelName=bar",
					},
				},
				assertFn: func(t *testing.T, pkg *pkger.Pkg) {
					defaultAssertFn(t, pkg)

					sum := pkg.Summary()

					require.Len(t, sum.Labels, 2)
					assert.Equal(t, "bar", sum.Labels[0].Name)
					assert.Equal(t, "foo", sum.Labels[1].Name)
				},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "with resourceKind filter",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					args: []string{
						"--org-id=" + expectedOrgID.String(),
						"--filter=resourceKind=Dashboard",
					},
				},
				assertFn: func(t *testing.T, pkg *pkger.Pkg) {
					sum := pkg.Summary()

					require.Len(t, sum.Dashboards, 1)
					assert.Equal(t, "Dashboard", sum.Dashboards[0].Name)
				},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "with multiple resourceKind filter",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					args: []string{
						"--org-id=" + expectedOrgID.String(),
						"--filter=resourceKind=Dashboard",
						"--filter=resourceKind=Bucket",
					},
				},
				assertFn: func(t *testing.T, pkg *pkger.Pkg) {
					sum := pkg.Summary()

					require.Len(t, sum.Buckets, 1)
					assert.Equal(t, "Bucket", sum.Buckets[0].Name)
					require.Len(t, sum.Dashboards, 1)
					assert.Equal(t, "Dashboard", sum.Dashboards[0].Name)
				},
			},
			{
				pkgFileArgs: pkgFileArgs{
					name:     "with mixed resourceKind and labelName filters",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					args: []string{
						"--org-id=" + expectedOrgID.String(),
						"--filter=labelName=foo",
						"--filter=resourceKind=Dashboard",
						"--filter=resourceKind=Bucket",
					},
				},
				assertFn: func(t *testing.T, pkg *pkger.Pkg) {
					sum := pkg.Summary()

					require.Len(t, sum.Labels, 1)
					assert.Equal(t, "foo", sum.Labels[0].Name)
					require.Len(t, sum.Buckets, 1)
					assert.Equal(t, "Bucket", sum.Buckets[0].Name)
					require.Len(t, sum.Dashboards, 1)
					assert.Equal(t, "Dashboard", sum.Dashboards[0].Name)
				},
			},
		}

		cmdFn := func(_ *globalFlags, opt genericCLIOpts) *cobra.Command {
			pkgSVC := &fakePkgSVC{
				createFn: func(_ context.Context, opts ...pkger.CreatePkgSetFn) (*pkger.Pkg, error) {
					opt := pkger.CreateOpt{}
					for _, o := range opts {
						if err := o(&opt); err != nil {
							return nil, err
						}
					}

					orgIDOpt := opt.OrgIDs[0]
					if orgIDOpt.OrgID != expectedOrgID {
						return nil, errors.New("did not provide expected orgID")
					}

					var pkg pkger.Pkg
					for _, labelName := range orgIDOpt.LabelNames {
						pkg.Objects = append(pkg.Objects, pkger.Object{
							APIVersion: pkger.APIVersion,
							Kind:       pkger.KindLabel,
							Metadata:   pkger.Resource{"name": labelName},
						})
					}
					if len(orgIDOpt.ResourceKinds) > 0 {
						for _, k := range orgIDOpt.ResourceKinds {
							pkg.Objects = append(pkg.Objects, pkger.Object{
								APIVersion: pkger.APIVersion,
								Kind:       k,
								Metadata: pkger.Resource{
									"name": k.String(),
								},
							})
						}
						// return early so we don't get the default bucket
						return &pkg, nil
					}

					pkg.Objects = append(pkg.Objects, pkger.Object{
						APIVersion: pkger.APIVersion,
						Kind:       pkger.KindBucket,
						Metadata:   pkger.Resource{"name": "bucket1"},
					})
					return &pkg, nil
				},
			}
			return newCmdPkgBuilder(fakeSVCFn(pkgSVC), opt).cmd()
		}

		for _, tt := range tests {
			tt.pkgFileArgs.args = append([]string{"pkg", "export", "all"}, tt.pkgFileArgs.args...)
			assertFn := defaultAssertFn
			if tt.assertFn != nil {
				assertFn = tt.assertFn
			}
			testPkgWrites(t, cmdFn, tt.pkgFileArgs, assertFn)
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

		cmdFn := func(_ *globalFlags, opt genericCLIOpts) *cobra.Command {
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
							Kind:       rc.Kind,
							Metadata:   pkger.Resource{"name": name},
						})
					}

					return &pkg, nil
				},
			}

			builder := newCmdPkgBuilder(fakeSVCFn(pkgSVC), opt)
			return builder.cmd()
		}
		for _, tt := range tests {
			tt.args = append(tt.args,
				"pkg", "export",
				"--buckets="+idsStr(tt.bucketIDs...),
				"--endpoints="+idsStr(tt.endpointIDs...),
				"--dashboards="+idsStr(tt.dashIDs...),
				"--labels="+idsStr(tt.labelIDs...),
				"--rules="+idsStr(tt.ruleIDs...),
				"--tasks="+idsStr(tt.taskIDs...),
				"--telegraf-configs="+idsStr(tt.telegrafIDs...),
				"--variables="+idsStr(tt.varIDs...),
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
			builder := newInfluxCmdBuilder(
				in(new(bytes.Buffer)),
				out(ioutil.Discard),
			)
			cmd := builder.cmd(func(f *globalFlags, opt genericCLIOpts) *cobra.Command {
				return newCmdPkgBuilder(fakeSVCFn(new(fakePkgSVC)), opt).cmd()
			})

			cmd.SetArgs([]string{
				"pkg",
				"validate",
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
			builder := newInfluxCmdBuilder(
				in(strings.NewReader(pkgYml)),
				out(ioutil.Discard),
			)
			cmd := builder.cmd(func(f *globalFlags, opt genericCLIOpts) *cobra.Command {
				return newCmdPkgBuilder(fakeSVCFn(new(fakePkgSVC)), opt).cmd()
			})
			cmd.SetArgs([]string{"pkg", "validate"})

			require.Error(t, cmd.Execute())
		})
	})

	t.Run("stack", func(t *testing.T) {
		t.Run("init", func(t *testing.T) {
			tests := []struct {
				name          string
				args          []string
				envVars       map[string]string
				expectedStack pkger.Stack
				shouldErr     bool
			}{
				{
					name: "when only org and token provided is successful",
					args: []string{"--org-id=" + influxdb.ID(1).String()},
					expectedStack: pkger.Stack{
						OrgID: 1,
					},
				},
				{
					name: "when org and name provided provided is successful",
					args: []string{
						"--org-id=" + influxdb.ID(1).String(),
						"--stack-name=foo",
					},
					expectedStack: pkger.Stack{
						OrgID: 1,
						Name:  "foo",
					},
				},
				{
					name: "when all flags provided provided is successful",
					args: []string{
						"--org-id=" + influxdb.ID(1).String(),
						"--stack-name=foo",
						"--stack-description=desc",
						"--package-url=http://example.com/1",
						"--package-url=http://example.com/2",
					},
					expectedStack: pkger.Stack{
						OrgID:       1,
						Name:        "foo",
						Description: "desc",
						URLs: []string{
							"http://example.com/1",
							"http://example.com/2",
						},
					},
				},
				{
					name: "when all shorthand flags provided provided is successful",
					args: []string{
						"--org-id=" + influxdb.ID(1).String(),
						"-n=foo",
						"-d=desc",
						"-u=http://example.com/1",
						"-u=http://example.com/2",
					},
					expectedStack: pkger.Stack{
						OrgID:       1,
						Name:        "foo",
						Description: "desc",
						URLs: []string{
							"http://example.com/1",
							"http://example.com/2",
						},
					},
				},
			}

			for _, tt := range tests {
				fn := func(t *testing.T) {
					defer addEnvVars(t, envVarsZeroMap)()

					outBuf := new(bytes.Buffer)
					defer func() {
						if t.Failed() && outBuf.Len() > 0 {
							t.Log(outBuf.String())
						}
					}()

					builder := newInfluxCmdBuilder(
						in(new(bytes.Buffer)),
						out(outBuf),
					)

					rootCmd := builder.cmd(func(f *globalFlags, opt genericCLIOpts) *cobra.Command {
						echoSVC := &fakePkgSVC{
							initStackFn: func(ctx context.Context, userID influxdb.ID, stack pkger.Stack) (pkger.Stack, error) {
								stack.ID = 9000
								return stack, nil
							},
						}
						return newCmdPkgBuilder(fakeSVCFn(echoSVC), opt).cmd()
					})

					baseArgs := []string{"pkg", "stack", "init", "--json"}

					rootCmd.SetArgs(append(baseArgs, tt.args...))

					err := rootCmd.Execute()
					if tt.shouldErr {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
						var stack pkger.Stack
						testDecodeJSONBody(t, outBuf, &stack)
						if tt.expectedStack.ID == 0 {
							tt.expectedStack.ID = 9000
						}
						assert.Equal(t, tt.expectedStack, stack)
					}
				}

				t.Run(tt.name, fn)
			}
		})
	})
}

func Test_readFilesFromPath(t *testing.T) {
	t.Run("single file", func(t *testing.T) {
		dir := newTempDir(t)
		defer os.RemoveAll(dir)

		f := newTempFile(t, dir)

		files, err := readFilesFromPath(f.Name(), false)
		require.NoError(t, err)
		assert.Equal(t, []string{f.Name()}, files)

		files, err = readFilesFromPath(f.Name(), true)
		require.NoError(t, err)
		assert.Equal(t, []string{f.Name()}, files)
	})

	t.Run("dir with no files", func(t *testing.T) {
		dir := newTempDir(t)
		defer os.RemoveAll(dir)

		files, err := readFilesFromPath(dir, false)
		require.NoError(t, err)
		assert.Empty(t, files)
	})

	t.Run("dir with only files", func(t *testing.T) {
		dir := newTempDir(t)
		defer os.RemoveAll(dir)

		filePaths := []string{
			newTempFile(t, dir).Name(),
			newTempFile(t, dir).Name(),
		}
		sort.Strings(filePaths)

		files, err := readFilesFromPath(dir, false)
		require.NoError(t, err)
		sort.Strings(files)
		assert.Equal(t, filePaths, files)

		files, err = readFilesFromPath(dir, true)
		require.NoError(t, err)
		sort.Strings(files)
		assert.Equal(t, filePaths, files)
	})

	t.Run("dir with nested dir that has files", func(t *testing.T) {
		dir := newTempDir(t)
		defer os.RemoveAll(dir)

		nestedDir := filepath.Join(dir, "/nested/twice")
		require.NoError(t, os.MkdirAll(nestedDir, os.ModePerm))

		filePaths := []string{
			newTempFile(t, nestedDir).Name(),
			newTempFile(t, nestedDir).Name(),
		}
		sort.Strings(filePaths)

		files, err := readFilesFromPath(dir, false)
		require.NoError(t, err)
		sort.Strings(files)
		assert.Empty(t, files)

		files, err = readFilesFromPath(dir, true)
		require.NoError(t, err)
		sort.Strings(files)
		assert.Equal(t, filePaths, files)
	})
}

type pkgFileArgs struct {
	name     string
	filename string
	encoding pkger.Encoding
	args     []string
	envVars  map[string]string
}

func testPkgWrites(t *testing.T, newCmdFn func(*globalFlags, genericCLIOpts) *cobra.Command, args pkgFileArgs, assertFn func(t *testing.T, pkg *pkger.Pkg)) {
	t.Helper()

	defer addEnvVars(t, args.envVars)()

	wrappedCmdFn := func(w io.Writer) *cobra.Command {
		builder := newInfluxCmdBuilder(
			in(new(bytes.Buffer)),
			out(w),
		)
		cmd := builder.cmd(newCmdFn)
		cmd.SetArgs([]string{}) // clears mess from test runner coming into cobra cli via stdin
		return cmd
	}

	t.Run(path.Join(args.name, "file"), testPkgWritesFile(wrappedCmdFn, args, assertFn))
	t.Run(path.Join(args.name, "buffer"), testPkgWritesToBuffer(wrappedCmdFn, args, assertFn))
}

func testPkgWritesFile(newCmdFn func(w io.Writer) *cobra.Command, args pkgFileArgs, assertFn func(t *testing.T, pkg *pkger.Pkg)) func(t *testing.T) {
	return func(t *testing.T) {
		t.Helper()

		tempDir := newTempDir(t)
		defer os.RemoveAll(tempDir)

		pathToFile := filepath.Join(tempDir, args.filename)

		cmd := newCmdFn(ioutil.Discard)
		cmd.SetArgs(append(args.args, "--file="+pathToFile))

		require.NoError(t, cmd.Execute())

		pkg, err := pkger.Parse(args.encoding, pkger.FromFile(pathToFile), pkger.ValidWithoutResources(), pkger.ValidSkipParseError())
		require.NoError(t, err)

		assertFn(t, pkg)
	}
}

func testPkgWritesToBuffer(newCmdFn func(w io.Writer) *cobra.Command, args pkgFileArgs, assertFn func(t *testing.T, pkg *pkger.Pkg)) func(t *testing.T) {
	return func(t *testing.T) {
		t.Helper()

		var buf bytes.Buffer
		cmd := newCmdFn(&buf)
		cmd.SetArgs(args.args)

		require.NoError(t, cmd.Execute())

		pkg, err := pkger.Parse(pkger.EncodingYAML, pkger.FromReader(&buf), pkger.ValidWithoutResources(), pkger.ValidSkipParseError())
		require.NoError(t, err)

		assertFn(t, pkg)
	}
}

type fakePkgSVC struct {
	initStackFn func(ctx context.Context, userID influxdb.ID, stack pkger.Stack) (pkger.Stack, error)
	createFn    func(ctx context.Context, setters ...pkger.CreatePkgSetFn) (*pkger.Pkg, error)
	dryRunFn    func(ctx context.Context, orgID, userID influxdb.ID, pkg *pkger.Pkg) (pkger.Summary, pkger.Diff, error)
	applyFn     func(ctx context.Context, orgID, userID influxdb.ID, pkg *pkger.Pkg, opts ...pkger.ApplyOptFn) (pkger.Summary, pkger.Diff, error)
}

func (f *fakePkgSVC) InitStack(ctx context.Context, userID influxdb.ID, stack pkger.Stack) (pkger.Stack, error) {
	if f.initStackFn != nil {
		return f.initStackFn(ctx, userID, stack)
	}
	panic("not implemented")
}

func (f *fakePkgSVC) ListStacks(ctx context.Context, orgID influxdb.ID, filter pkger.ListFilter) ([]pkger.Stack, error) {
	panic("not implemented")
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

func (f *fakePkgSVC) Apply(ctx context.Context, orgID, userID influxdb.ID, pkg *pkger.Pkg, opts ...pkger.ApplyOptFn) (pkger.Summary, pkger.Diff, error) {
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

func newTempFile(t *testing.T, dir string) *os.File {
	t.Helper()

	f, err := ioutil.TempFile(dir, "")
	require.NoError(t, err)
	return f
}

func idsStr(ids ...influxdb.ID) string {
	var idStrs []string
	for _, id := range ids {
		idStrs = append(idStrs, id.String())
	}
	return strings.Join(idStrs, ",")
}

func testDecodeJSONBody(t *testing.T, r io.Reader, v interface{}) {
	t.Helper()

	err := json.NewDecoder(r).Decode(v)
	require.NoError(t, err)
}
