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

	"github.com/influxdata/influxdb/v2/kit/platform"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/pkger"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Template_Commands(t *testing.T) {
	fakeSVCFn := func(svc pkger.SVC) templateSVCsFn {
		return func() (pkger.SVC, influxdb.OrganizationService, error) {
			return svc, &mock.OrganizationService{
				FindOrganizationF: func(ctx context.Context, filter influxdb.OrganizationFilter) (*influxdb.Organization, error) {
					return &influxdb.Organization{ID: platform.ID(9000), Name: "influxdata"}, nil
				},
			}, nil
		}
	}

	t.Run("export all", func(t *testing.T) {
		defaultAssertFn := func(t *testing.T, pkg *pkger.Template) {
			t.Helper()
			sum := pkg.Summary()

			require.Len(t, sum.Buckets, 1)
			assert.Equal(t, "bucket1", sum.Buckets[0].Name)
		}

		expectedOrgID := platform.ID(9000)

		tests := []struct {
			templateFileArgs
			assertFn func(t *testing.T, pkg *pkger.Template)
		}{
			{
				templateFileArgs: templateFileArgs{
					name:     "yaml out with org id",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					args:     []string{"--org-id=" + expectedOrgID.String()},
				},
			},
			{
				templateFileArgs: templateFileArgs{
					name:     "yaml out with org name",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					args:     []string{"--org=influxdata"},
				},
			},
			{
				templateFileArgs: templateFileArgs{
					name:     "yaml out with org name env var",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					envVars:  map[string]string{"INFLUX_ORG": "influxdata"},
				},
			},
			{
				templateFileArgs: templateFileArgs{
					name:     "yaml out with org id env var",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					envVars:  map[string]string{"INFLUX_ORG_ID": expectedOrgID.String()},
				},
			},
			{
				templateFileArgs: templateFileArgs{
					name:     "with labelName filter",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					args: []string{
						"--org-id=" + expectedOrgID.String(),
						"--filter=labelName=foo",
					},
				},
				assertFn: func(t *testing.T, pkg *pkger.Template) {
					defaultAssertFn(t, pkg)

					sum := pkg.Summary()

					require.Len(t, sum.Labels, 1)
					assert.Equal(t, "foo", sum.Labels[0].Name)
				},
			},
			{
				templateFileArgs: templateFileArgs{
					name:     "with multiple labelName filters",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					args: []string{
						"--org-id=" + expectedOrgID.String(),
						"--filter=labelName=foo",
						"--filter=labelName=bar",
					},
				},
				assertFn: func(t *testing.T, pkg *pkger.Template) {
					defaultAssertFn(t, pkg)

					sum := pkg.Summary()

					require.Len(t, sum.Labels, 2)
					assert.Equal(t, "bar", sum.Labels[0].Name)
					assert.Equal(t, "foo", sum.Labels[1].Name)
				},
			},
			{
				templateFileArgs: templateFileArgs{
					name:     "with resourceKind filter",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					args: []string{
						"--org-id=" + expectedOrgID.String(),
						"--filter=resourceKind=Dashboard",
					},
				},
				assertFn: func(t *testing.T, pkg *pkger.Template) {
					sum := pkg.Summary()

					require.Len(t, sum.Dashboards, 1)
					assert.Equal(t, "dashboard", sum.Dashboards[0].Name)
				},
			},
			{
				templateFileArgs: templateFileArgs{
					name:     "with multiple resourceKind filter",
					encoding: pkger.EncodingYAML,
					filename: "pkg_0.yml",
					args: []string{
						"--org-id=" + expectedOrgID.String(),
						"--filter=resourceKind=Dashboard",
						"--filter=resourceKind=Bucket",
					},
				},
				assertFn: func(t *testing.T, pkg *pkger.Template) {
					sum := pkg.Summary()

					require.Len(t, sum.Buckets, 1)
					assert.Equal(t, "bucket", sum.Buckets[0].Name)
					require.Len(t, sum.Dashboards, 1)
					assert.Equal(t, "dashboard", sum.Dashboards[0].Name)
				},
			},
			{
				templateFileArgs: templateFileArgs{
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
				assertFn: func(t *testing.T, pkg *pkger.Template) {
					sum := pkg.Summary()

					require.Len(t, sum.Labels, 1)
					assert.Equal(t, "foo", sum.Labels[0].Name)
					require.Len(t, sum.Buckets, 1)
					assert.Equal(t, "bucket", sum.Buckets[0].Name)
					require.Len(t, sum.Dashboards, 1)
					assert.Equal(t, "dashboard", sum.Dashboards[0].Name)
				},
			},
		}

		cmdFn := func(f *globalFlags, opt genericCLIOpts) *cobra.Command {
			pkgSVC := &fakePkgSVC{
				exportFn: func(_ context.Context, opts ...pkger.ExportOptFn) (*pkger.Template, error) {
					opt := pkger.ExportOpt{}
					for _, o := range opts {
						if err := o(&opt); err != nil {
							return nil, err
						}
					}

					orgIDOpt := opt.OrgIDs[0]
					if orgIDOpt.OrgID != expectedOrgID {
						return nil, errors.New("did not provide expected orgID")
					}

					var pkg pkger.Template
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
									"name": strings.ToLower(k.String()),
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
			return newCmdPkgerBuilder(fakeSVCFn(pkgSVC), f, opt).cmdExport()
		}

		for _, tt := range tests {
			tt.templateFileArgs.args = append([]string{"export", "all"}, tt.templateFileArgs.args...)
			assertFn := defaultAssertFn
			if tt.assertFn != nil {
				assertFn = tt.assertFn
			}
			testPkgWrites(t, cmdFn, tt.templateFileArgs, assertFn)
		}
	})

	t.Run("export resources", func(t *testing.T) {
		t.Run("by resource", func(t *testing.T) {
			tests := []struct {
				name string
				templateFileArgs
				bucketIDs   []platform.ID
				dashIDs     []platform.ID
				endpointIDs []platform.ID
				labelIDs    []platform.ID
				ruleIDs     []platform.ID
				taskIDs     []platform.ID
				telegrafIDs []platform.ID
				varIDs      []platform.ID
				stackID     platform.ID
			}{
				{
					templateFileArgs: templateFileArgs{
						name:     "buckets",
						encoding: pkger.EncodingYAML,
						filename: "pkg_0.yml",
					},
					bucketIDs: []platform.ID{1, 2},
				},
				{
					templateFileArgs: templateFileArgs{
						name:     "dashboards",
						encoding: pkger.EncodingYAML,
						filename: "pkg_0.yml",
					},
					dashIDs: []platform.ID{1, 2},
				},
				{
					templateFileArgs: templateFileArgs{
						name:     "endpoints",
						encoding: pkger.EncodingYAML,
						filename: "pkg_0.yml",
					},
					endpointIDs: []platform.ID{1, 2},
				},
				{
					templateFileArgs: templateFileArgs{
						name:     "labels",
						encoding: pkger.EncodingYAML,
						filename: "pkg_0.yml",
					},
					labelIDs: []platform.ID{1, 2},
				},
				{
					templateFileArgs: templateFileArgs{
						name:     "rules",
						encoding: pkger.EncodingYAML,
						filename: "pkg_0.yml",
					},
					ruleIDs: []platform.ID{1, 2},
				},
				{
					templateFileArgs: templateFileArgs{
						name:     "tasks",
						encoding: pkger.EncodingYAML,
						filename: "pkg_0.yml",
					},
					taskIDs: []platform.ID{1, 2},
				},
				{
					templateFileArgs: templateFileArgs{
						name:     "telegrafs",
						encoding: pkger.EncodingYAML,
						filename: "pkg_0.yml",
					},
					telegrafIDs: []platform.ID{1, 2},
				},
				{
					templateFileArgs: templateFileArgs{
						name:     "variables",
						encoding: pkger.EncodingYAML,
						filename: "pkg_0.yml",
					},
					varIDs: []platform.ID{1, 2},
				},
			}

			cmdFn := func(f *globalFlags, opt genericCLIOpts) *cobra.Command {
				pkgSVC := &fakePkgSVC{
					exportFn: func(_ context.Context, opts ...pkger.ExportOptFn) (*pkger.Template, error) {
						var opt pkger.ExportOpt
						for _, o := range opts {
							if err := o(&opt); err != nil {
								return nil, err
							}
						}

						var pkg pkger.Template
						for _, rc := range opt.Resources {
							if rc.Kind == pkger.KindNotificationEndpoint {
								rc.Kind = pkger.KindNotificationEndpointHTTP
							}
							name := strings.ToLower(rc.Kind.String()) + strconv.Itoa(int(rc.ID))
							pkg.Objects = append(pkg.Objects, pkger.Object{
								APIVersion: pkger.APIVersion,
								Kind:       rc.Kind,
								Metadata:   pkger.Resource{"name": name},
							})
						}

						return &pkg, nil
					},
				}

				builder := newCmdPkgerBuilder(fakeSVCFn(pkgSVC), f, opt)
				return builder.cmdExport()
			}
			for _, tt := range tests {
				tt.args = append(tt.args,
					"export",
					"--buckets="+idsStr(tt.bucketIDs...),
					"--endpoints="+idsStr(tt.endpointIDs...),
					"--dashboards="+idsStr(tt.dashIDs...),
					"--labels="+idsStr(tt.labelIDs...),
					"--rules="+idsStr(tt.ruleIDs...),
					"--tasks="+idsStr(tt.taskIDs...),
					"--telegraf-configs="+idsStr(tt.telegrafIDs...),
					"--variables="+idsStr(tt.varIDs...),
				)

				testPkgWrites(t, cmdFn, tt.templateFileArgs, func(t *testing.T, pkg *pkger.Template) {
					sum := pkg.Summary()

					kindToName := func(k pkger.Kind, id platform.ID) string {
						return strings.ToLower(k.String()) + strconv.Itoa(int(id))
					}

					require.Len(t, sum.Buckets, len(tt.bucketIDs))
					for i, id := range tt.bucketIDs {
						actual := sum.Buckets[i]
						assert.Equal(t, kindToName(pkger.KindBucket, id), actual.Name)
					}
					require.Len(t, sum.Dashboards, len(tt.dashIDs))
					for i, id := range tt.dashIDs {
						actual := sum.Dashboards[i]
						assert.Equal(t, kindToName(pkger.KindDashboard, id), actual.Name)
					}
					require.Len(t, sum.NotificationEndpoints, len(tt.endpointIDs))
					for i, id := range tt.endpointIDs {
						actual := sum.NotificationEndpoints[i]
						assert.Equal(t, kindToName(pkger.KindNotificationEndpointHTTP, id), actual.NotificationEndpoint.GetName())
					}
					require.Len(t, sum.Labels, len(tt.labelIDs))
					for i, id := range tt.labelIDs {
						actual := sum.Labels[i]
						assert.Equal(t, kindToName(pkger.KindLabel, id), actual.Name)
					}
					require.Len(t, sum.NotificationRules, len(tt.ruleIDs))
					for i, id := range tt.ruleIDs {
						actual := sum.NotificationRules[i]
						assert.Equal(t, kindToName(pkger.KindNotificationRule, id), actual.Name)
					}
					require.Len(t, sum.Tasks, len(tt.taskIDs))
					for i, id := range tt.taskIDs {
						actual := sum.Tasks[i]
						assert.Equal(t, kindToName(pkger.KindTask, id), actual.Name)
					}
					require.Len(t, sum.TelegrafConfigs, len(tt.telegrafIDs))
					for i, id := range tt.telegrafIDs {
						actual := sum.TelegrafConfigs[i]
						assert.Equal(t, kindToName(pkger.KindTelegraf, id), actual.TelegrafConfig.Name)
					}
					require.Len(t, sum.Variables, len(tt.varIDs))
					for i, id := range tt.varIDs {
						actual := sum.Variables[i]
						assert.Equal(t, kindToName(pkger.KindVariable, id), actual.Name)
					}
				})
			}
		})

		t.Run("by stack", func(t *testing.T) {
			cmdFn := func(f *globalFlags, opt genericCLIOpts) *cobra.Command {
				pkgSVC := &fakePkgSVC{
					exportFn: func(_ context.Context, opts ...pkger.ExportOptFn) (*pkger.Template, error) {
						var opt pkger.ExportOpt
						for _, o := range opts {
							if err := o(&opt); err != nil {
								return nil, err
							}
						}

						if opt.StackID != 1 {
							return nil, errors.New("wrong stack ID, got: " + opt.StackID.String())
						}
						return &pkger.Template{
							Objects: []pkger.Object{
								pkger.LabelToObject("", influxdb.Label{
									Name: "label-1",
								}),
							},
						}, nil
					},
				}

				builder := newCmdPkgerBuilder(fakeSVCFn(pkgSVC), f, opt)
				return builder.cmdExport()
			}

			tmplFileArgs := templateFileArgs{
				name:     "stack",
				encoding: pkger.EncodingYAML,
				filename: "pkg_0.yml",
				args:     []string{"export", "--stack-id=" + platform.ID(1).String()},
			}

			testPkgWrites(t, cmdFn, tmplFileArgs, func(t *testing.T, pkg *pkger.Template) {
				sum := pkg.Summary()

				require.Len(t, sum.Labels, 1)
				assert.Equal(t, "label-1", sum.Labels[0].Name)
			})
		})
	})

	t.Run("validate", func(t *testing.T) {
		t.Run("template is valid returns no error", func(t *testing.T) {
			builder := newInfluxCmdBuilder(
				in(new(bytes.Buffer)),
				out(ioutil.Discard),
			)
			cmd := builder.cmd(func(f *globalFlags, opt genericCLIOpts) *cobra.Command {
				return newCmdPkgerBuilder(fakeSVCFn(new(fakePkgSVC)), f, opt).cmdTemplate()
			})

			cmd.SetArgs([]string{
				"template",
				"validate",
				"--file=../../pkger/testdata/bucket.yml",
				"-f=../../pkger/testdata/label.yml",
			})
			require.NoError(t, cmd.Execute())
		})

		t.Run("template is invalid returns error", func(t *testing.T) {
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
				return newCmdPkgerBuilder(fakeSVCFn(new(fakePkgSVC)), f, opt).cmdTemplate()
			})
			cmd.SetArgs([]string{"template", "validate"})

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
					args: []string{"--org-id=" + platform.ID(1).String()},
					expectedStack: pkger.Stack{
						OrgID: 1,
						Events: []pkger.StackEvent{{
							EventType: pkger.StackEventCreate,
						}},
					},
				},
				{
					name: "when org and name provided provided is successful",
					args: []string{
						"--org-id=" + platform.ID(1).String(),
						"--stack-name=foo",
					},
					expectedStack: pkger.Stack{
						OrgID: 1,
						Events: []pkger.StackEvent{{
							EventType: pkger.StackEventCreate,
							Name:      "foo",
						}},
					},
				},
				{
					name: "when all flags provided provided is successful",
					args: []string{
						"--org-id=" + platform.ID(1).String(),
						"--stack-name=foo",
						"--stack-description=desc",
						"--template-url=http://example.com/1",
						"--template-url=http://example.com/2",
					},
					expectedStack: pkger.Stack{
						OrgID: 1,
						Events: []pkger.StackEvent{{
							EventType:   pkger.StackEventCreate,
							Name:        "foo",
							Description: "desc",
							TemplateURLs: []string{
								"http://example.com/1",
								"http://example.com/2",
							},
						}},
					},
				},
				{
					name: "when all shorthand flags provided provided is successful",
					args: []string{
						"--org-id=" + platform.ID(1).String(),
						"-n=foo",
						"-d=desc",
						"-u=http://example.com/1",
						"-u=http://example.com/2",
					},
					expectedStack: pkger.Stack{
						OrgID: 1,
						Events: []pkger.StackEvent{{
							EventType:   pkger.StackEventCreate,
							Name:        "foo",
							Description: "desc",
							TemplateURLs: []string{
								"http://example.com/1",
								"http://example.com/2",
							},
						}},
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
							initStackFn: func(ctx context.Context, userID platform.ID, stCreate pkger.StackCreate) (pkger.Stack, error) {
								return pkger.Stack{
									ID:    9000,
									OrgID: stCreate.OrgID,
									Events: []pkger.StackEvent{
										{
											EventType:    pkger.StackEventCreate,
											Name:         stCreate.Name,
											Description:  stCreate.Description,
											Sources:      stCreate.Sources,
											TemplateURLs: stCreate.TemplateURLs,
											Resources:    stCreate.Resources,
										},
									},
								}, nil
							},
						}
						return newCmdPkgerBuilder(fakeSVCFn(echoSVC), f, opt).cmdStacks()
					})

					baseArgs := []string{"stacks", "init", "--json"}

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

		t.Run("update", func(t *testing.T) {
			tests := []struct {
				name          string
				args          []string
				expectedStack pkger.Stack
			}{
				{
					name: "when stack name and decription are set",
					args: []string{"--stack-name=updated_name", "--stack-description=updated_description"},
					expectedStack: pkger.Stack{
						OrgID: 1,
						Events: []pkger.StackEvent{{
							Name:        "updated_name",
							Description: "updated_description",
						}},
					},
				},
				{
					name: "when only stack name is set",
					args: []string{"--stack-name=updated_name"},
					expectedStack: pkger.Stack{
						OrgID: 1,
						Events: []pkger.StackEvent{{
							Name:        "updated_name",
							Description: "original_description",
						}},
					},
				},
				{
					name: "when only stack description is set",
					args: []string{"--stack-description=updated_description"},
					expectedStack: pkger.Stack{
						OrgID: 1,
						Events: []pkger.StackEvent{{
							Name:        "original_name",
							Description: "updated_description",
						}},
					},
				},
				{
					name: "when neither stack name/description is set",
					args: []string{},
					expectedStack: pkger.Stack{
						OrgID: 1,
						Events: []pkger.StackEvent{{
							Name:        "original_name",
							Description: "original_description",
						}},
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

					initialStack := pkger.Stack{
						ID:    9000,
						OrgID: 1,
						Events: []pkger.StackEvent{
							{
								Name:        "original_name",
								Description: "original_description",
							},
						},
					}
					rootCmd := builder.cmd(func(f *globalFlags, opt genericCLIOpts) *cobra.Command {
						echoSVC := &fakePkgSVC{
							updateStackFn: func(ctx context.Context, stUpdate pkger.StackUpdate) (pkger.Stack, error) {
								returnStack := initialStack
								if stUpdate.Name != nil {
									returnStack.Events[0].Name = *stUpdate.Name
								}
								if stUpdate.Description != nil {
									returnStack.Events[0].Description = *stUpdate.Description
								}
								return returnStack, nil
							},
						}
						return newCmdPkgerBuilder(fakeSVCFn(echoSVC), f, opt).cmdStacks()
					})

					baseArgs := []string{"stacks", "update", "--stack-id=" + platform.ID(1).String(), "--json"}

					rootCmd.SetArgs(append(baseArgs, tt.args...))

					err := rootCmd.Execute()
					require.NoError(t, err)
					var stack pkger.Stack
					testDecodeJSONBody(t, outBuf, &stack)
					if tt.expectedStack.ID == 0 {
						tt.expectedStack.ID = 9000
					}
					assert.Equal(t, tt.expectedStack, stack)
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

type templateFileArgs struct {
	name     string
	filename string
	encoding pkger.Encoding
	args     []string
	envVars  map[string]string
}

func testPkgWrites(t *testing.T, newCmdFn func(*globalFlags, genericCLIOpts) *cobra.Command, args templateFileArgs, assertFn func(t *testing.T, pkg *pkger.Template)) {
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

func testPkgWritesFile(newCmdFn func(w io.Writer) *cobra.Command, args templateFileArgs, assertFn func(t *testing.T, pkg *pkger.Template)) func(t *testing.T) {
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

func testPkgWritesToBuffer(newCmdFn func(w io.Writer) *cobra.Command, args templateFileArgs, assertFn func(t *testing.T, pkg *pkger.Template)) func(t *testing.T) {
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
	initStackFn   func(ctx context.Context, userID platform.ID, stack pkger.StackCreate) (pkger.Stack, error)
	updateStackFn func(ctx context.Context, upd pkger.StackUpdate) (pkger.Stack, error)
	exportFn      func(ctx context.Context, setters ...pkger.ExportOptFn) (*pkger.Template, error)
	dryRunFn      func(ctx context.Context, orgID, userID platform.ID, opts ...pkger.ApplyOptFn) (pkger.ImpactSummary, error)
	applyFn       func(ctx context.Context, orgID, userID platform.ID, opts ...pkger.ApplyOptFn) (pkger.ImpactSummary, error)
}

var _ pkger.SVC = (*fakePkgSVC)(nil)

func (f *fakePkgSVC) InitStack(ctx context.Context, userID platform.ID, stack pkger.StackCreate) (pkger.Stack, error) {
	if f.initStackFn != nil {
		return f.initStackFn(ctx, userID, stack)
	}
	panic("not implemented")
}

func (f *fakePkgSVC) ListStacks(ctx context.Context, orgID platform.ID, filter pkger.ListFilter) ([]pkger.Stack, error) {
	panic("not implemented")
}

func (f *fakePkgSVC) UninstallStack(ctx context.Context, identifiers struct{ OrgID, UserID, StackID platform.ID }) (pkger.Stack, error) {
	panic("not implemeted")
}

func (f *fakePkgSVC) DeleteStack(ctx context.Context, identifiers struct{ OrgID, UserID, StackID platform.ID }) error {
	panic("not implemented")
}

func (f *fakePkgSVC) ExportStack(ctx context.Context, orgID, stackID platform.ID) (*pkger.Template, error) {
	panic("not implemented")
}

func (f *fakePkgSVC) ReadStack(ctx context.Context, id platform.ID) (pkger.Stack, error) {
	panic("not implemented")
}

func (f *fakePkgSVC) UpdateStack(ctx context.Context, upd pkger.StackUpdate) (pkger.Stack, error) {
	if f.updateStackFn != nil {
		return f.updateStackFn(ctx, upd)
	}
	panic("not implemented")
}

func (f *fakePkgSVC) Export(ctx context.Context, setters ...pkger.ExportOptFn) (*pkger.Template, error) {
	if f.exportFn != nil {
		return f.exportFn(ctx, setters...)
	}
	panic("not implemented")
}

func (f *fakePkgSVC) DryRun(ctx context.Context, orgID, userID platform.ID, opts ...pkger.ApplyOptFn) (pkger.ImpactSummary, error) {
	if f.dryRunFn != nil {
		return f.dryRunFn(ctx, orgID, userID, opts...)
	}
	panic("not implemented")
}

func (f *fakePkgSVC) Apply(ctx context.Context, orgID, userID platform.ID, opts ...pkger.ApplyOptFn) (pkger.ImpactSummary, error) {
	if f.applyFn != nil {
		return f.applyFn(ctx, orgID, userID, opts...)
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

func idsStr(ids ...platform.ID) string {
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
