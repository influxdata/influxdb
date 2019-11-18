package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdata/influxdb/pkger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Pkg(t *testing.T) {
	fakeSVCFn := func(svc pkger.SVC) pkgSVCFn {
		return func(opts httpClientOpts) (pkger.SVC, error) {
			return svc, nil
		}
	}

	t.Run("new", func(t *testing.T) {
		tests := []struct {
			name         string
			encoding     pkger.Encoding
			filename     string
			flags        []struct{ name, val string }
			expectedMeta pkger.Metadata
		}{
			{
				name:     "yaml out",
				encoding: pkger.EncodingYAML,
				filename: "pkg_0.yml",
				flags: []struct{ name, val string }{
					{name: "name", val: "new name"},
					{name: "description", val: "new desc"},
					{name: "version", val: "new version"},
				},
				expectedMeta: pkger.Metadata{
					Name:        "new name",
					Description: "new desc",
					Version:     "new version",
				},
			},
			{
				name:     "json out",
				encoding: pkger.EncodingJSON,
				filename: "pkg_1.json",
				flags: []struct{ name, val string }{
					{name: "name", val: "new name"},
					{name: "description", val: "new desc"},
					{name: "version", val: "new version"},
				},
				expectedMeta: pkger.Metadata{
					Name:        "new name",
					Description: "new desc",
					Version:     "new version",
				},
			},
			{
				name:     "quiet mode",
				encoding: pkger.EncodingJSON,
				filename: "pkg_1.json",
				flags: []struct{ name, val string }{
					{name: "quiet", val: "true"},
				},
			},
		}

		for _, tt := range tests {
			fn := func(t *testing.T) {
				tempDir := newTempDir(t)
				defer os.RemoveAll(tempDir)

				pathToFile := filepath.Join(tempDir, tt.filename)

				cmd := pkgCreateCmd(fakeSVCFn(pkger.NewService()))
				require.NoError(t, cmd.Flags().Set("out", pathToFile))
				for _, f := range tt.flags {
					require.NoError(t, cmd.Flags().Set(f.name, f.val))
				}
				require.NoError(t, cmd.Execute())

				pkg, err := pkger.Parse(tt.encoding, pkger.FromFile(pathToFile), pkger.ValidWithoutResources())
				require.NoError(t, err)

				assert.Equal(t, pkger.KindPackage.String(), pkg.Kind)
				assert.Equal(t, tt.expectedMeta.Description, pkg.Metadata.Description)

				hasNotZeroDefault := func(t *testing.T, expected, actual string) {
					t.Helper()
					if expected == "" {
						assert.NotZero(t, actual)
						return
					}
					assert.Equal(t, expected, actual)
				}
				hasNotZeroDefault(t, tt.expectedMeta.Name, pkg.Metadata.Name)
				hasNotZeroDefault(t, tt.expectedMeta.Version, pkg.Metadata.Version)
			}

			t.Run(tt.name, fn)
		}
	})
}

func newTempDir(t *testing.T) string {
	t.Helper()

	tempDir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	return tempDir
}
