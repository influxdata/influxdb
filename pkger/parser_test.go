package pkger

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	t.Run("file has all necessary metadata", func(t *testing.T) {
		t.Run("has valid metadata and at least 1 resource", func(t *testing.T) {
			testfileRunner(t, "testdata/bucket", nil)
		})

		t.Run("malformed required metadata", func(t *testing.T) {
			containsField := func(t *testing.T, expected []string, actual string) {
				t.Helper()

				for _, e := range expected {
					if e == actual {
						return
					}
				}
				assert.Fail(t, "did not find field: "+actual)
			}

			tests := []struct {
				name           string
				in             string
				expectedFields []string
			}{
				{
					name: "missing apiVersion",
					in: `kind: Package
meta:
  pkgName:      first_bucket_package
  pkgVersion:   1
spec:
  resources:
    - kind: Bucket
      name: buck_1
      retention_period: 1h
`,
					expectedFields: []string{"apiVersion"},
				},
				{
					name: "apiVersion is invalid version",
					in: `apiVersion: 222.2 #invalid apiVersion
kind: Package
meta:
  pkgName:      first_bucket_package
  pkgVersion:   1
spec:
  resources:
    - kind: Bucket
      name: buck_1
      retention_period: 1h
`,
					expectedFields: []string{"apiVersion"},
				},
				{
					name: "missing kind",
					in: `apiVersion: 0.1.0
meta:
  pkgName:   first_bucket_package
  pkgVersion:   1
spec:
  resources:
    - kind: Bucket
      name: buck_1
      retention_period: 1h
`,
					expectedFields: []string{"kind"},
				},
				{
					name: "missing pkgName",
					in: `apiVersion: 0.1.0
kind: Package
meta:
  pkgVersion:   1
spec:
  resources:
    - kind: Bucket
      name: buck_1
      retention_period: 1h
`,
					expectedFields: []string{"pkgName"},
				},
				{
					name: "missing pkgVersion",
					in: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName:   foo_name
spec:
  resources:
    - kind: Bucket
      name: buck_1
      retention_period: 1h
`,
					expectedFields: []string{"pkgVersion"},
				},
				{
					name: "missing multiple",
					in: `spec:
  resources:
    - kind: Bucket
      name: buck_1
      retention_period: 1h
`,
					expectedFields: []string{"pkgVersion", "pkgName", "kind", "apiVersion"},
				},
			}

			for _, tt := range tests {
				fn := func(t *testing.T) {
					_, err := Parse(EncodingYAML, FromString(tt.in))
					require.Error(t, err)

					pErr, ok := IsParseErr(err)
					require.True(t, ok)

					require.Len(t, pErr.Resources, 1)

					failedResource := pErr.Resources[0]
					assert.Equal(t, "Package", failedResource.Type)

					require.Len(t, failedResource.ValidationFailures, len(tt.expectedFields))

					for _, f := range failedResource.ValidationFailures {
						containsField(t, tt.expectedFields, f.Field)
					}
				}

				t.Run(tt.name, fn)
			}
		})
	})

	t.Run("file with just a bucket", func(t *testing.T) {
		t.Run("with valid bucket pkg should be valid", func(t *testing.T) {
			testfileRunner(t, "testdata/bucket", func(t *testing.T, pkg *Pkg) {
				buckets := pkg.buckets()
				require.Len(t, buckets, 1)

				actual := buckets[0]
				expectedBucket := bucket{
					Name:            "rucket_11",
					Description:     "bucket 1 description",
					RetentionPeriod: time.Hour,
				}
				assert.Equal(t, expectedBucket, *actual)
			})
		})

		t.Run("with missing bucket name should error", func(t *testing.T) {
			tests := []struct {
				name    string
				numErrs int
				in      string
			}{
				{
					name:    "missing name",
					numErrs: 1,
					in: `apiVersion: 1
kind: Package
meta:
  pkgName:      first_bucket_package
  pkgVersion:   1
spec:
  resources:
    - kind: Bucket
      retention_period: 1h
`,
				}, {
					name:    "mixed valid and missing name",
					numErrs: 1,
					in: `apiVersion: 1
kind: Package
meta:
  pkgName:      first_bucket_package
  pkgVersion:   1
spec:
  resources:
    - kind: Bucket
      retention_period: 1h
      name: valid name
    - kind: Bucket
      retention_period: 1h
`,
				}, {
					name:    "mixed valid and multiple bad names",
					numErrs: 2,
					in: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName:      first_bucket_package
  pkgVersion:   1
spec:
  resources:
    - kind: Bucket
      retention_period: 1h
      name: valid name
    - kind: Bucket
      retention_period: 1h
    - kind: Bucket
      retention_period: 1h
`,
				},
			}

			for _, tt := range tests {
				fn := func(t *testing.T) {
					_, err := Parse(EncodingYAML, FromString(tt.in))
					pErr, ok := IsParseErr(err)
					require.True(t, ok)
					assert.Len(t, pErr.Resources, tt.numErrs)
				}

				t.Run(tt.name, fn)
			}
		})

		t.Run("with duplicate buckets should error", func(t *testing.T) {
			yamlFile := `apiVersion: 0.1.0
kind: Package
meta:
  pkgName:      first_bucket_package
  pkgVersion:   1
spec:
  resources:
    - kind: Bucket
      retention_period: 1h
      name: valid name
    - kind: Bucket
      retention_period: 1h
      name: valid name
`
			_, err := Parse(EncodingYAML, FromString(yamlFile))
			require.Error(t, err)

			pErr, ok := IsParseErr(err)
			require.True(t, ok)
			assert.Len(t, pErr.Resources, 1)

			fields := pErr.Resources[0].ValidationFailures
			require.Len(t, fields, 1)
			assert.Equal(t, "name", fields[0].Field)
		})
	})

	t.Run("file with just a label", func(t *testing.T) {
		t.Run("with valid label pkg should be valid", func(t *testing.T) {
			testfileRunner(t, "testdata/label", func(t *testing.T, pkg *Pkg) {
				labels := pkg.labels()
				require.Len(t, labels, 2)

				expectedLabel1 := label{
					Name:        "label_1",
					Description: "label 1 description",
					Color:       "#FFFFFF",
				}
				assert.Equal(t, expectedLabel1, *labels[0])

				expectedLabel2 := label{
					Name:        "label_2",
					Description: "label 2 description",
					Color:       "#000000",
				}
				assert.Equal(t, expectedLabel2, *labels[1])
			})
		})

		t.Run("with missing label name should error", func(t *testing.T) {
			tests := []struct {
				name    string
				numErrs int
				in      string
			}{
				{
					name:    "missing name",
					numErrs: 1,
					in: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName: first_label_pkg 
  pkgVersion:   1
spec:
  resources:
    - kind: Label 
`,
				},
				{
					name:    "mixed valid and missing name",
					numErrs: 1,
					in: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName: label_pkg
  pkgVersion: 1
spec:
  resources:
    - kind: Label
      name: valid name
    - kind: Label
`,
				},
				{
					name:    "multiple labels with missing name",
					numErrs: 2,
					in: `apiVersion: 0.1.0
kind: Package
meta:
  pkgName: label_pkg
  pkgVersion: 1
spec:
  resources:
    - kind: Label
    - kind: Label
`,
				},
			}

			for _, tt := range tests {
				fn := func(t *testing.T) {
					_, err := Parse(EncodingYAML, FromString(tt.in))
					pErr, ok := IsParseErr(err)
					require.True(t, ok)
					assert.Len(t, pErr.Resources, tt.numErrs)
				}

				t.Run(tt.name, fn)
			}
		})
	})
}

type baseAsserts struct {
	version     string
	kind        string
	description string
	metaName    string
	metaVersion string
}

func validParsedPkg(t *testing.T, path string, encoding Encoding, expected baseAsserts) *Pkg {
	t.Helper()

	pkg, err := Parse(encoding, FromFile(path))
	require.NoError(t, err)

	require.Equal(t, expected.version, pkg.APIVersion)
	require.Equal(t, expected.kind, pkg.Kind)
	require.Equal(t, expected.description, pkg.Metadata.Description)
	require.Equal(t, expected.metaName, pkg.Metadata.Name)
	require.Equal(t, expected.metaVersion, pkg.Metadata.Version)

	return pkg
}

func testfileRunner(t *testing.T, path string, testFn func(t *testing.T, pkg *Pkg)) {
	t.Helper()

	tests := []struct {
		name      string
		extension string
		encoding  Encoding
	}{
		{
			name:      "yaml",
			extension: "yml",
			encoding:  EncodingYAML,
		},
		{
			name:      "json",
			extension: "json",
			encoding:  EncodingJSON,
		},
	}

	for _, tt := range tests {
		fn := func(t *testing.T) {
			t.Helper()

			pkg := validParsedPkg(t, path+"."+tt.extension, tt.encoding, baseAsserts{
				version:     "0.1.0",
				kind:        "Package",
				description: "pack description",
				metaName:    "pkg_name",
				metaVersion: "1",
			})
			if testFn != nil {
				testFn(t, pkg)
			}
		}
		t.Run(tt.name, fn)
	}

}
