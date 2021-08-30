package v1validation

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	icontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/mock"
	datagen "github.com/influxdata/influxdb/v2/pkg/data/gen"
	"github.com/influxdata/influxdb/v2/storage/reads"
	"github.com/influxdata/influxdb/v2/tests"
	"github.com/influxdata/influxdb/v2/tests/pipeline"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v2"
)

var skipMap = map[string]string{
	// file_name_without_extension: skip_reason
}

type GeneratedDataset struct {
	Start string `yaml:"start"`
	End   string `yaml:"end"`
	Toml  string `yaml:"toml"`
}

type TestSuite struct {
	Tests     []Test            `yaml:"tests"`
	Dataset   string            `yaml:"dataset"`   // Line protocol OR
	Generated *GeneratedDataset `yaml:"generated"` // TOML schema description
}

type Test struct {
	Name   string `yaml:"name"`
	Query  string `yaml:"query"`
	Result string `yaml:"result"`
}

func TestGoldenFiles(t *testing.T) {
	err := filepath.WalkDir("./goldenfiles", func(path string, info os.DirEntry, err error) error {
		if info.IsDir() {
			return nil
		}
		base := filepath.Base(path)
		ext := filepath.Ext(base)
		testName := strings.TrimSuffix(base, ext)
		t.Run(testName, func(t *testing.T) {
			if reason, ok := skipMap[testName]; ok {
				t.Skip(reason)
			}
			gf := testSuiteFromPath(t, path)
			validate(t, gf)
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Unmarshal a TestSuite from a YAML file
func testSuiteFromPath(t *testing.T, path string) *TestSuite {
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}

	gf := &TestSuite{}
	err = yaml.Unmarshal(b, gf)
	if err != nil {
		t.Fatal(err)
	}
	return gf
}

func validate(t *testing.T, gf *TestSuite) {
	t.Helper()
	ctx := context.Background()
	p := tests.NewDefaultPipeline(t, func(o *launcher.InfluxdOpts) {
		o.LogLevel = zapcore.ErrorLevel
	})
	p.MustOpen()
	defer p.MustClose()
	orgID := p.DefaultOrgID
	bucketID := p.DefaultBucketID
	fx := pipeline.NewBaseFixture(t, p.Pipeline, orgID, bucketID)

	var dataset string

	if gf.Generated != nil {
		spec, err := datagen.NewSpecFromToml(gf.Generated.Toml)
		if err != nil {
			t.Fatalf("error processing TOML: %v", err)
		}

		tryParse := func(s string) (time.Time, error) {
			if v, err := strconv.Atoi(s); err == nil {
				return time.Unix(0, int64(v)), nil
			}

			return time.Parse(time.RFC3339, s)
		}

		start, err := tryParse(gf.Generated.Start)
		if err != nil {
			t.Fatalf("error parsing start: %v", err)
		}
		end, err := tryParse(gf.Generated.End)
		if err != nil {
			t.Fatalf("error parsing end: %v", err)
		}

		if end.Before(start) {
			t.Fatal("error: start must be before end")
		}

		sg := datagen.NewSeriesGeneratorFromSpec(spec, datagen.TimeRange{
			Start: start,
			End:   end,
		})

		rs := mock.NewResultSetFromSeriesGenerator(sg, mock.WithGeneratorMaxValues(10000))
		var sb strings.Builder
		if err := reads.ResultSetToLineProtocol(&sb, rs); err != nil {
			t.Fatalf("error generating data: %v", err)
		}
		dataset = sb.String()
		if len(dataset) == 0 {
			t.Fatal("no data generated")
		}
	} else {
		dataset = gf.Dataset
	}

	if err := fx.Admin.WriteBatch(dataset); err != nil {
		t.Fatal(err)
	}
	p.Flush()

	ctx = icontext.SetAuthorizer(ctx, tests.MakeAuthorization(p.DefaultOrgID, p.DefaultUserID, influxdb.OperPermissions()))

	if err := p.Launcher.DBRPMappingService().Create(ctx, &influxdb.DBRPMapping{
		Database:        "mydb",
		RetentionPolicy: "autogen",
		Default:         true,
		OrganizationID:  orgID,
		BucketID:        bucketID,
	}); err != nil {
		t.Fatal(err)
	}

	for i := range gf.Tests {
		test := &gf.Tests[i]
		name := test.Name
		if len(name) == 0 {
			name = fmt.Sprintf("query_%02d", i)
		}
		t.Run(name, func(t *testing.T) {
			err := fx.Admin.Client.Get("/query").
				QueryParams([2]string{"db", "mydb"}).
				QueryParams([2]string{"q", test.Query}).
				QueryParams([2]string{"epoch", "ns"}).
				Header("Content-Type", "application/vnd.influxql").
				Header("Accept", "application/csv").
				RespFn(func(resp *http.Response) error {
					b, err := ioutil.ReadAll(resp.Body)
					assert.NoError(t, err)
					assert.Equal(t, test.Result, string(b))
					return nil
				}).
				Do(ctx)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
