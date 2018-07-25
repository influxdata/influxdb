package spectests

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/mock"
	"github.com/influxdata/platform/query"
	"github.com/influxdata/platform/query/influxql"
	platformtesting "github.com/influxdata/platform/testing"
)

var dbrpMappingSvc = mock.NewDBRPMappingService()
var organizationID *platform.ID //= platform.ID("aaaa")
var bucketID *platform.ID       //= platform.ID("bbbb")
var altBucketID *platform.ID    //= platform.ID("cccc")

func init() {
	mapping := platform.DBRPMapping{
		Cluster:         "cluster",
		Database:        "db0",
		RetentionPolicy: "autogen",
		Default:         true,
		OrganizationID:  organizationID,
		BucketID:        bucketID,
	}
	altMapping := platform.DBRPMapping{
		Cluster:         "cluster",
		Database:        "db0",
		RetentionPolicy: "autogen",
		Default:         true,
		OrganizationID:  organizationID,
		BucketID:        altBucketID,
	}
	dbrpMappingSvc.FindByFn = func(ctx context.Context, cluster string, db string, rp string) (*platform.DBRPMapping, error) {
		if rp == "alternate" {
			return &altMapping, nil
		}
		return &mapping, nil
	}
	dbrpMappingSvc.FindFn = func(ctx context.Context, filter platform.DBRPMappingFilter) (*platform.DBRPMapping, error) {
		if filter.RetentionPolicy != nil && *filter.RetentionPolicy == "alternate" {
			return &altMapping, nil
		}
		return &mapping, nil
	}
	dbrpMappingSvc.FindManyFn = func(ctx context.Context, filter platform.DBRPMappingFilter, opt ...platform.FindOptions) ([]*platform.DBRPMapping, int, error) {
		m := &mapping
		if filter.RetentionPolicy != nil && *filter.RetentionPolicy == "alternate" {
			m = &altMapping
		}
		return []*platform.DBRPMapping{m}, 1, nil
	}
}

// Fixture is a structure that will run tests.
type Fixture interface {
	Run(t *testing.T)
}

type fixture struct {
	stmt string
	spec *query.Spec

	file string
	line int
}

func NewFixture(stmt string, spec *query.Spec) Fixture {
	_, file, line, _ := runtime.Caller(1)
	return &fixture{
		stmt: stmt,
		spec: spec,
		file: filepath.Base(file),
		line: line,
	}
}

func (f *fixture) Run(t *testing.T) {
	organizationID = platformtesting.MustIDFromString(t, "aaaaaaaaaaaaaaaa")
	bucketID = platformtesting.MustIDFromString(t, "bbbbbbbbbbbbbbbb")
	altBucketID = platformtesting.MustIDFromString(t, "cccccccccccccccc")

	t.Run(f.stmt, func(t *testing.T) {
		if err := f.spec.Validate(); err != nil {
			t.Fatalf("%s:%d: expected spec is not valid: %s", f.file, f.line, err)
		}

		transpiler := influxql.NewTranspilerWithConfig(
			dbrpMappingSvc,
			influxql.Config{
				NowFn: Now,
			},
		)
		spec, err := transpiler.Transpile(context.Background(), f.stmt)
		if err != nil {
			t.Fatalf("%s:%d: unexpected error: %s", f.file, f.line, err)
		} else if err := spec.Validate(); err != nil {
			t.Fatalf("%s:%d: spec is not valid: %s", f.file, f.line, err)
		}

		// Encode both of these to JSON and compare the results.
		exp, _ := json.Marshal(f.spec)
		got, _ := json.Marshal(spec)
		if !bytes.Equal(exp, got) {
			// Unmarshal into objects so we can compare the key/value pairs.
			var expObj, gotObj interface{}
			json.Unmarshal(exp, &expObj)
			json.Unmarshal(got, &gotObj)

			// If there is no diff, then they were trivial byte differences and
			// there is no error.
			if diff := cmp.Diff(expObj, gotObj); diff != "" {
				t.Fatalf("unexpected spec in test at %s:%d\n%s", f.file, f.line, diff)
			}
		}
	})
}

type collection struct {
	stmts []string
	specs []*query.Spec

	file string
	line int
}

func (c *collection) Add(stmt string, spec *query.Spec) {
	c.stmts = append(c.stmts, stmt)
	c.specs = append(c.specs, spec)
}

func (c *collection) Run(t *testing.T) {
	for i, stmt := range c.stmts {
		f := fixture{
			stmt: stmt,
			spec: c.specs[i],
			file: c.file,
			line: c.line,
		}
		f.Run(t)
	}
}

var allFixtures []Fixture

func RegisterFixture(fixtures ...Fixture) {
	allFixtures = append(allFixtures, fixtures...)
}

func All() []Fixture {
	return allFixtures
}

func Now() time.Time {
	t, err := time.Parse(time.RFC3339, "2010-09-15T09:00:00Z")
	if err != nil {
		panic(err)
	}
	return t
}
