package spectests

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/andreyvit/diff"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/ast/astutil"
	"github.com/influxdata/flux/parser"
	platform "github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/mock"
	"github.com/influxdata/influxdb/v2/query/influxql"
	platformtesting "github.com/influxdata/influxdb/v2/testing"
	"github.com/stretchr/testify/require"
)

var dbrpMappingSvc = &mock.DBRPMappingServiceV2{}
var organizationID platform.ID
var bucketID platform.ID
var altBucketID platform.ID

func init() {
	mapping := platform.DBRPMappingV2{
		Database:        "db0",
		RetentionPolicy: "autogen",
		Default:         true,
		OrganizationID:  organizationID,
		BucketID:        bucketID,
	}
	altMapping := platform.DBRPMappingV2{
		Database:        "db0",
		RetentionPolicy: "autogen",
		Default:         true,
		OrganizationID:  organizationID,
		BucketID:        altBucketID,
	}
	dbrpMappingSvc.FindByIDFn = func(ctx context.Context, orgID, id platform.ID) (*platform.DBRPMappingV2, error) {
		return &mapping, nil
	}
	dbrpMappingSvc.FindManyFn = func(ctx context.Context, filter platform.DBRPMappingFilterV2, opt ...platform.FindOptions) ([]*platform.DBRPMappingV2, int, error) {
		m := &mapping
		if filter.RetentionPolicy != nil && *filter.RetentionPolicy == "alternate" {
			m = &altMapping
		}
		return []*platform.DBRPMappingV2{m}, 1, nil
	}
}

// Fixture is a structure that will run tests.
type Fixture interface {
	Run(t *testing.T)
}

type fixture struct {
	stmt string
	want string

	file string
	line int
}

func NewFixture(stmt, want string) Fixture {
	_, file, line, _ := runtime.Caller(1)
	return &fixture{
		stmt: stmt,
		want: want,
		file: filepath.Base(file),
		line: line,
	}
}

func (f *fixture) Run(t *testing.T) {
	organizationID = platformtesting.MustIDBase16("aaaaaaaaaaaaaaaa")
	bucketID = platformtesting.MustIDBase16("bbbbbbbbbbbbbbbb")
	altBucketID = platformtesting.MustIDBase16("cccccccccccccccc")

	t.Run(f.stmt, func(t *testing.T) {
		wantAST := parser.ParseSource(f.want)
		if ast.Check(wantAST) > 0 {
			err := ast.GetError(wantAST)
			t.Fatalf("found parser errors in the want text: %s", err.Error())
		}
		want, err := astutil.Format(wantAST.Files[0])
		require.NoError(t, err)

		transpiler := influxql.NewTranspilerWithConfig(
			dbrpMappingSvc,
			influxql.Config{
				DefaultDatabase: "db0",
				Cluster:         "cluster",
				Now:             Now(),
			},
		)
		pkg, err := transpiler.Transpile(context.Background(), f.stmt)
		if err != nil {
			t.Fatalf("%s:%d: unexpected error: %s", f.file, f.line, err)
		}
		got, err := astutil.Format(pkg.Files[0])
		require.NoError(t, err)

		// Encode both of these to JSON and compare the results.
		if want != got {
			out := diff.LineDiff(want, got)
			t.Fatalf("unexpected ast at %s:%d\n%s", f.file, f.line, out)
		}
	})
}

type collection struct {
	stmts []string
	wants []string

	file string
	line int
}

func (c *collection) Add(stmt, want string) {
	c.stmts = append(c.stmts, stmt)
	c.wants = append(c.wants, want)
}

func (c *collection) Run(t *testing.T) {
	for i, stmt := range c.stmts {
		f := fixture{
			stmt: stmt,
			want: c.wants[i],
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
