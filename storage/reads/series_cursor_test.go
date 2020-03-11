package reads

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
	"github.com/influxdata/influxql"
)

func TestPlannerCondition(t *testing.T) {
	sqry := &floatIterator{
		Points: []storage.SeriesCursorRow{
			{
				Name: []byte("org_bucket"), Tags: models.Tags{
					{Key: models.MeasurementTagKeyBytes, Value: []byte("cpu")},
					{Key: []byte("host"), Value: []byte("host1")},
					{Key: models.FieldKeyTagKeyBytes, Value: []byte("system")},
				},
			},
			{
				Name: []byte("org_bucket"), Tags: models.Tags{
					{Key: models.MeasurementTagKeyBytes, Value: []byte("mem")},
					{Key: []byte("host"), Value: []byte("host1")},
					{Key: models.FieldKeyTagKeyBytes, Value: []byte("user")},
				},
			},
		},
	}

	expr := fmt.Sprintf(`(%[1]s = 'cpu' AND (%[2]s = 'user' OR %[2]s = 'system')) OR (%[1]s = 'mem' AND "_value" = 0)`, datatypes.MeasurementKey, datatypes.FieldKey)
	cond, err := parseExpr(expr)
	if err != nil {
		t.Fatal("ParseExpr", err)
	}

	p := &indexSeriesCursor{
		sqry:         sqry,
		cond:         cond,
		hasValueExpr: true,
	}

	var keys []string
	// In first row, value cond should reduce to "true" and be nil.
	row := p.Next()
	if row.ValueCond != nil {
		t.Errorf("expected nil ValueCond, got %s", row.ValueCond)
	}
	keys = append(keys, string(models.MakeKey(row.Name, row.Tags)))

	// In second row, the value condition applies.
	row = p.Next()
	if want, got := "_value = 0", row.ValueCond.String(); !cmp.Equal(want, got) {
		t.Errorf("unexpected, %s", cmp.Diff(want, got))
	}
	keys = append(keys, string(models.MakeKey(row.Name, row.Tags)))

	expr = `org_bucket,%[2]s=system,%[1]s=cpu,host=host1
org_bucket,%[2]s=user,%[1]s=mem,host=host1`

	expr = fmt.Sprintf(expr, datatypes.MeasurementKey, datatypes.FieldKey)

	exp := strings.Split(expr, "\n")
	if !cmp.Equal(exp, keys) {
		t.Errorf("unexpected, %s", cmp.Diff(exp, keys))
	}
}

// parseExpr parses the given InfluxQL expression and rewrites
// _measurement and _field vars as their storage tag key equivalents.
func parseExpr(expr string) (influxql.Expr, error) {
	e, err := influxql.ParseExpr(expr)
	if err != nil {
		return nil, err
	}

	e = influxql.RewriteExpr(e, func(expr influxql.Expr) influxql.Expr {
		if vr, ok := expr.(*influxql.VarRef); ok {
			switch vr.Val {
			case datatypes.MeasurementKey:
				vr.Val = models.MeasurementTagKey
			case datatypes.FieldKey:
				vr.Val = models.FieldKeyTagKey
			}
		}
		return expr
	})

	return e, nil
}

// floatIterator is a represents an iterator that reads from a slice.
type floatIterator struct {
	Points []storage.SeriesCursorRow
}

// Close is a no-op closer for testing.
func (itr *floatIterator) Close() {
}

func (itr *floatIterator) Next() (*storage.SeriesCursorRow, error) {
	if len(itr.Points) == 0 {
		return nil, nil
	}

	v := &itr.Points[0]
	itr.Points = itr.Points[1:]
	return v, nil
}
