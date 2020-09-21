package promqltests

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/lang"
	fpromql "github.com/influxdata/flux/promql"
	"github.com/influxdata/influxdb/v2/cmd/influxd/launcher"
	"github.com/influxdata/influxdb/v2/models"
	"github.com/influxdata/influxdb/v2/query"
	itsdb "github.com/influxdata/influxdb/v2/tsdb"
	ipromql "github.com/influxdata/promql/v2"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/tsdb"
	"github.com/prometheus/tsdb/wal"
)

type TestEngine struct {
	inputPath  string
	influxDB   *launcher.TestLauncher
	promDB     storage.Storage
	promEngine *promql.Engine
}

func NewTestEngine(inputPath string) (*TestEngine, error) {
	l := launcher.NewTestLauncher()
	if err := l.Run(context.Background()); err != nil {
		return nil, err
	}
	if err := l.Setup(); err != nil {
		return nil, err
	}

	tsdbPath, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, fmt.Errorf("error while creating temporary directory: %v", err)
	}
	db, err := tsdb.Open(tsdbPath, nil, nil, &tsdb.Options{
		WALSegmentSize:    wal.DefaultSegmentSize,
		RetentionDuration: 99999 * 24 * 60 * 60 * model.Duration(time.Second),
		MinBlockDuration:  model.Duration(2 * time.Hour),
		MaxBlockDuration:  model.Duration(2 * time.Hour),
	})
	if err != nil {
		return nil, err
	}
	db.DisableCompactions()
	te := &TestEngine{
		inputPath: inputPath,
		influxDB:  l,
		promDB:    tsdb.Adapter(db, 0),
		promEngine: promql.NewEngine(promql.EngineOpts{
			MaxConcurrent: 10,
			MaxSamples:    1e12,
			Timeout:       time.Hour,
		}),
	}
	if err := te.writeInput(); err != nil {
		return nil, err
	}
	return te, nil
}

func (e *TestEngine) writeInput() error {
	if err := e.writeToInfluxDB(); err != nil {
		return err
	}
	return e.writeToPromDB()
}

func (e *TestEngine) writeToInfluxDB() error {
	inputFile, err := os.Open(e.inputPath)
	if err != nil {
		return err
	}
	defer inputFile.Close()
	data, err := ioutil.ReadAll(inputFile)
	if err != nil {
		return err
	}
	if err := e.influxDB.WritePoints(string(data)); err != nil {
		return err
	}
	return nil
}

func (e *TestEngine) writeToPromDB() error {
	inputFile, err := os.Open(e.inputPath)
	if err != nil {
		return err
	}
	defer inputFile.Close()
	scanner := bufio.NewScanner(inputFile)
	app, err := e.promDB.Appender()
	if err != nil {
		return err
	}
	// no matter which org or bucket
	name := itsdb.EncodeName(1, 2)
	for scanner.Scan() {
		points, err := models.ParsePoints(scanner.Bytes(), name[:])
		if err != nil {
			return err
		}
		for _, p := range points {
			ls := make([]labels.Label, 0)
			p.ForEachTag(func(k, v []byte) bool {
				if !bytes.Equal(k, models.FieldKeyTagKeyBytes) && !bytes.Equal(k, models.MeasurementTagKeyBytes) {
					ls = append(ls, labels.Label{
						Name:  string(k),
						Value: string(v),
					})
				}
				return true
			})
			ts := p.Time()
			fields := p.FieldIterator()
			for fields.Next() {
				k := string(fields.FieldKey())
				v, err := fields.FloatValue()
				if err != nil {
					return err
				}
				lb := labels.NewBuilder(ls)
				lb.Set("__name__", k)
				if _, err := app.Add(lb.Labels(), ts.UnixNano()/1e6, v); err != nil {
					return err
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return app.Commit()
}

func (e *TestEngine) Close() error {
	if err := e.promDB.Close(); err != nil {
		return err
	}
	return e.influxDB.Shutdown(context.Background())
}

func (e *TestEngine) Test(t *testing.T, q string, skipComparison string, shouldFail bool, start, end time.Time, resolution time.Duration) {
	// Transpile PromQL into Flux.
	promNode, err := ipromql.ParseExpr(q)
	if err != nil {
		t.Fatalf("error parsing PromQL expression %s: %s", q, err)
	}
	tr := &fpromql.Transpiler{
		Bucket:     e.influxDB.Bucket.Name,
		Start:      start,
		End:        end,
		Resolution: resolution,
	}
	fluxFile, trErr := tr.Transpile(promNode)
	if trErr != nil && !shouldFail {
		t.Fatalf("error transpiling PromQL expression %s to Flux: %s", q, err)
	}

	// Query Prometheus.
	rq, err := e.promEngine.NewRangeQuery(e.promDB, q, start, end, resolution)
	if err != nil {
		t.Fatalf("error creating PromQL range query for %s: %s", q, err)
	}
	defer rq.Close()
	promResult := rq.Exec(context.Background())
	if (promResult.Err != nil) != shouldFail {
		if promResult.Err != nil {
			t.Fatalf("error querying Prometheus for %s: %s", q, promResult.Err)
		}
		t.Fatalf("expected PromQL query %s to fail, but succeeded", q)
	}
	var promMatrix promql.Matrix
	if !shouldFail {
		promMatrix, err = promResult.Matrix()
		if err != nil {
			t.Fatalf("error converting Prometheus result for %s to Matrix: %s", q, err)
		}
	}

	// Query InfluxDB.
	req := &query.Request{
		Authorization:  e.influxDB.Auth,
		OrganizationID: e.influxDB.Org.ID,
		Compiler: lang.ASTCompiler{
			AST: &ast.Package{Package: "main", Files: []*ast.File{fluxFile}},
			Now: time.Now(),
		},
	}
	var influxMatrix promql.Value = promql.Matrix{}
	err = e.influxDB.QueryAndConsume(context.Background(), req, func(r flux.Result) error {
		m, err := FluxResultToPromQLValue(r, promql.ValueTypeMatrix)
		influxMatrix = m
		return err
	})
	if (err != nil) != shouldFail {
		if err != nil {
			t.Fatalf("error querying InfluxDB for %s: %v\n\nFlux script:\n\n%s", q, err, ast.Format(fluxFile))
		}
		t.Fatalf("expected Flux query for %s to fail, but succeeded. Flux script:\n\n%s\n\nOutput matrix: %s", q, ast.Format(fluxFile), influxMatrix.String())
	}

	if len(skipComparison) > 0 {
		t.Logf("query %s has been compiled, transpiled, and run with no errors, but the comparison must be skipped: %s", q, skipComparison)
		return
	}
	if shouldFail {
		return
	}

	cmpOpts := cmp.Options{
		// Translate sample values into float64 so that cmpopts.EquateApprox() works.
		cmp.Transformer("", func(in model.SampleValue) float64 {
			return float64(in)
		}),
		// Allow comparison tolerances due to floating point inaccuracy.
		cmpopts.EquateApprox(0.0000000000001, 0),
		cmpopts.EquateNaNs(),
		cmpopts.EquateEmpty(),
	}
	if diff := cmp.Diff(promMatrix, influxMatrix, cmpOpts); diff != "" {
		t.Fatal(
			"FAILED! Prometheus and InfluxDB results differ:\n\n", diff,
			"\nPromQL query was:\n============================================\n", q, "\n============================================\n\n",
			"\nFlux query was:\n============================================\n", ast.Format(fluxFile), "\n============================================\n\n",
			"\nFull results:",
			"\n=== InfluxDB results:\n", influxMatrix,
			"\n=== Prometheus results:\n", promMatrix,
		)
	}
}
