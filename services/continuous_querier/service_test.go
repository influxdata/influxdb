package continuous_querier

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/meta"
)

var (
	expectedErr   = errors.New("expected error")
	unexpectedErr = errors.New("unexpected error")
)

// Test closing never opened, open, open already open, close, and close already closed.
func TestOpenAndClose(t *testing.T) {
	s := NewTestService()

	if err := s.Close(); err != nil {
		t.Error(err)
	} else if err = s.Open(); err != nil {
		t.Error(err)
	} else if err = s.Open(); err != nil {
		t.Error(err)
	} else if err = s.Close(); err != nil {
		t.Error(err)
	} else if err = s.Close(); err != nil {
		t.Error(err)
	}
}

// Test ExecuteContinuousQuery happy path.
func TestExecuteContinuousQuery_HappyPath(t *testing.T) {
	s := NewTestService()
	dbis, _ := s.MetaStore.Databases()
	dbi := dbis[0]
	cqi := dbi.ContinuousQueries[0]

	pointCnt := 1000
	qe := s.QueryExecutor.(*QueryExecutor)
	qe.Results = []*influxql.Result{genResult(1, pointCnt)}

	pw := s.PointsWriter.(*PointsWriter)
	pw.WritePointsFn = func(p *cluster.WritePointsRequest) error {
		if len(p.Points) != pointCnt {
			return fmt.Errorf("exp = %d, got = %d", pointCnt, len(p.Points))
		}
		return nil
	}

	err := s.ExecuteContinuousQuery(&dbi, &cqi)
	if err != nil {
		t.Error(err)
	}
}

// Test the service happy path.
func TestService_HappyPath(t *testing.T) {
	s := NewTestService()

	pointCnt := 1000
	qe := s.QueryExecutor.(*QueryExecutor)
	qe.Results = []*influxql.Result{genResult(1, pointCnt)}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	pw := s.PointsWriter.(*PointsWriter)
	pw.WritePointsFn = func(p *cluster.WritePointsRequest) error {
		defer wg.Done()
		if len(p.Points) != pointCnt {
			return fmt.Errorf("exp = %d, got = %d", pointCnt, len(p.Points))
		}
		return nil
	}

	s.Open()
	// Test will timeout if the query doesn't process.
	wg.Wait()
	s.Close()
}

// Test ExecuteContinuousQuery with invalid queries.
func TestExecuteContinuousQuery_InvalidQueries(t *testing.T) {
	s := NewTestService()
	dbis, _ := s.MetaStore.Databases()
	dbi := dbis[0]
	cqi := dbi.ContinuousQueries[0]

	cqi.Query = `this is not a query`
	err := s.ExecuteContinuousQuery(&dbi, &cqi)
	if err == nil {
		t.Error("expected error but got nil")
	}

	// Valid query but invalid continuous query.
	cqi.Query = `SELECT * FROM cpu`
	err = s.ExecuteContinuousQuery(&dbi, &cqi)
	if err == nil {
		t.Error("expected error but got nil")
	}

	// Group by requires aggregate.
	cqi.Query = `SELECT value INTO other_value FROM cpu WHERE time > now() - 1d GROUP BY time(1h)`
	err = s.ExecuteContinuousQuery(&dbi, &cqi)
	if err == nil {
		t.Error("expected error but got nil")
	}
}

// Test ExecuteContinuousQuery when QueryExecutor returns an error.
func TestExecuteContinuousQuery_QueryExecutor_Error(t *testing.T) {
	s := NewTestService()
	qe := s.QueryExecutor.(*QueryExecutor)
	qe.Err = expectedErr

	dbis, _ := s.MetaStore.Databases()
	dbi := dbis[0]
	cqi := dbi.ContinuousQueries[0]

	err := s.ExecuteContinuousQuery(&dbi, &cqi)
	if err != expectedErr {
		t.Errorf("exp = %s, got = %v", expectedErr, err)
	}
}

// NewTestService returns a new *Service with default mock object members.
func NewTestService() *Service {
	s := NewService(NewConfig())
	s.MetaStore = NewMetaStore()
	s.QueryExecutor = NewQueryExecutor()
	s.PointsWriter = NewPointsWriter()

	// Set Logger to write to dev/null so stdout isn't polluted.
	null, _ := os.Open(os.DevNull)
	s.Logger = log.New(null, "", 0)

	return s
}

// MetaStore is a mock meta store.
type MetaStore struct {
	Leader        bool
	DatabaseInfos []meta.DatabaseInfo
	Err           error
}

// NewMetaStore returns a *MetaStore.
func NewMetaStore() *MetaStore {
	return &MetaStore{
		Leader: true,
		DatabaseInfos: []meta.DatabaseInfo{
			{
				Name: "db",
				DefaultRetentionPolicy: "rp",
				ContinuousQueries: []meta.ContinuousQueryInfo{
					{
						Name:  "cq",
						Query: `SELECT count(cpu) INTO cpu_count FROM cpu WHERE time > now() - 1d GROUP BY time(1h)`,
					},
				},
			},
		},
	}
}

// IsLeader returns true if the node is the cluster leader.
func (ms *MetaStore) IsLeader() bool { return ms.Leader }

// Databases returns a list of database info about each database in the cluster.
func (ms *MetaStore) Databases() ([]meta.DatabaseInfo, error) { return ms.DatabaseInfos, ms.Err }

// QueryExecutor is a mock query executor.
type QueryExecutor struct {
	ExecuteQueryFn      func(query *influxql.Query, database string, chunkSize int) error
	Results             []*influxql.Result
	ResultInterval      time.Duration
	ResultCh            chan *influxql.Result
	Err                 error
	ErrAfterResult      int
	StopRespondingAfter int
	Wg                  *sync.WaitGroup
}

// NewQueryExecutor returns a *QueryExecutor.
func NewQueryExecutor() *QueryExecutor {
	return &QueryExecutor{
		ResultCh:            make(chan *influxql.Result, 0),
		ErrAfterResult:      -1,
		StopRespondingAfter: -1,
	}
}

// ExecuteQuery returns a channel that the caller can read query results from.
func (qe *QueryExecutor) ExecuteQuery(query *influxql.Query, database string, chunkSize int) (<-chan *influxql.Result, error) {
	// Call the callback if set.
	if qe.ExecuteQueryFn != nil {
		if err := qe.ExecuteQueryFn(query, database, chunkSize); err != nil {
			return nil, err
		}
	}

	// Are we supposed to error immediately?
	if qe.ErrAfterResult == -1 && qe.Err != nil {
		return nil, qe.Err
	}

	qe.Wg = &sync.WaitGroup{}
	qe.Wg.Add(1)

	// Start a go routine to send results and / or error.
	go func() {
		defer qe.Wg.Done()
		for i, r := range qe.Results {
			if i == qe.ErrAfterResult {
				qe.ResultCh <- &influxql.Result{Err: qe.Err}
				close(qe.ResultCh)
				return
			} else if i == qe.StopRespondingAfter {
				return
			}

			qe.ResultCh <- r
			time.Sleep(qe.ResultInterval)
		}
		close(qe.ResultCh)
	}()

	return qe.ResultCh, nil
}

// PointsWriter is a mock points writer.
type PointsWriter struct {
	WritePointsFn   func(p *cluster.WritePointsRequest) error
	Err             error
	PointsPerSecond int
}

// NewPointsWriter returns a new *PointsWriter.
func NewPointsWriter() *PointsWriter {
	return &PointsWriter{
		PointsPerSecond: 25000,
	}
}

func (pw *PointsWriter) WritePoints(p *cluster.WritePointsRequest) error {
	if pw.WritePointsFn != nil {
		if err := pw.WritePointsFn(p); err != nil {
			return err
		}
	}

	if pw.Err != nil {
		return pw.Err
	}
	ns := time.Duration((1 / pw.PointsPerSecond) * 1000000000)
	time.Sleep(ns)
	return nil
}

// genResult generates a dummy query result.
func genResult(rowCnt, valCnt int) *influxql.Result {
	rows := make(influxql.Rows, 0, rowCnt)
	now := time.Now()
	for n := 0; n < rowCnt; n++ {
		vals := make([][]interface{}, 0, valCnt)
		for m := 0; m < valCnt; m++ {
			vals = append(vals, []interface{}{now, float64(m)})
			now.Add(time.Second)
		}
		row := &influxql.Row{
			Name:    "cpu",
			Tags:    map[string]string{"host": "server01"},
			Columns: []string{"time", "value"},
			Values:  vals,
		}
		rows = append(rows, row)
	}
	return &influxql.Result{
		Series: rows,
	}
}
