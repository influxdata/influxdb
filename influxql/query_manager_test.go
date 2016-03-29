package influxql_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/influxdb/influxql"
)

func TestQueryManager_AttachQuery(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	qm := influxql.DefaultQueryManager(0)
	params := influxql.QueryParams{
		Query:    q,
		Database: `mydb`,
	}

	qid, _, err := qm.AttachQuery(&params)
	if err != nil {
		t.Fatal(err)
	}
	defer qm.KillQuery(qid)

	if qid != 1 {
		t.Errorf("incorrect query id: exp=1 got=%d", qid)
	}
}

func TestQueryManager_KillQuery(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	qm := influxql.DefaultQueryManager(0)
	params := influxql.QueryParams{
		Query:    q,
		Database: `mydb`,
	}

	qid, ch, err := qm.AttachQuery(&params)
	if err != nil {
		t.Fatal(err)
	}
	qm.KillQuery(qid)

	select {
	case <-ch:
	case <-time.After(100 * time.Millisecond):
		t.Error("killing the query did not close the channel after 100 milliseconds")
	}

	if err := qm.KillQuery(qid); err == nil || err.Error() != fmt.Sprintf("no such query id: %d", qid) {
		t.Errorf("incorrect error killing query, got %s", err)
	}
}

func TestQueryManager_Interrupt(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	closing := make(chan struct{})
	qm := influxql.DefaultQueryManager(0)
	params := influxql.QueryParams{
		Query:       q,
		Database:    `mydb`,
		InterruptCh: closing,
	}

	_, ch, err := qm.AttachQuery(&params)
	if err != nil {
		t.Fatal(err)
	}
	close(closing)

	select {
	case <-ch:
	case <-time.After(100 * time.Millisecond):
		t.Error("interrupting the query did not close the channel after 100 milliseconds")
	}
}

func TestQueryManager_Queries(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	qm := influxql.DefaultQueryManager(0)
	params := influxql.QueryParams{
		Query:    q,
		Database: `mydb`,
	}

	qid, _, err := qm.AttachQuery(&params)
	if err != nil {
		t.Fatal(err)
	}

	queries := qm.Queries()
	if len(queries) != 1 {
		t.Errorf("expected 1 query, got %d", len(queries))
	} else {
		qi := queries[0]
		if qi.ID != qid {
			t.Errorf("query id: exp=%d got=%d", qid, qi.ID)
		}
		if qi.Query != `SELECT count(value) FROM cpu` {
			t.Errorf("query id: incorrect query string, got '%s'", qi.Query)
		}
		if qi.Database != "mydb" {
			t.Errorf("query id: incorrect database, got %s", qi.Database)
		}
	}

	qm.KillQuery(qid)
	queries = qm.Queries()
	if len(queries) != 0 {
		t.Errorf("expected 0 queries, got %d", len(queries))
	}
}

func TestQueryManager_Limit_Timeout(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	qm := influxql.DefaultQueryManager(0)
	params := influxql.QueryParams{
		Query:    q,
		Database: `mydb`,
		Timeout:  time.Nanosecond,
	}

	_, ch, err := qm.AttachQuery(&params)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Errorf("timeout has not killed the query")
	}
}

func TestQueryManager_Limit_ConcurrentQueries(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	qm := influxql.DefaultQueryManager(1)
	params := influxql.QueryParams{
		Query:    q,
		Database: `mydb`,
	}

	qid, _, err := qm.AttachQuery(&params)
	if err != nil {
		t.Fatal(err)
	}
	defer qm.KillQuery(qid)

	_, _, err = qm.AttachQuery(&params)
	if err == nil || err != influxql.ErrMaxConcurrentQueriesReached {
		t.Errorf("unexpected error: %s", err)
	}
}

func TestQueryManager_Close(t *testing.T) {
	q, err := influxql.ParseQuery(`SELECT count(value) FROM cpu`)
	if err != nil {
		t.Fatal(err)
	}

	qm := influxql.DefaultQueryManager(0)
	params := influxql.QueryParams{
		Query:    q,
		Database: `mydb`,
	}

	_, ch, err := qm.AttachQuery(&params)
	if err != nil {
		t.Fatal(err)
	}
	qm.Close()

	select {
	case <-ch:
	case <-time.After(100 * time.Millisecond):
		t.Error("closing the query manager did not kill the query after 100 milliseconds")
	}

	_, _, err = qm.AttachQuery(&params)
	if err == nil || err != influxql.ErrQueryManagerShutdown {
		t.Errorf("unexpected error: %s", err)
	}
}
