package storageflux

import (
	"testing"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/codes"
	"github.com/influxdata/flux/execute"
	kiterrors "github.com/influxdata/influxdb/kit/platform/errors"
	"github.com/stretchr/testify/require"
)

// These tests pin the error a consumer sees when it claims a table too late:
// losing the used CAS to an abandoning producer must report cancellation
// (codes.Canceled), because on cancellation this error can win the race to be
// the query's reported error and the controller maps it to a 4xx. Losing the
// CAS to another consumer - a genuine misuse - must keep the internal error.

// newBareTable builds a *table with no cursor and no buffer, sufficient for
// exercising the used-CAS claim paths. empty is set so a successful do() does
// not touch colBufs.
func newBareTable() (*table, chan struct{}) {
	done := make(chan struct{})
	cols := []flux.ColMeta{{Label: execute.DefaultValueColLabel, Type: flux.TFloat}}
	tbl := newTable(done, execute.Bounds{}, nil, cols, make([][]byte, len(cols)), nil, nil)
	tbl.empty = true
	return &tbl, done
}

func nopColReader(flux.ColReader) error { return nil }
func noAdvance() bool                   { return false }

func TestTable_DoAfterAbandonReturnsCancelled(t *testing.T) {
	tbl, done := newBareTable()
	tbl.awaitAbandoned(done)

	err := tbl.do(nopColReader, noAdvance)
	var ferr *flux.Error
	require.ErrorAs(t, err, &ferr)
	require.Equal(t, codes.Canceled, ferr.Code)
}

// TestTable_DoTwiceReturnsAlreadyUsed is the control: losing the CAS to a
// consumer, not an abandoning producer, is still the internal misuse error.
func TestTable_DoTwiceReturnsAlreadyUsed(t *testing.T) {
	tbl, _ := newBareTable()
	require.NoError(t, tbl.do(nopColReader, noAdvance))

	err := tbl.do(nopColReader, noAdvance)
	require.EqualError(t, err, "table already used")
	var ferr *flux.Error
	require.NotErrorAs(t, err, &ferr, "double use must not be reported as cancellation")
}

func TestWindowTableRow_DoAfterAbandonReturnsCancelled(t *testing.T) {
	done := make(chan struct{})
	row := &windowTableRow{done: done}
	row.abandon(done)

	err := row.Do(nopColReader)
	var ferr *flux.Error
	require.ErrorAs(t, err, &ferr)
	require.Equal(t, codes.Canceled, ferr.Code)
}

// TestWindowTableRow_DoAfterDoneReturnsAlreadyRead is the control: a row
// claimed by Done without abandonment keeps the internal misuse error.
func TestWindowTableRow_DoAfterDoneReturnsAlreadyRead(t *testing.T) {
	row := &windowTableRow{done: make(chan struct{})}
	row.Done()

	err := row.Do(nopColReader)
	var kerr *kiterrors.Error
	require.ErrorAs(t, err, &kerr)
	require.Equal(t, kiterrors.EInternal, kerr.Code)
}
