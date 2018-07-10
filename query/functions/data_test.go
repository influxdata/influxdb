package functions_test

import (
	"math/rand"
	"time"

	"github.com/gonum/stat/distuv"
	"github.com/influxdata/platform/query"
	_ "github.com/influxdata/platform/query/builtin"
	"github.com/influxdata/platform/query/execute"
	"github.com/influxdata/platform/query/execute/executetest"
	"github.com/influxdata/platform/query/values"
)

const (
	N     = 1e6
	Mu    = 10
	Sigma = 3

	seed = 42
)

// NormalData is a slice of N random values that are normaly distributed with mean Mu and standard deviation Sigma.
var NormalData []float64

// NormalTable is a table of data whose value col is NormalData.
var NormalTable query.Table

func init() {
	dist := distuv.Normal{
		Mu:     Mu,
		Sigma:  Sigma,
		Source: rand.New(rand.NewSource(seed)),
	}
	NormalData = make([]float64, N)
	for i := range NormalData {
		NormalData[i] = dist.Rand()
	}
	start := execute.Time(time.Date(2016, 10, 10, 0, 0, 0, 0, time.UTC).UnixNano())
	stop := execute.Time(time.Date(2017, 10, 10, 0, 0, 0, 0, time.UTC).UnixNano())
	t1Value := "a"
	key := execute.NewGroupKey(
		[]query.ColMeta{
			{Label: execute.DefaultStartColLabel, Type: query.TTime},
			{Label: execute.DefaultStopColLabel, Type: query.TTime},
			{Label: "t1", Type: query.TString},
		},
		[]values.Value{
			values.NewTimeValue(start),
			values.NewTimeValue(stop),
			values.NewStringValue(t1Value),
		},
	)
	normalTableBuilder := execute.NewColListTableBuilder(key, executetest.UnlimitedAllocator)

	normalTableBuilder.AddCol(query.ColMeta{Label: execute.DefaultTimeColLabel, Type: query.TTime})
	normalTableBuilder.AddCol(query.ColMeta{Label: execute.DefaultStartColLabel, Type: query.TTime})
	normalTableBuilder.AddCol(query.ColMeta{Label: execute.DefaultStopColLabel, Type: query.TTime})
	normalTableBuilder.AddCol(query.ColMeta{Label: execute.DefaultValueColLabel, Type: query.TFloat})
	normalTableBuilder.AddCol(query.ColMeta{Label: "t1", Type: query.TString})
	normalTableBuilder.AddCol(query.ColMeta{Label: "t2", Type: query.TString})

	times := make([]execute.Time, N)
	startTimes := make([]execute.Time, N)
	stopTimes := make([]execute.Time, N)
	values := NormalData
	t1 := make([]string, N)
	t2 := make([]string, N)

	for i, v := range values {
		startTimes[i] = start
		stopTimes[i] = stop
		t1[i] = t1Value
		// There are roughly 1 million, 31 second intervals in a year.
		times[i] = start + execute.Time(time.Duration(i*31)*time.Second)
		// Pick t2 based off the value
		switch int(v) % 3 {
		case 0:
			t2[i] = "x"
		case 1:
			t2[i] = "y"
		case 2:
			t2[i] = "z"
		}
	}

	normalTableBuilder.AppendTimes(0, times)
	normalTableBuilder.AppendTimes(1, startTimes)
	normalTableBuilder.AppendTimes(2, stopTimes)
	normalTableBuilder.AppendFloats(3, values)
	normalTableBuilder.AppendStrings(4, t1)
	normalTableBuilder.AppendStrings(5, t2)

	NormalTable, _ = normalTableBuilder.Table()
}
