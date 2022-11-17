package aggregators

import (
	"bytes"
	"sync"
	"testing"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/stretchr/testify/require"
)

type result struct {
	fields uint64
	tags   uint64
	series uint64
}

type test struct {
	db  string
	rp  string
	key []byte
}

// Ensure that tags and fields and series which differ only in database, retention policy, or measurement
// are correctly counted.
func Test_canonicalize(t *testing.T) {
	totalDepth := 3

	// measurement,tag1=tag1_value1,tag2=tag2_value1#!~#field1
	tests := []test{
		{
			db:  "db1",
			rp:  "rp1",
			key: []byte("m1,t1=t1_v1,t2=t2_v1#!~#f1"),
		},
		{
			db:  "db1",
			rp:  "rp1",
			key: []byte("m1,t1=t1_v2,t2=t2_v1#!~#f1"),
		},
		{
			db:  "db1",
			rp:  "rp1",
			key: []byte("m1,t1=t1_v1,t2=t2_v2#!~#f1"),
		},
		{
			db:  "db1",
			rp:  "rp1",
			key: []byte("m1,t1=t1_v2,t2=t2_v2#!~#f1"),
		},
		{
			db:  "db1",
			rp:  "rp1",
			key: []byte("m1,t1=t1_v2,t2=t2_v2#!~#f2"),
		},
		{
			db:  "db1",
			rp:  "rp2",
			key: []byte("m1,t1=t1_v1,t2=t2_v1#!~#f1"),
		},
		{
			db:  "db1",
			rp:  "rp2",
			key: []byte("m1,t1=t1_v2,t2=t2_v1#!~#f1"),
		},
		{
			db:  "db1",
			rp:  "rp2",
			key: []byte("m1,t1=t1_v1,t2=t2_v2#!~#f1"),
		},
		{
			db:  "db1",
			rp:  "rp2",
			key: []byte("m1,t1=t1_v2,t2=t2_v2#!~#f3"),
		},
		{
			db:  "db1",
			rp:  "rp2",
			key: []byte("m1,t1=t1_v2,t2=t2_v2#!~#f2"),
		},
		{
			db:  "db1",
			rp:  "rp1",
			key: []byte("m2,t1=t1_v1,t2=t2_v1#!~#f1"),
		},
		{
			db:  "db1",
			rp:  "rp1",
			key: []byte("m2,t1=t1_v2,t2=t2_v1#!~#f1"),
		},
		{
			db:  "db1",
			rp:  "rp1",
			key: []byte("m2,t1=t1_v1,t2=t2_v2#!~#f1"),
		},
		{
			db:  "db1",
			rp:  "rp1",
			key: []byte("m2,t1=t1_v2,t2=t2_v2#!~#f1"),
		},
		{
			db:  "db1",
			rp:  "rp1",
			key: []byte("m2,t1=t1_v2,t2=t2_v2#!~#f2"),
		},
		{
			db:  "db1",
			rp:  "rp2",
			key: []byte("m2,t1=t1_v1,t2=t2_v1#!~#f1"),
		},
		{
			db:  "db1",
			rp:  "rp2",
			key: []byte("m2,t1=t1_v2,t2=t2_v1#!~#f1"),
		},
		{
			db:  "db1",
			rp:  "rp2",
			key: []byte("m2,t1=t1_v1,t2=t2_v2#!~#f1"),
		},
		{
			db:  "db1",
			rp:  "rp2",
			key: []byte("m2,t1=t1_v2,t2=t2_v2#!~#f1"),
		},
		{
			db:  "db1",
			rp:  "rp2",
			key: []byte("m2,t1=t1_v2,t2=t2_v2#!~#f2"),
		},
		{
			db:  "db2",
			rp:  "rp1",
			key: []byte("m1,t1=t1_v1,t2=t2_v1#!~#f1"),
		},
		{
			db:  "db2",
			rp:  "rp1",
			key: []byte("m1,t1=t1_v2,t2=t2_v1#!~#f1"),
		},
		{
			db:  "db2",
			rp:  "rp1",
			key: []byte("m1,t1=t1_v1,t2=t2_v2#!~#f1"),
		},
		{
			db:  "db2",
			rp:  "rp1",
			key: []byte("m1,t1=t1_v2,t2=t2_v2#!~#f1"),
		},
		{
			db:  "db2",
			rp:  "rp1",
			key: []byte("m1,t1=t1_v2,t2=t2_v2#!~#f2"),
		},
		{
			db:  "db2",
			rp:  "rp2",
			key: []byte("m1,t1=t1_v1,t2=t2_v1#!~#f1"),
		},
		{
			db:  "db2",
			rp:  "rp2",
			key: []byte("m1,t1=t1_v2,t2=t2_v1#!~#f1"),
		},
		{
			db:  "db2",
			rp:  "rp2",
			key: []byte("m1,t1=t1_v1,t2=t2_v2#!~#f1"),
		},
		{
			db:  "db2",
			rp:  "rp2",
			key: []byte("m1,t1=t1_v2,t2=t2_v2#!~#f1"),
		},
		{
			db:  "db2",
			rp:  "rp2",
			key: []byte("m1,t1=t1_v2,t2=t2_v2#!~#f2"),
		},
		{
			db:  "db2",
			rp:  "rp1",
			key: []byte("m2,t1=t1_v1,t2=t2_v1#!~#f1"),
		},
		{
			db:  "db2",
			rp:  "rp1",
			key: []byte("m2,t1=t1_v2,t2=t2_v1#!~#f1"),
		},
		{
			db:  "db2",
			rp:  "rp1",
			key: []byte("m2,t1=t1_v1,t2=t2_v2#!~#f1"),
		},
		{
			db:  "db2",
			rp:  "rp1",
			key: []byte("m2,t1=t1_v2,t2=t2_v2#!~#f1"),
		},
		{
			db:  "db2",
			rp:  "rp1",
			key: []byte("m2,t1=t1_v2,t2=t2_v2#!~#f2"),
		},
		{
			db:  "db2",
			rp:  "rp2",
			key: []byte("m2,t1=t1_v1,t2=t2_v1#!~#f1"),
		},
		{
			db:  "db2",
			rp:  "rp2",
			key: []byte("m2,t1=t1_v2,t2=t2_v1#!~#f1"),
		},
		{
			db:  "db2",
			rp:  "rp2",
			key: []byte("m2,t1=t1_v1,t2=t2_v2#!~#f1"),
		},
		{
			db:  "db2",
			rp:  "rp2",
			key: []byte("m2,t1=t1_v2,t2=t2_v2#!~#f1"),
		},
		{
			db:  "db2",
			rp:  "rp2",
			key: []byte("m2,t1=t1_v2,t2=t2_v2#!~#f2"),
		},
	}

	results := map[string]map[string]map[string]*result{
		"db1": {
			"rp1": {
				"m1": {2, 4, 5},
				"m2": {2, 4, 5},
				"":   {4, 8, 10},
			},
			"rp2": {
				"m1": {3, 4, 5},
				"m2": {2, 4, 5},
				"":   {5, 8, 10},
			},
			"": {
				"": {9, 16, 20},
			},
		},
		"db2": {
			"rp1": {
				"m1": {2, 4, 5},
				"m2": {2, 4, 5},
				"":   {4, 8, 10},
			},
			"rp2": {
				"m1": {2, 4, 5},
				"m2": {2, 4, 5},
				"":   {4, 8, 10},
			},
			"": {
				"": {8, 16, 20},
			},
		},
		"": {
			"": {
				"": {17, 32, 40},
			},
		},
	}

	testLoop(t, false, true, totalDepth, tests, results)
	testLoop(t, true, true, totalDepth, tests, results)
	testLoop(t, false, false, totalDepth, tests, results)
	testLoop(t, true, false, totalDepth, tests, results)

}

func testLoop(t *testing.T, detailed bool, exact bool, totalDepth int, tests []test, results map[string]map[string]map[string]*result) {
	factory := CreateNodeFactory(detailed, exact)
	tree := factory.NewNode(totalDepth == 0)

	wg := sync.WaitGroup{}
	tf := func() {
		for i := range tests {
			seriesKey, field, _ := bytes.Cut(tests[i].key, []byte("#!~#"))
			measurement, tags := models.ParseKey(seriesKey)
			tree.Record(0, totalDepth, tests[i].db, tests[i].rp, measurement, tests[i].key, field, tags)
		}
		wg.Done()
	}
	const concurrency = 5
	wg.Add(concurrency)
	for j := 0; j < concurrency; j++ {
		go tf()
	}
	wg.Wait()

	for d, db := range tree.Children() {
		for r, rp := range db.Children() {
			for m, measure := range rp.Children() {
				checkNode(t, measure, results[d][r][m], d, r, m)
			}
			checkNode(t, rp, results[d][r][""], d, r, "")
		}
		checkNode(t, db, results[d][""][""], d, "", "")
	}
	checkNode(t, tree, results[""][""][""], "", "", "")
}

func checkNode(t *testing.T, measure RollupNode, results *result, d string, r string, m string) {
	mr, ok := measure.(NodeWrapper)
	if !ok {
		t.Fatalf("internal error: expected a NodeWrapper type")
	}

	switch node := mr.RollupNode.(type) {
	case *detailedNode:
		require.Equalf(t, results.series, node.Count(), "series count wrong. db: %q, rp: %q, ms: %q", d, r, m)
		require.Equalf(t, results.fields, node.fields.Count(), "field count wrong. db: %q, rp: %q, ms: %q", d, r, m)
		tagSum := uint64(0)
		for _, t := range node.tags {
			tagSum += t.Count()
		}
		require.Equalf(t, results.tags, tagSum, "tag value count wrong.  db: %q, rp: %q, ms: %q", d, r, m)
	case *simpleNode:
		require.Equalf(t, results.series, node.Count(), "series count wrong. db: %q, rp: %q, ms: %q", d, r, m)
	default:
		t.Fatalf("internal error: unknown node type")
	}
}
