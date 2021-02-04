package tsdb

import (
	"sort"
	"sync"
	"sync/atomic"
)

type MetricKey struct {
	measurement string
	db          string
	rp          string
	login       string
}

type MetricValue struct {
	points int64
	values int64
	series int64
}

type IngressMetrics struct {
	m      sync.Map
	length int64
}

func (i *IngressMetrics) AddMetric(measurement, db, rp, login string, points, values, series int64) {
	key := MetricKey{measurement, db, rp, login}
	val, ok := i.m.Load(key)
	if !ok {
		var loaded bool
		val, loaded = i.m.LoadOrStore(key, &MetricValue{})
		if !loaded {
			atomic.AddInt64(&i.length, 1)
		}
	}
	metricVal := val.(*MetricValue)
	atomic.AddInt64(&metricVal.points, points)
	atomic.AddInt64(&metricVal.values, values)
	atomic.AddInt64(&metricVal.series, series)
}

func (i *IngressMetrics) ForEach(f func(m MetricKey, points, values, series int64)) {
	keys := make([]MetricKey, 0, atomic.LoadInt64(&i.length))
	i.m.Range(func(key, value interface{}) bool {
		keys = append(keys, key.(MetricKey))
		return true
	})
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].db != keys[j].db {
			return keys[i].db < keys[j].db
		}
		if keys[i].rp != keys[j].rp {
			return keys[i].rp < keys[j].rp
		}
		if keys[i].measurement != keys[j].measurement {
			return keys[i].measurement < keys[j].measurement
		}
		return keys[i].login < keys[j].login
	})
	for _, key := range keys {
		val, ok := i.m.Load(key)
		// ok should always be true - we don't delete keys. But if we did we would ignore keys concurrently deleted.
		if ok {
			f(key, val.(*MetricValue).points, val.(*MetricValue).values, val.(*MetricValue).series)
		}
	}
}
