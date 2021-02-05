package tsdb

import (
	"sync"
	"testing"

	assert2 "github.com/stretchr/testify/assert"
)

func TestIngressMetrics(t *testing.T) {
	assert := assert2.New(t)
	ingress := &IngressMetrics{}
	wg := sync.WaitGroup{}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			ingress.AddMetric("cpu", "telegraf", "autogen", "user1", 1, 10, 0)
			wg.Done()
		}()
		wg.Add(1)
		go func() {
			ingress.AddMetric("mem", "telegraf", "autogen", "user1", 2, 20, 1)
			wg.Done()
		}()
	}
	wg.Wait()
	metric := 0
	ingress.ForEach(func(m MetricKey, points, values, series int64) {
		if metric == 0 {
			assert.Equal(MetricKey{
				measurement: "cpu",
				db:          "telegraf",
				rp:          "autogen",
				login:       "user1",
			}, m)
			assert.Equal(int64(10), points)
			assert.Equal(int64(100), values)
		} else if metric == 1 {
			assert.Equal(MetricKey{
				measurement: "mem",
				db:          "telegraf",
				rp:          "autogen",
				login:       "user1",
			}, m)
			assert.Equal(int64(20), points)
			assert.Equal(int64(200), values)
			assert.Equal(int64(10), series)
		}
		metric++
	})
	assert.Equal(2, metric)
}
