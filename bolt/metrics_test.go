package bolt_test

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/bolt"
	"github.com/influxdata/influxdb/v2/kit/prom"
	"github.com/influxdata/influxdb/v2/kit/prom/promtest"
	"github.com/influxdata/influxdb/v2/kv/migration/all"
	telegrafservice "github.com/influxdata/influxdb/v2/telegraf/service"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestInitialMetrics(t *testing.T) {
	t.Parallel()

	client, teardown, err := NewTestClient(t)
	if err != nil {
		t.Fatalf("unable to setup bolt client: %v", err)
	}
	defer teardown()

	reg := prom.NewRegistry(zaptest.NewLogger(t))
	reg.MustRegister(client)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}

	metrics := map[string]int{
		"influxdb_organizations_total": 0,
		"influxdb_buckets_total":       0,
		"influxdb_users_total":         0,
		"influxdb_tokens_total":        0,
		"influxdb_dashboards_total":    0,
		"boltdb_reads_total":           0,
	}
	for name, count := range metrics {
		c := promtest.MustFindMetric(t, mfs, name, nil)
		if got := c.GetCounter().GetValue(); int(got) != count {
			t.Errorf("expected %s counter to be %d, got %v", name, count, got)
		}
	}
}

func TestPluginMetrics(t *testing.T) {
	t.Parallel()

	// Set up a BoltDB, and register a telegraf config.
	client, teardown, err := NewTestClient(t)
	require.NoError(t, err)
	defer teardown()

	ctx := context.Background()
	log := zaptest.NewLogger(t)
	kvStore := bolt.NewKVStore(log, client.Path)
	kvStore.WithDB(client.DB())
	require.NoError(t, all.Up(ctx, log, kvStore))

	tsvc := telegrafservice.New(kvStore)
	tconf := influxdb.TelegrafConfig{
		Name:   "test",
		Config: "[[inputs.cpu]]\n[[outputs.influxdb_v2]]",
		OrgID:  1,
	}
	require.NoError(t, tsvc.CreateTelegrafConfig(ctx, &tconf, 1))

	// Run a plugin metrics collector with a quicker tick interval than the default.
	pluginCollector := bolt.NewPluginMetricsCollector(time.Millisecond)
	pluginCollector.Open(client.DB())
	defer pluginCollector.Close()

	reg := prom.NewRegistry(zaptest.NewLogger(t))
	reg.MustRegister(pluginCollector)

	// Run a periodic gather in the background.
	gatherTick := time.NewTicker(time.Millisecond)
	doneCh := make(chan struct{})
	defer close(doneCh)

	go func() {
		for {
			select {
			case <-doneCh:
				return
			case <-gatherTick.C:
				_, err := reg.Gather()
				require.NoError(t, err)
			}
		}
	}()

	// Run a few gathers to see if any race conditions are flushed out.
	time.Sleep(250 * time.Millisecond)

	// Gather plugin metrics and ensure they're correct.
	metrics, err := reg.Gather()
	require.NoError(t, err)
	inCpu := promtest.MustFindMetric(t, metrics, "influxdb_telegraf_plugins_count", map[string]string{"plugin": "inputs.cpu"})
	outInfluxDb := promtest.MustFindMetric(t, metrics, "influxdb_telegraf_plugins_count", map[string]string{"plugin": "outputs.influxdb_v2"})
	require.Equal(t, 1, int(inCpu.GetGauge().GetValue()))
	require.Equal(t, 1, int(outInfluxDb.GetGauge().GetValue()))

	// Register some more plugins.
	tconf = influxdb.TelegrafConfig{
		Name:   "test",
		Config: "[[inputs.mem]]\n[[outputs.influxdb_v2]]",
		OrgID:  1,
	}
	require.NoError(t, tsvc.CreateTelegrafConfig(ctx, &tconf, 2))

	// Let a few more background gathers run.
	time.Sleep(250 * time.Millisecond)

	// Gather again, and ensure plugin metrics have been updated.
	metrics, err = reg.Gather()
	require.NoError(t, err)
	inCpu = promtest.MustFindMetric(t, metrics, "influxdb_telegraf_plugins_count", map[string]string{"plugin": "inputs.cpu"})
	inMem := promtest.MustFindMetric(t, metrics, "influxdb_telegraf_plugins_count", map[string]string{"plugin": "inputs.mem"})
	outInfluxDb = promtest.MustFindMetric(t, metrics, "influxdb_telegraf_plugins_count", map[string]string{"plugin": "outputs.influxdb_v2"})
	require.Equal(t, 1, int(inCpu.GetGauge().GetValue()))
	require.Equal(t, 1, int(inMem.GetGauge().GetValue()))
	require.Equal(t, 2, int(outInfluxDb.GetGauge().GetValue()))
}
