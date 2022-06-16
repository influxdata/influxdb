package bolt

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	bolt "go.etcd.io/bbolt"
)

var _ prometheus.Collector = (*Client)(nil)

// available buckets
// TODO: nuke this whole thing?
var (
	authorizationBucket   = []byte("authorizationsv1")
	bucketBucket          = []byte("bucketsv1")
	dashboardBucket       = []byte("dashboardsv2")
	organizationBucket    = []byte("organizationsv1")
	scraperBucket         = []byte("scraperv2")
	telegrafBucket        = []byte("telegrafv1")
	telegrafPluginsBucket = []byte("telegrafPluginsv1")
	remoteBucket          = []byte("remotesv2")
	replicationBucket     = []byte("replicationsv2")
	userBucket            = []byte("usersv1")
)

var (
	orgsDesc = prometheus.NewDesc(
		"influxdb_organizations_total",
		"Number of total organizations on the server",
		nil, nil)

	bucketsDesc = prometheus.NewDesc(
		"influxdb_buckets_total",
		"Number of total buckets on the server",
		nil, nil)

	usersDesc = prometheus.NewDesc(
		"influxdb_users_total",
		"Number of total users on the server",
		nil, nil)

	tokensDesc = prometheus.NewDesc(
		"influxdb_tokens_total",
		"Number of total tokens on the server",
		nil, nil)

	dashboardsDesc = prometheus.NewDesc(
		"influxdb_dashboards_total",
		"Number of total dashboards on the server",
		nil, nil)

	scrapersDesc = prometheus.NewDesc(
		"influxdb_scrapers_total",
		"Number of total scrapers on the server",
		nil, nil)

	telegrafsDesc = prometheus.NewDesc(
		"influxdb_telegrafs_total",
		"Number of total telegraf configurations on the server",
		nil, nil)

	telegrafPluginsDesc = prometheus.NewDesc(
		"influxdb_telegraf_plugins_count",
		"Number of individual telegraf plugins configured",
		[]string{"plugin"}, nil)

	remoteDesc = prometheus.NewDesc(
		"influxdb_remotes_total",
		"Number of total remote connections configured on the server",
		nil, nil)

	replicationDesc = prometheus.NewDesc(
		"influxdb_replications_total",
		"Number of total replication configurations on the server",
		nil, nil)

	boltWritesDesc = prometheus.NewDesc(
		"boltdb_writes_total",
		"Total number of boltdb writes",
		nil, nil)

	boltReadsDesc = prometheus.NewDesc(
		"boltdb_reads_total",
		"Total number of boltdb reads",
		nil, nil)
)

// Describe returns all descriptions of the collector.
func (c *Client) Describe(ch chan<- *prometheus.Desc) {
	ch <- orgsDesc
	ch <- bucketsDesc
	ch <- usersDesc
	ch <- tokensDesc
	ch <- dashboardsDesc
	ch <- scrapersDesc
	ch <- telegrafsDesc
	ch <- remoteDesc
	ch <- replicationDesc
	ch <- boltWritesDesc
	ch <- boltReadsDesc

	c.pluginsCollector.Describe(ch)
}

type pluginMetricsCollector struct {
	ticker     *time.Ticker
	tickerDone chan struct{}

	// cacheMu protects cache
	cacheMu sync.RWMutex
	cache   map[string]float64
}

func (c *pluginMetricsCollector) Open(db *bolt.DB) {
	go c.pollTelegrafStats(db)
}

func (c *pluginMetricsCollector) pollTelegrafStats(db *bolt.DB) {
	for {
		select {
		case <-c.tickerDone:
			return
		case <-c.ticker.C:
			c.refreshTelegrafStats(db)
		}
	}
}

func (c *pluginMetricsCollector) refreshTelegrafStats(db *bolt.DB) {
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	// Check if stats-polling got canceled between the point of receiving
	// a tick and grabbing the lock.
	select {
	case <-c.tickerDone:
		return
	default:
	}

	// Clear plugins from last check.
	c.cache = map[string]float64{}

	// Loop through all registered plugins.
	_ = db.View(func(tx *bolt.Tx) error {
		rawPlugins := [][]byte{}
		if err := tx.Bucket(telegrafPluginsBucket).ForEach(func(k, v []byte) error {
			rawPlugins = append(rawPlugins, v)
			return nil
		}); err != nil {
			return err
		}

		for _, v := range rawPlugins {
			pStats := map[string]float64{}
			if err := json.Unmarshal(v, &pStats); err != nil {
				return err
			}

			for k, v := range pStats {
				c.cache[k] += v
			}
		}

		return nil
	})
}

func (c *pluginMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- telegrafPluginsDesc
}

func (c *pluginMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()

	for k, v := range c.cache {
		ch <- prometheus.MustNewConstMetric(
			telegrafPluginsDesc,
			prometheus.GaugeValue,
			v,
			k, // Adds a label for plugin type.name.
		)
	}
}

func (c *pluginMetricsCollector) Close() {
	// Wait for any already-running cache-refresh procedures to complete.
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()

	close(c.tickerDone)
}

func NewPluginMetricsCollector(tickDuration time.Duration) *pluginMetricsCollector {
	return &pluginMetricsCollector{
		ticker:     time.NewTicker(tickDuration),
		tickerDone: make(chan struct{}),
		cache:      make(map[string]float64),
	}
}

// Collect returns the current state of all metrics of the collector.
func (c *Client) Collect(ch chan<- prometheus.Metric) {
	stats := c.db.Stats()
	writes := stats.TxStats.Write
	reads := stats.TxN

	ch <- prometheus.MustNewConstMetric(
		boltReadsDesc,
		prometheus.CounterValue,
		float64(reads),
	)

	ch <- prometheus.MustNewConstMetric(
		boltWritesDesc,
		prometheus.CounterValue,
		float64(writes),
	)

	orgs, buckets, users, tokens := 0, 0, 0, 0
	dashboards, scrapers, telegrafs := 0, 0, 0
	remotes, replications := 0, 0
	_ = c.db.View(func(tx *bolt.Tx) error {
		buckets = tx.Bucket(bucketBucket).Stats().KeyN
		dashboards = tx.Bucket(dashboardBucket).Stats().KeyN
		orgs = tx.Bucket(organizationBucket).Stats().KeyN
		scrapers = tx.Bucket(scraperBucket).Stats().KeyN
		telegrafs = tx.Bucket(telegrafBucket).Stats().KeyN
		remotes = tx.Bucket(remoteBucket).Stats().KeyN
		replications = tx.Bucket(replicationBucket).Stats().KeyN
		tokens = tx.Bucket(authorizationBucket).Stats().KeyN
		users = tx.Bucket(userBucket).Stats().KeyN
		return nil
	})

	ch <- prometheus.MustNewConstMetric(
		orgsDesc,
		prometheus.CounterValue,
		float64(orgs),
	)

	ch <- prometheus.MustNewConstMetric(
		bucketsDesc,
		prometheus.CounterValue,
		float64(buckets),
	)

	ch <- prometheus.MustNewConstMetric(
		usersDesc,
		prometheus.CounterValue,
		float64(users),
	)

	ch <- prometheus.MustNewConstMetric(
		tokensDesc,
		prometheus.CounterValue,
		float64(tokens),
	)

	ch <- prometheus.MustNewConstMetric(
		dashboardsDesc,
		prometheus.CounterValue,
		float64(dashboards),
	)

	ch <- prometheus.MustNewConstMetric(
		scrapersDesc,
		prometheus.CounterValue,
		float64(scrapers),
	)

	ch <- prometheus.MustNewConstMetric(
		telegrafsDesc,
		prometheus.CounterValue,
		float64(telegrafs),
	)

	ch <- prometheus.MustNewConstMetric(
		remoteDesc,
		prometheus.CounterValue,
		float64(remotes),
	)

	ch <- prometheus.MustNewConstMetric(
		replicationDesc,
		prometheus.CounterValue,
		float64(replications),
	)

	c.pluginsCollector.Collect(ch)
}
