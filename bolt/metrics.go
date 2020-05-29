package bolt

import (
	"encoding/json"
	"time"

	bolt "github.com/coreos/bbolt"
	"github.com/prometheus/client_golang/prometheus"
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
	ch <- telegrafPluginsDesc
	ch <- boltWritesDesc
	ch <- boltReadsDesc
}

type instaTicker struct {
	tick   chan struct{}
	timeCh <-chan time.Time
}

var (
	// ticker is this influx' timer for when to renew the cache of configured plugin metrics.
	ticker *instaTicker
	// telegrafPlugins is a cache of this influx' metrics of configured plugins.
	telegrafPlugins = map[string]float64{}
)

// Initialize a simple channel that will instantly "tick",
// backed by a time.Ticker's channel.
func init() {
	ticker = &instaTicker{
		tick:   make(chan struct{}, 1),
		timeCh: time.NewTicker(time.Minute * 59).C,
	}

	ticker.tick <- struct{}{}

	go func() {
		for range ticker.timeCh {
			ticker.tick <- struct{}{}
		}
	}()
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
	_ = c.db.View(func(tx *bolt.Tx) error {
		buckets = tx.Bucket(bucketBucket).Stats().KeyN
		dashboards = tx.Bucket(dashboardBucket).Stats().KeyN
		orgs = tx.Bucket(organizationBucket).Stats().KeyN
		scrapers = tx.Bucket(scraperBucket).Stats().KeyN
		telegrafs = tx.Bucket(telegrafBucket).Stats().KeyN
		tokens = tx.Bucket(authorizationBucket).Stats().KeyN
		users = tx.Bucket(userBucket).Stats().KeyN

		// Only process and store telegraf configs once per hour.
		select {
		case <-ticker.tick:
			// Clear plugins from last check.
			telegrafPlugins = map[string]float64{}
			rawPlugins := [][]byte{}

			// Loop through all reported number of plugins in the least intrusive way
			// (vs a global map and locking every time a config is updated).
			tx.Bucket(telegrafPluginsBucket).ForEach(func(k, v []byte) error {
				rawPlugins = append(rawPlugins, v)
				return nil
			})

			for _, v := range rawPlugins {
				pStats := map[string]float64{}
				err := json.Unmarshal(v, &pStats)
				if err != nil {
					return err
				}

				for k, v := range pStats {
					if _, ok := telegrafPlugins[k]; ok {
						telegrafPlugins[k] += v
					} else {
						telegrafPlugins[k] = v
					}
				}
			}

			return nil
		default:
			return nil
		}
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

	for k, v := range telegrafPlugins {
		ch <- prometheus.MustNewConstMetric(
			telegrafPluginsDesc,
			prometheus.GaugeValue,
			v,
			k, // Adds a label for plugin type.name.
		)
	}
}
