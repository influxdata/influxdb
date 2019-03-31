package bolt

import (
	"fmt"

	bolt "github.com/coreos/bbolt"
	"github.com/influxdata/influxdb"
	"github.com/prometheus/client_golang/prometheus"
)

var _ prometheus.Collector = (*KVStore)(nil)

var (
	kvWritesDesc = prometheus.NewDesc(
		"boltdb_writes_total",
		"Total number of boltdb writes",
		nil, nil)

	kvReadsDesc = prometheus.NewDesc(
		"boltdb_reads_total",
		"Total number of boltdb reads",
		nil, nil)
)

var resourceBuckets = map[influxdb.ResourceType][]byte{
	influxdb.AuthorizationsResourceType: []byte("authorizationsv1"),
	influxdb.BucketsResourceType:        []byte("bucketsv1"),
	influxdb.DashboardsResourceType:     []byte("dashboardsv2"),
	influxdb.OrgsResourceType:           []byte("organizationsv1"),
	influxdb.TelegrafsResourceType:      []byte("telegrafv1"),
	influxdb.ScraperResourceType:        []byte("scraperv2"),
	influxdb.UsersResourceType:          []byte("usersv1"),
	influxdb.LabelsResourceType:         []byte("labelsv1"),
	influxdb.VariablesResourceType:      []byte("variablesv1"),
}

// Describe returns all descriptions of the collector.
func (s *KVStore) Describe(ch chan<- *prometheus.Desc) {
	ch <- kvWritesDesc
	ch <- kvReadsDesc
	for resourceType := range resourceBuckets {
		resourceDesc := prometheus.NewDesc(
			fmt.Sprintf("influxdb_%s_total", resourceType),
			fmt.Sprintf("Number of total %s on the server", resourceType),
			nil, nil)
		ch <- resourceDesc
	}
}

// Collect returns the current state of all metrics of the collector.
func (s *KVStore) Collect(ch chan<- prometheus.Metric) {
	stats := s.db.Stats()
	writes := stats.TxStats.Write
	reads := stats.TxN

	ch <- prometheus.MustNewConstMetric(
		kvReadsDesc,
		prometheus.CounterValue,
		float64(reads),
	)

	ch <- prometheus.MustNewConstMetric(
		kvWritesDesc,
		prometheus.CounterValue,
		float64(writes),
	)

	_ = s.db.View(func(tx *bolt.Tx) error {
		for resourceType, bucketName := range resourceBuckets {
			resourceDesc := prometheus.NewDesc(
				fmt.Sprintf("influxdb_%s_total", resourceType),
				fmt.Sprintf("Number of total %s on the server", resourceType),
				nil, nil)

			keyNum := 0
			if b := tx.Bucket(bucketName); b != nil {
				keyNum = b.Stats().KeyN
			}

			ch <- prometheus.MustNewConstMetric(
				resourceDesc,
				prometheus.CounterValue,
				float64(keyNum),
			)
		}
		return nil
	})
}
