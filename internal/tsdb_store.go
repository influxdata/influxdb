package internal

import (
	"io"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/uber-go/zap"
)

// TSDBStoreMock is a mockable implementation of tsdb.Store.
type TSDBStoreMock struct {
	BackupShardFn             func(id uint64, since time.Time, w io.Writer) error
	CloseFn                   func() error
	CreateShardFn             func(database, policy string, shardID uint64, enabled bool) error
	CreateShardSnapshotFn     func(id uint64) (string, error)
	DatabasesFn               func() []string
	DeleteDatabaseFn          func(name string) error
	DeleteMeasurementFn       func(database, name string) error
	DeleteRetentionPolicyFn   func(database, name string) error
	DeleteSeriesFn            func(database string, sources []influxql.Source, condition influxql.Expr) error
	DeleteShardFn             func(id uint64) error
	DiskSizeFn                func() (int64, error)
	ExpandSourcesFn           func(sources influxql.Sources) (influxql.Sources, error)
	ImportShardFn             func(id uint64, r io.Reader) error
	MeasurementSeriesCountsFn func(database string) (measuments int, series int)
	MeasurementsCardinalityFn func(database string) (int64, error)
	MeasurementNamesFn        func(database string, cond influxql.Expr) ([][]byte, error)
	OpenFn                    func() error
	PathFn                    func() string
	RestoreShardFn            func(id uint64, r io.Reader) error
	SeriesCardinalityFn       func(database string) (int64, error)
	SetShardEnabledFn         func(shardID uint64, enabled bool) error
	ShardFn                   func(id uint64) *tsdb.Shard
	ShardGroupFn              func(ids []uint64) tsdb.ShardGroup
	ShardIDsFn                func() []uint64
	ShardNFn                  func() int
	ShardRelativePathFn       func(id uint64) (string, error)
	ShardsFn                  func(ids []uint64) []*tsdb.Shard
	StatisticsFn              func(tags map[string]string) []models.Statistic
	TagValuesFn               func(auth query.Authorizer, database string, cond influxql.Expr) ([]tsdb.TagValues, error)
	WithLoggerFn              func(log zap.Logger)
	WriteToShardFn            func(shardID uint64, points []models.Point) error
}

// BackupShard creates a mock of TSDBStore.BackupShard
func (s *TSDBStoreMock) BackupShard(id uint64, since time.Time, w io.Writer) error {
	return s.BackupShardFn(id, since, w)
}

// Close creates a mock of TSDBStore.Close
func (s *TSDBStoreMock) Close() error { return s.CloseFn() }

// CreateShard creates a mock of TSDBStore.CreateShard
func (s *TSDBStoreMock) CreateShard(database string, retentionPolicy string, shardID uint64, enabled bool) error {
	return s.CreateShardFn(database, retentionPolicy, shardID, enabled)
}

// CreateShardSnapshot creates a mock of TSDBStore.CreateShardSnapshot
func (s *TSDBStoreMock) CreateShardSnapshot(id uint64) (string, error) {
	return s.CreateShardSnapshotFn(id)
}

// Databases creates a mock of TSDBStore.Databases
func (s *TSDBStoreMock) Databases() []string {
	return s.DatabasesFn()
}

// DeleteDatabase creates a mock of TSDBStore.DeleteDatabase
func (s *TSDBStoreMock) DeleteDatabase(name string) error {
	return s.DeleteDatabaseFn(name)
}

// DeleteMeasurement creates a mock of TSDBStore.DeleteMeasurement
func (s *TSDBStoreMock) DeleteMeasurement(database string, name string) error {
	return s.DeleteMeasurementFn(database, name)
}

// DeleteRetentionPolicy creates a mock of TSDBStore.DeleteRetentionPolicy
func (s *TSDBStoreMock) DeleteRetentionPolicy(database string, name string) error {
	return s.DeleteRetentionPolicyFn(database, name)
}

// DeleteSeries creates a mock of TSDBStore.DeleteSeries
func (s *TSDBStoreMock) DeleteSeries(database string, sources []influxql.Source, condition influxql.Expr) error {
	return s.DeleteSeriesFn(database, sources, condition)
}

// DeleteShard creates a mock of TSDBStore.DeleteShard
func (s *TSDBStoreMock) DeleteShard(shardID uint64) error {
	return s.DeleteShardFn(shardID)
}

// DiskSize creates a mock of TSDBStore.DiskSize
func (s *TSDBStoreMock) DiskSize() (int64, error) {
	return s.DiskSizeFn()
}

// ExpandSources creates a mock of TSDBStore.ExpandSources
func (s *TSDBStoreMock) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	return s.ExpandSourcesFn(sources)
}

// ImportShard creates a mock of TSDBStore.ImportShard
func (s *TSDBStoreMock) ImportShard(id uint64, r io.Reader) error {
	return s.ImportShardFn(id, r)
}

// MeasurementNames creates a mock of TSDBStore.MeasurementNames
func (s *TSDBStoreMock) MeasurementNames(database string, cond influxql.Expr) ([][]byte, error) {
	return s.MeasurementNamesFn(database, cond)
}

// MeasurementSeriesCounts creates a mock of TSDBStore.MeasurementSeriesCounts
func (s *TSDBStoreMock) MeasurementSeriesCounts(database string) (measuments int, series int) {
	return s.MeasurementSeriesCountsFn(database)
}

// MeasurementsCardinality creates a mock of TSDBStore.MeasurementsCardinality
func (s *TSDBStoreMock) MeasurementsCardinality(database string) (int64, error) {
	return s.MeasurementsCardinalityFn(database)
}

// Open creates a mock of TSDBStore.Open
func (s *TSDBStoreMock) Open() error {
	return s.OpenFn()
}

// Path creates a mock of TSDBStore.Path
func (s *TSDBStoreMock) Path() string {
	return s.PathFn()
}

// RestoreShard creates a mock of TSDBStore.RestoreShard
func (s *TSDBStoreMock) RestoreShard(id uint64, r io.Reader) error {
	return s.RestoreShardFn(id, r)
}

// SeriesCardinality creates a mock of TSDBStore.SeriesCardinality
func (s *TSDBStoreMock) SeriesCardinality(database string) (int64, error) {
	return s.SeriesCardinalityFn(database)
}

// SetShardEnabled creates a mock of TSDBStore.SetShardEnabled
func (s *TSDBStoreMock) SetShardEnabled(shardID uint64, enabled bool) error {
	return s.SetShardEnabledFn(shardID, enabled)
}

// Shard creates a mock of TSDBStore.Shard
func (s *TSDBStoreMock) Shard(id uint64) *tsdb.Shard {
	return s.ShardFn(id)
}

// ShardGroup creates a mock of TSDBStore.ShardGroup
func (s *TSDBStoreMock) ShardGroup(ids []uint64) tsdb.ShardGroup {
	return s.ShardGroupFn(ids)
}

// ShardIDs creates a mock of TSDBStore.ShardIDs
func (s *TSDBStoreMock) ShardIDs() []uint64 {
	return s.ShardIDsFn()
}

// ShardN creates a mock of TSDBStore.ShardN
func (s *TSDBStoreMock) ShardN() int {
	return s.ShardNFn()
}

// ShardRelativePath creates a mock of TSDBStore.ShardRelativePath
func (s *TSDBStoreMock) ShardRelativePath(id uint64) (string, error) {
	return s.ShardRelativePathFn(id)
}

// Shards creates a mock of TSDBStore.Shards
func (s *TSDBStoreMock) Shards(ids []uint64) []*tsdb.Shard {
	return s.ShardsFn(ids)
}

// Statistics creates a mock of TSDBStore.Statistics
func (s *TSDBStoreMock) Statistics(tags map[string]string) []models.Statistic {
	return s.StatisticsFn(tags)
}

// TagValues creates a mock of TSDBStore.TagValues
func (s *TSDBStoreMock) TagValues(auth query.Authorizer, database string, cond influxql.Expr) ([]tsdb.TagValues, error) {
	return s.TagValuesFn(auth, database, cond)
}

// WithLogger creates a mock of TSDBStore.WithLogger
func (s *TSDBStoreMock) WithLogger(log zap.Logger) {
	s.WithLoggerFn(log)
}

// WriteToShard creates a mock of TSDBStore.WriteToShard
func (s *TSDBStoreMock) WriteToShard(shardID uint64, points []models.Point) error {
	return s.WriteToShardFn(shardID, points)
}
