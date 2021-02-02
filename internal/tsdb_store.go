package internal

import (
	"io"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

// TSDBStoreMock is a mockable implementation of tsdb.Store.
type TSDBStoreMock struct {
	BackupShardFn             func(id uint64, since time.Time, w io.Writer) error
	BackupSeriesFileFn        func(database string, w io.Writer) error
	ExportShardFn             func(id uint64, ExportStart time.Time, ExportEnd time.Time, w io.Writer) error
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
	MeasurementNamesFn        func(auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error)
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
	TagKeysFn                 func(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error)
	TagValuesFn               func(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error)
	WithLoggerFn              func(log *zap.Logger)
	WriteToShardFn            func(shardID uint64, points []models.Point) error
}

func (s *TSDBStoreMock) BackupShard(id uint64, since time.Time, w io.Writer) error {
	return s.BackupShardFn(id, since, w)
}
func (s *TSDBStoreMock) BackupSeriesFile(database string, w io.Writer) error {
	return s.BackupSeriesFileFn(database, w)
}
func (s *TSDBStoreMock) ExportShard(id uint64, ExportStart time.Time, ExportEnd time.Time, w io.Writer) error {
	return s.ExportShardFn(id, ExportStart, ExportEnd, w)
}
func (s *TSDBStoreMock) Close() error { return s.CloseFn() }
func (s *TSDBStoreMock) CreateShard(database string, retentionPolicy string, shardID uint64, enabled bool) error {
	return s.CreateShardFn(database, retentionPolicy, shardID, enabled)
}
func (s *TSDBStoreMock) CreateShardSnapshot(id uint64) (string, error) {
	return s.CreateShardSnapshotFn(id)
}
func (s *TSDBStoreMock) Databases() []string {
	return s.DatabasesFn()
}
func (s *TSDBStoreMock) DeleteDatabase(name string) error {
	return s.DeleteDatabaseFn(name)
}
func (s *TSDBStoreMock) DeleteMeasurement(database string, name string) error {
	return s.DeleteMeasurementFn(database, name)
}
func (s *TSDBStoreMock) DeleteRetentionPolicy(database string, name string) error {
	return s.DeleteRetentionPolicyFn(database, name)
}
func (s *TSDBStoreMock) DeleteSeries(database string, sources []influxql.Source, condition influxql.Expr) error {
	return s.DeleteSeriesFn(database, sources, condition)
}
func (s *TSDBStoreMock) DeleteShard(shardID uint64) error {
	return s.DeleteShardFn(shardID)
}
func (s *TSDBStoreMock) DiskSize() (int64, error) {
	return s.DiskSizeFn()
}
func (s *TSDBStoreMock) ExpandSources(sources influxql.Sources) (influxql.Sources, error) {
	return s.ExpandSourcesFn(sources)
}
func (s *TSDBStoreMock) ImportShard(id uint64, r io.Reader) error {
	return s.ImportShardFn(id, r)
}
func (s *TSDBStoreMock) MeasurementNames(auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error) {
	return s.MeasurementNamesFn(auth, database, cond)
}
func (s *TSDBStoreMock) MeasurementSeriesCounts(database string) (measuments int, series int) {
	return s.MeasurementSeriesCountsFn(database)
}
func (s *TSDBStoreMock) MeasurementsCardinality(database string) (int64, error) {
	return s.MeasurementsCardinalityFn(database)
}
func (s *TSDBStoreMock) Open() error {
	return s.OpenFn()
}
func (s *TSDBStoreMock) Path() string {
	return s.PathFn()
}
func (s *TSDBStoreMock) RestoreShard(id uint64, r io.Reader) error {
	return s.RestoreShardFn(id, r)
}
func (s *TSDBStoreMock) SeriesCardinality(database string) (int64, error) {
	return s.SeriesCardinalityFn(database)
}
func (s *TSDBStoreMock) SetShardEnabled(shardID uint64, enabled bool) error {
	return s.SetShardEnabledFn(shardID, enabled)
}
func (s *TSDBStoreMock) Shard(id uint64) *tsdb.Shard {
	return s.ShardFn(id)
}
func (s *TSDBStoreMock) ShardGroup(ids []uint64) tsdb.ShardGroup {
	return s.ShardGroupFn(ids)
}
func (s *TSDBStoreMock) ShardIDs() []uint64 {
	return s.ShardIDsFn()
}
func (s *TSDBStoreMock) ShardN() int {
	return s.ShardNFn()
}
func (s *TSDBStoreMock) ShardRelativePath(id uint64) (string, error) {
	return s.ShardRelativePathFn(id)
}
func (s *TSDBStoreMock) Shards(ids []uint64) []*tsdb.Shard {
	return s.ShardsFn(ids)
}
func (s *TSDBStoreMock) Statistics(tags map[string]string) []models.Statistic {
	return s.StatisticsFn(tags)
}
func (s *TSDBStoreMock) TagKeys(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error) {
	return s.TagKeysFn(auth, shardIDs, cond)
}
func (s *TSDBStoreMock) TagValues(auth query.Authorizer, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error) {
	return s.TagValuesFn(auth, shardIDs, cond)
}
func (s *TSDBStoreMock) WithLogger(log *zap.Logger) {
	s.WithLoggerFn(log)
}
func (s *TSDBStoreMock) WriteToShard(shardID uint64, points []models.Point) error {
	return s.WriteToShardFn(shardID, points)
}
