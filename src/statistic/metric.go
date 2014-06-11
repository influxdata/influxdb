package statistic

type MetricType int
type MetricOperation int

// NOTE(chobie:) please also add statistic.InitializeInternalStatistic.
const (
	TYPE_SELECT                       MetricType = iota
	TYPE_OPENING_OR_CREATING_SHARD
	TYPE_WRITE_SERIES
	TYPE_QUERY
	TYPE_DELETE_SERIES
	TYPE_DROP_SERIES
	TYPE_LIST_SERIES
	TYPE_TOTAL_CONNECTION
	TYPE_CURRENT_CONNECTION
	TYPE_DELETE_SHARD
	TYPE_WAL_COMMIT_ENTRY
	TYPE_WAL_APPEND_ENTRY
	TYPE_WAL_BOOKMARK_ENTRY
	TYPE_WAL_CLOSE_ENTRY
	TYPE_WAL_CLOSE
	TYPE_WAL_OPEN
	TYPE_LEVELDB_CURRENT_READOPTIONS
	TYPE_LEVELDB_TOTAL_READOPTIONS
	TYPE_LEVELDB_CURRENT_WRITEOPTIONS
	TYPE_LEVELDB_TOTAL_WRITEOPTIONS
	TYPE_LEVELDB_COMPACT
	TYPE_LEVELDB_CURRENT_WRITEBATCH
	TYPE_LEVELDB_TOTAL_WRITEBATCH
	TYPE_HTTPAPI_BYTES_WRITTEN
	TYPE_HTTPAPI_BYTES_READ
	TYPE_HTTP_STATUS
	TYPE_HTTPAPI_RESPONSE_TIME
	TYPE_LEVELDB_POINTS_READ
	TYPE_LEVELDB_POINTS_WRITE
	TYPE_LEVELDB_POINTS_DELETE
	TYPE_LEVELDB_BYTES_WRITTEN
	TYPE_LEVELDB_WRITE_TIME

	OPERATION_INCREMENT MetricOperation = iota
	OPERATION_DECREMENT
	OPERATION_APPEND
	OPERATION_CUSTOM
)

type Metric interface {
	Increment(value interface{})
	Decrement(value interface{})
	Append(value interface{})
	GetType() MetricType
	GetOperation() MetricOperation
}

type MetricBase struct {
	Type      MetricType
	Operation MetricOperation
}

func (self MetricBase) GetType() MetricType {
	return self.Type
}

func (self MetricBase) GetOperation() MetricOperation {
	return self.Operation
}

type MetricUint64 struct {
	MetricBase
	Value    uint64
	HasValue bool
}

type MetricFloat64 struct {
	MetricBase
	Value    float64
	HasValue bool
}

type MetricCustom struct {
	MetricBase
	Yield func(stat *InternalStatistic, metric Metric)
}

func (self MetricFloat64) Increment(value interface{}) {
	if _, ok := value.(*float64); ok {
		if !self.HasValue {
			*(value.(*float64))++
		} else {
			*(value.(*float64)) += self.Value
		}
	}
}

func (self MetricFloat64) Decrement(value interface{}) {
	if _, ok := value.(*float64); ok {
		if !self.HasValue {
			*(value.(*float64))--
		} else {
			*(value.(*float64)) -= self.Value
		}
	}
}

func (self MetricFloat64) Append(value interface{}) {
	if _, ok := value.(*[]float64); ok {
		*(value.(*[]float64)) = append(*(value.(*[]float64)), self.Value)
	}
}

func (self MetricUint64) Decrement(value interface{}) {
	if _, ok := value.(*uint64); ok {
		if !self.HasValue {
			*(value.(*uint64))--
		} else {
			*(value.(*uint64)) -= self.Value
		}
	}
}

func (self MetricUint64) Increment(value interface{}) {
	if _, ok := value.(*uint64); ok {
		if !self.HasValue {
			*(value.(*uint64))++
		} else {
			*(value.(*uint64)) += self.Value
		}
	}
}

func (self MetricUint64) Append(value interface{}) {
	if _, ok := value.(*[]uint64); ok {
		*(value.(*[]uint64)) = append(*(value.(*[]uint64)), self.Value)
	}
}

func (self MetricCustom) Append(value interface{}) {
	panic("MetricCustom doesn't support append operation")
}
func (self MetricCustom) Increment(value interface{}) {
	panic("MetricCustom doesn't support increment operation")
}
func (self MetricCustom) Decrement(value interface{}) {
	panic("MetricCustom doesn't support decrement operation")
}

// it's handy.
func NewMetricUint64(record_type MetricType, operation MetricOperation, v ...uint64) Metric {
	r := MetricUint64{
		MetricBase: MetricBase{
			Type:      record_type,
			Operation: operation,
		},
	}
	if len(v) > 0 {
		r.HasValue = true
		r.Value = v[0]
	}

	return r
}

func NewMetricFloat64(record_type MetricType, operation MetricOperation, v ...float64) Metric {
	r := MetricFloat64{
		MetricBase: MetricBase{
			Type:      record_type,
			Operation: operation,
		},
	}
	if len(v) > 0 {
		r.HasValue = true
		r.Value = v[0]
	}

	return r
}

func NewMetricCustom(record_type MetricType, yield func(*InternalStatistic, Metric)) Metric {
	r := MetricCustom{
		MetricBase: MetricBase{
			Type:      record_type,
			Operation: OPERATION_CUSTOM,
		},
		Yield: yield,
	}

	return r
}
