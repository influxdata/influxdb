package statistic

import (
	"configuration"
	"fmt"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"

	log "code.google.com/p/log4go"
)

const GOROUTINES_COLLECT_LIMIT = 10

// Statistic structs should support Protobuf.
type Statistic struct {
	Name        string               `json:"name"`
	Version     string               `json:"version"`
	Pid         int                  `json:"pid"`
	Uptime      float64              `json:"uptime"`
	Time        int64                `json:"time"`
	Api         StatisticApi         `json:"api"`
	Wal         StatisticWal         `json:"wal"`
	Shard       StatisticShard       `json:"shard"`
	Coordinator StatisticCoordinator `json:"coordinator"`
	Net         StatisticNet         `json:"net"`
	LevelDB     StatisticLevelDB     `json:"leveldb"`
	Go          StatisticGo          `json:"go"`
	Sys         StatisticSys         `json:"sys"`
	Raft        StatisticRaft        `json:"raft"`
	Protobuf    StatisticProtobuf    `json:"protobuf"`
}

type StatisticWal struct {
	Opening       uint64 `json:"opening"`
	AppendEntry   uint64 `json:"append_entry"`
	CommitEntry   uint64 `json:"commit_entry"`
	BookmarkEntry uint64 `json:"bookmark_entry"`
}

type StatisticShard struct {
	Opening uint   `json:"opening"`
	Delete  uint64 `json:"delete"`
}

type StatisticCoordinator struct {
	CmdQuery       uint64 `json:"cmd_query"`
	CmdSelect      uint64 `json:"cmd_select"`
	CmdWriteSeries uint64 `json:"cmd_write_series"`
	CmdDelete      uint64 `json:"cmd_delete"`
	CmdDrop        uint64 `json:"cmd_drop"`
	CmdListSeries  uint64 `json:"cmd_list_series"`
}

type StatisticNet struct {
	CurrentConnections uint64 `json:"current_connections"`
	Connections        uint64 `json:"connections"`
	BytesWritten       uint64 `json:"bytes_written"`
	BytesRead          uint64 `json:"bytes_read"`
}

type StatisticLevelDB struct {
	PointsRead   uint64  `json:"points_read"`
	PointsWrite  uint64  `json:"points_write"`
	PointsDelete uint64  `json:"points_delete"`
	WriteTimeMin float64 `json:"min"`
	WriteTimeAvg float64 `json:"avg"`
	WriteTimeMax float64 `json:"max"`
	BytesWritten uint64  `json:"bytes_written"`
}

type StatisticGo struct {
	CurrentGoroutines int     `json:"current_goroutines"`
	GoroutinesAvg     float64 `json:"goroutines_avg"`
	CgoCall           int64   `json:"cgo_call"`
}

type StatisticTimeVal struct {
	Sec  int64 `json:"sec"`
	Usec int64 `json:"usec"`
}

type StatisticRusage struct {
	User   StatisticTimeVal `json:"user"`
	System StatisticTimeVal `json:"sys"`
}

type StatisticSys struct {
	Rusage   StatisticRusage `json:"rusage"`
	SysBytes uint64          `json:"sys_bytes"`
	Alloc    uint64          `json:"alloc"`
}

type StatisticRaft struct {
	//TODO
}

type StatisticProtobuf struct {
	//TODO
}

type StatisticApi struct {
	Http StatisticApiHTTP `json:"http"`
}

type StatisticApiHTTPResponseStatus struct {
	Code  int    `json:"code"`
	Count uint64 `json:"count"`
}
type StatisticApiHTTPResponse struct {
	Min    float64                          `json:"min"`
	Avg    float64                          `json:"avg"`
	Max    float64                          `json:"max"`
	Status []StatisticApiHTTPResponseStatus `json:"status"`
}

type StatisticApiHTTP struct {
	BytesWritten       uint64                   `json:"bytes_written"`
	BytesRead          uint64                   `json:"bytes_read"`
	CurrentConnections uint64                   `json:"current_connections"`
	Connections        uint64                   `json:"connections"`
	Response           StatisticApiHTTPResponse `json:"response"`
}

type InternalStatistic struct {
	Pid     int
	Name    string
	Version string

	WalCommitEntry   uint64
	WalAppendEntry   uint64
	WalBookmarkEntry uint64
	WalCloseEntry    uint64
	WalClose         uint64
	WalOpen          uint64

	LeveldbCompact             uint64
	LeveldbCurrentReadOptions  uint64
	LeveldbTotalReadOptions    uint64
	LeveldbCurrentWriteOptions uint64
	LeveldbTotalWriteOptions   uint64
	LeveldbCurrentWriteBatch   uint64
	LeveldbTotalWriteBatch     uint64
	LeveldbPointsRead          uint64
	LeveldbPointsWrite         uint64
	LeveldbPointsDelete        uint64
	LeveldbBytesWritten        uint64
	LeveldbWriteTime           []float64
	LeveldbWriteTimeMean       float64
	LeveldbWriteTimeMin        float64
	LeveldbWriteTimeMax        float64

	HTTPApiBytesWritten     uint64
	HTTPApiBytesRead        uint64
	HTTPApiResponseTime     []float64
	HTTPApiResponseTimeMean float64
	HTTPApiResponseTimeMin  float64
	HTTPApiResponseTimeMax  float64

	HttpStatus map[uint64]uint64

	NumOpeningOrCreatingShard int
	DeleteShard               uint64
	Receiver                  chan Metric

	CurrentConnections uint64
	TotalConnections   uint64

	NumQuery       uint64
	NumWriteSeries uint64
	NumSelect      uint64
	NumDelete      uint64
	NumDrop        uint64
	NumListSeries  uint64

	NumCgoCall      int64
	NumGoroutine    int
	NumGoroutineAvg float64

	Time      time.Time
	Uptime    int64
	StartedAt time.Time

	MemStats *runtime.MemStats

	RusageBegin   syscall.Rusage
	RusageCurrent syscall.Rusage

	NumGoroutines []int

	Mutex sync.Mutex

	Collect chan bool
	Run chan bool
}

var stat *InternalStatistic
var typemap map[MetricType]interface{}

func (stat *InternalStatistic) Atomic(yield func()) {
	stat.Mutex.Lock()
	defer stat.Mutex.Unlock()
	yield()
}

// collect metric
func Prove(records ...Metric) {
	defer func() {
		if err := recover(); err != nil {
			log.Debug("BUG:", err)
		}
	}()

	stat.update(records)
}

// receives channels and update statistic.
func runReceiver() {
	var v interface{}
	var ok bool

	for {
		select {
		case req := <-stat.Receiver:
			if v, ok = typemap[req.GetType()]; ok {
				switch req.GetOperation() {
				case OPERATION_INCREMENT:
					req.Increment(v)
				case OPERATION_DECREMENT:
					req.Decrement(v)
				case OPERATION_APPEND:
					stat.Atomic(func() {
						req.Append(v)
					})
				case OPERATION_CUSTOM:
					if custom, ok := req.(MetricCustom); ok {
						custom.Yield(stat, custom)
					}
				}
			} else {
				panic(fmt.Sprintf("BUG: typemap has not key for %d", req.GetType()))
			}
		case _, ok := <- stat.Collect:
			if !ok {
				return
			}
		}
	}
}

func min(values *[]float64) float64 {
	var min float64 = 0.0
	flag := false
	if len(*values) == 0 {
		return min
	}
	for _, value := range *values {
		if !flag {
			min = value
			flag = true
		} else {
			if value < min {
				min = value
			}
		}
	}
	return min
}

func max(values *[]float64) float64 {
	var max float64 = 0.0
	flag := false
	if len(*values) == 0 {
		return max
	}
	for _, value := range *values {
		if !flag {
			max = value
			flag = true
		} else {
			if value > max {
				max = value
			}
		}
	}
	return max
}

func mean(values *[]float64) float64 {
	var sum float64 = 0.0
	if len(*values) == 0 {
		return sum
	}
	for _, value := range *values {
		sum += value
	}

	return float64(sum / float64(len(*values)))
}

func meani(values *[]int) float64 {
	var sum int = 0
	if len(*values) == 0 {
		return float64(sum)
	}
	for _, value := range *values {
		sum += value
	}

	return float64(sum / len(*values))
}

func collectStat() {
	for _ = range time.Tick(1 * time.Second) {
		stat.NumGoroutine = runtime.NumGoroutine()
		if len(stat.NumGoroutines) >= GOROUTINES_COLLECT_LIMIT {
			stat.NumGoroutines = stat.NumGoroutines[1:]
		}
		stat.NumGoroutines = append(stat.NumGoroutines, stat.NumGoroutine)
		stat.NumGoroutineAvg = meani(&stat.NumGoroutines)

		stat.NumCgoCall = runtime.NumCgoCall()

		stat.Atomic(func() {
			stat.HTTPApiResponseTimeMean = mean(&stat.HTTPApiResponseTime)
			stat.HTTPApiResponseTimeMin = min(&stat.HTTPApiResponseTime)
			stat.HTTPApiResponseTimeMax = max(&stat.HTTPApiResponseTime)

			stat.LeveldbWriteTimeMean = mean(&stat.LeveldbWriteTime)
			stat.LeveldbWriteTimeMin = min(&stat.LeveldbWriteTime)
			stat.LeveldbWriteTimeMax = max(&stat.LeveldbWriteTime)

			stat.HTTPApiResponseTime = stat.HTTPApiResponseTime[:0]
			stat.LeveldbWriteTime = stat.LeveldbWriteTime[:0]
		})

		runtime.ReadMemStats(stat.MemStats)
		syscall.Getrusage(syscall.RUSAGE_SELF, &stat.RusageCurrent)
		stat.Time = time.Now()
		stat.Uptime = stat.Time.Unix() - stat.StartedAt.Unix()

		select {
			case _, ok := <- stat.Collect:
				if !ok {
					return
				}
			default:
		}
	}
}

func InitializeInternalStatistic(pid int, config *configuration.Configuration) {
	stat = &InternalStatistic{
		Pid:           pid,
		Version:       config.Version,
		Receiver:      make(chan Metric, 128),
		StartedAt:     time.Now(),
		MemStats:      &runtime.MemStats{},
		RusageBegin:   syscall.Rusage{},
		RusageCurrent: syscall.Rusage{},
		HttpStatus:    map[uint64]uint64{},
		Collect: make(chan bool, 1),
		Run: make(chan bool, 1),
	}
	if config.Hostname == "" {
		stat.Name, _ = os.Hostname()
	} else {
		stat.Name = config.Hostname
	}

	err := syscall.Getrusage(syscall.RUSAGE_SELF, &stat.RusageBegin)
	if err != nil {
		log.Debug("Getrusage error: %s", err)
	}

	typemap = make(map[MetricType]interface{})
	typemap[TYPE_SELECT] = &stat.NumSelect
	typemap[TYPE_QUERY] = &stat.NumQuery
	typemap[TYPE_WRITE_SERIES] = &stat.NumWriteSeries
	typemap[TYPE_DELETE_SERIES] = &stat.NumDelete
	typemap[TYPE_DROP_SERIES] = &stat.NumDrop
	typemap[TYPE_LIST_SERIES] = &stat.NumListSeries
	typemap[TYPE_OPENING_OR_CREATING_SHARD] = &stat.NumOpeningOrCreatingShard
	typemap[TYPE_CURRENT_CONNECTION] = &stat.CurrentConnections
	typemap[TYPE_TOTAL_CONNECTION] = &stat.TotalConnections
	typemap[TYPE_DELETE_SHARD] = &stat.DeleteShard
	typemap[TYPE_WAL_COMMIT_ENTRY] = &stat.WalCommitEntry
	typemap[TYPE_WAL_APPEND_ENTRY] = &stat.WalAppendEntry
	typemap[TYPE_WAL_BOOKMARK_ENTRY] = &stat.WalBookmarkEntry
	typemap[TYPE_WAL_CLOSE_ENTRY] = &stat.WalCloseEntry
	typemap[TYPE_WAL_CLOSE] = &stat.WalClose
	typemap[TYPE_WAL_OPEN] = &stat.WalOpen
	typemap[TYPE_LEVELDB_COMPACT] = &stat.LeveldbCompact
	typemap[TYPE_LEVELDB_CURRENT_READOPTIONS] = &stat.LeveldbCurrentReadOptions
	typemap[TYPE_LEVELDB_TOTAL_READOPTIONS] = &stat.LeveldbTotalReadOptions
	typemap[TYPE_LEVELDB_CURRENT_WRITEOPTIONS] = &stat.LeveldbCurrentWriteOptions
	typemap[TYPE_LEVELDB_TOTAL_WRITEOPTIONS] = &stat.LeveldbTotalWriteOptions
	typemap[TYPE_LEVELDB_CURRENT_WRITEBATCH] = &stat.LeveldbCurrentWriteBatch
	typemap[TYPE_LEVELDB_TOTAL_WRITEBATCH] = &stat.LeveldbTotalWriteBatch
	typemap[TYPE_HTTPAPI_BYTES_WRITTEN] = &stat.HTTPApiBytesWritten
	typemap[TYPE_HTTPAPI_BYTES_READ] = &stat.HTTPApiBytesRead
	typemap[TYPE_HTTP_STATUS] = &stat.HttpStatus
	typemap[TYPE_HTTPAPI_RESPONSE_TIME] = &stat.HTTPApiResponseTime
	typemap[TYPE_LEVELDB_POINTS_READ] = &stat.LeveldbPointsRead
	typemap[TYPE_LEVELDB_POINTS_WRITE] = &stat.LeveldbPointsWrite
	typemap[TYPE_LEVELDB_POINTS_DELETE] = &stat.LeveldbPointsDelete
	typemap[TYPE_LEVELDB_BYTES_WRITTEN] = &stat.LeveldbBytesWritten
	typemap[TYPE_LEVELDB_WRITE_TIME] = &stat.LeveldbWriteTime
}

func BeginCorrect() {
	go collectStat()
	go runReceiver()
}

func StopCorrect() {
	close(stat.Run)
	close(stat.Collect)
}


func (self *InternalStatistic) update(record []Metric) {
	for _, r := range record {
		self.Receiver <- r
	}
}

func GetStatistic(s *Statistic) {
	now := time.Now()

	if stat == nil {
		return
	}

	stat.Atomic(func() {
		s.Name = stat.Name
		s.Version = stat.Version
		s.Pid = stat.Pid
		s.Uptime = now.Sub(stat.StartedAt).Seconds()
		s.Time = now.Unix()
		s.Api.Http.BytesWritten = stat.HTTPApiBytesWritten
		s.Api.Http.BytesRead = stat.HTTPApiBytesRead
		s.Api.Http.CurrentConnections = stat.CurrentConnections
		s.Api.Http.Connections = stat.TotalConnections
		s.Api.Http.Response.Status = s.Api.Http.Response.Status[:0]

		for status, count := range stat.HttpStatus {
			s.Api.Http.Response.Status = append(s.Api.Http.Response.Status, StatisticApiHTTPResponseStatus{
				int(status),
				count,
			})
		}

		s.Api.Http.Response.Min = stat.HTTPApiResponseTimeMin
		s.Api.Http.Response.Max = stat.HTTPApiResponseTimeMax
		s.Api.Http.Response.Avg = stat.HTTPApiResponseTimeMean

		s.Wal.Opening = stat.WalOpen
		s.Wal.AppendEntry = stat.WalAppendEntry
		s.Wal.CommitEntry = stat.WalCommitEntry
		s.Wal.BookmarkEntry = stat.WalBookmarkEntry

		s.Shard.Opening = uint(stat.NumOpeningOrCreatingShard)
		s.Shard.Delete = stat.DeleteShard
		s.Coordinator.CmdQuery = stat.NumQuery
		s.Coordinator.CmdSelect = stat.NumSelect
		s.Coordinator.CmdWriteSeries = stat.NumWriteSeries
		s.Coordinator.CmdDelete = stat.NumDelete
		s.Coordinator.CmdDrop = stat.NumDrop
		s.Coordinator.CmdListSeries = stat.NumListSeries

		s.Net.CurrentConnections = stat.CurrentConnections
		s.Net.Connections = stat.TotalConnections
		s.Net.BytesWritten = stat.HTTPApiBytesWritten
		s.Net.BytesRead = stat.HTTPApiBytesRead

		s.LevelDB.PointsRead = stat.LeveldbPointsRead
		s.LevelDB.PointsWrite = stat.LeveldbPointsWrite
		s.LevelDB.PointsDelete = stat.LeveldbPointsDelete
		s.LevelDB.BytesWritten = stat.LeveldbBytesWritten
		s.LevelDB.WriteTimeAvg = stat.LeveldbWriteTimeMean
		s.LevelDB.WriteTimeMin = stat.LeveldbWriteTimeMin
		s.LevelDB.WriteTimeMax = stat.LeveldbWriteTimeMax

		s.Go.CurrentGoroutines = stat.NumGoroutine
		s.Go.GoroutinesAvg = stat.NumGoroutineAvg
		s.Go.CgoCall = stat.NumCgoCall

		s.Sys.Rusage.User.Sec = stat.RusageCurrent.Utime.Sec
		s.Sys.Rusage.User.Usec = int64(stat.RusageCurrent.Utime.Usec)
		s.Sys.Rusage.System.Sec = stat.RusageCurrent.Stime.Sec
		s.Sys.Rusage.System.Usec = int64(stat.RusageCurrent.Stime.Usec)
		s.Sys.SysBytes = stat.MemStats.Sys
		s.Sys.Alloc = stat.MemStats.Alloc
	})

	return
}
