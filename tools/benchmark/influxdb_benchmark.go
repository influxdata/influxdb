package main

import (
	crand "crypto/rand"
	"crypto/tls"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/influxdb/influxdb/client"
)

type benchmarkConfig struct {
	OutputAfterCount   int                `toml:"output_after_count"`
	LogFile            string             `toml:"log_file"`
	StatsServer        statsServer        `toml:"stats_server"`
	Servers            []server           `toml:"servers"`
	ClusterCredentials clusterCredentials `toml:"cluster_credentials"`
	LoadSettings       loadSettings       `toml:"load_settings"`
	LoadDefinitions    []loadDefinition   `toml:"load_definitions"`
	Log                *os.File
}

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) (err error) {
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

type statsServer struct {
	ConnectionString string   `toml:"connection_string"`
	User             string   `toml:"user"`
	Password         string   `toml:"password"`
	Database         string   `toml:"database"`
	IsSecure         bool     `toml:"is_secure"`
	SkipVerify       bool     `toml:"skip_verify"`
	Timeout          duration `toml:"timeout"`
}

type clusterCredentials struct {
	Database   string   `toml:"database"`
	User       string   `toml:"user"`
	Password   string   `toml:"password"`
	IsSecure   bool     `toml:"is_secure"`
	SkipVerify bool     `toml:"skip_verify"`
	Timeout    duration `toml:"timeout"`
}

type server struct {
	ConnectionString string `toml:"connection_string"`
}

type loadSettings struct {
	ConcurrentConnections int `toml:"concurrent_connections"`
	RunPerLoadDefinition  int `toml:"runs_per_load_definition"`
}

type loadDefinition struct {
	Name                   string         `toml:"name"`
	ReportSamplingInterval int            `toml:"report_sampling_interval"`
	Percentiles            []float64      `toml:"percentiles"`
	PercentileTimeInterval string         `toml:"percentile_time_interval"`
	BaseSeriesName         string         `toml:"base_series_name"`
	SeriesCount            int            `toml:"series_count"`
	WriteSettings          writeSettings  `toml:"write_settings"`
	IntColumns             []intColumn    `toml:"int_columns"`
	StringColumns          []stringColumn `toml:"string_columns"`
	FloatColumns           []floatColumn  `toml:"float_columns"`
	BoolColumns            []boolColumn   `toml:"bool_columns"`
	Queries                []query        `toml:"queries"`
	ReportSampling         int            `toml:"report_sampling"`
}

type writeSettings struct {
	BatchSeriesSize   int    `toml:"batch_series_size"`
	BatchPointsSize   int    `toml:"batch_points_size"`
	DelayBetweenPosts string `toml:"delay_between_posts"`
}

type query struct {
	Name         string `toml:"name"`
	FullQuery    string `toml:"full_query"`
	QueryStart   string `toml:"query_start"`
	QueryEnd     string `toml:"query_end"`
	PerformEvery string `toml:"perform_every"`
}

type intColumn struct {
	Name     string `toml:"name"`
	MinValue int    `toml:"min_value"`
	MaxValue int    `toml:"max_value"`
}

type floatColumn struct {
	Name     string  `toml:"name"`
	MinValue float64 `toml:"min_value"`
	MaxValue float64 `toml:"max_value"`
}

type boolColumn struct {
	Name string `toml:"name"`
}

type stringColumn struct {
	Name         string   `toml:"name"`
	Values       []string `toml:"values"`
	RandomLength int      `toml:"random_length"`
}

func main() {
	configFile := flag.String("config", "benchmark_config.sample.toml", "Config file")

	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	data, err := ioutil.ReadFile(*configFile)
	if err != nil {
		panic(err)
	}
	var conf benchmarkConfig
	if _, err := toml.Decode(string(data), &conf); err != nil {
		panic(err)
	}
	logFile, err := os.OpenFile(conf.LogFile, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		panic(fmt.Sprintf("Error opening log file \"%s\": %s", conf.LogFile, err))
	}
	conf.Log = logFile
	defer logFile.Close()
	fmt.Println("Logging benchmark results to ", conf.LogFile)
	logFile.WriteString("Starting benchmark run...\n")

	harness := NewBenchmarkHarness(&conf)

	startTime := time.Now()
	harness.Run()
	elapsed := time.Now().Sub(startTime)

	message := fmt.Sprintf("Finished in %.3f seconds\n", elapsed.Seconds())
	fmt.Printf(message)
	logFile.WriteString(message)
}

type BenchmarkHarness struct {
	Config *benchmarkConfig
	writes chan *LoadWrite
	sync.WaitGroup
	success chan *successResult
	failure chan *failureResult
}

type successResult struct {
	write        *LoadWrite
	microseconds int64
}

type failureResult struct {
	write        *LoadWrite
	err          error
	microseconds int64
}

func (fr failureResult) String() string {
	return fmt.Sprintf("%s - %d", fr.err.Error(), fr.microseconds)
}

type LoadWrite struct {
	LoadDefinition *loadDefinition
	Series         []*client.Series
}

const MAX_SUCCESS_REPORTS_TO_QUEUE = 100000

func NewHttpClient(timeout time.Duration, skipVerify bool) *http.Client {
	return &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: skipVerify},
			ResponseHeaderTimeout: timeout,
			Dial: func(network, address string) (net.Conn, error) {
				return net.DialTimeout(network, address, timeout)
			},
		},
	}
}

func NewBenchmarkHarness(conf *benchmarkConfig) *BenchmarkHarness {
	rand.Seed(time.Now().UnixNano())
	harness := &BenchmarkHarness{
		Config:  conf,
		success: make(chan *successResult, MAX_SUCCESS_REPORTS_TO_QUEUE),
		failure: make(chan *failureResult, 1000)}
	harness.startPostWorkers()
	go harness.reportResults()
	return harness
}

func (self *BenchmarkHarness) Run() {
	for _, loadDef := range self.Config.LoadDefinitions {
		def := loadDef
		self.Add(1)
		go func() {
			self.runLoadDefinition(&def)
			self.Done()
		}()
	}
	self.Wait()
}

func (self *BenchmarkHarness) startPostWorkers() {
	self.writes = make(chan *LoadWrite)
	for i := 0; i < self.Config.LoadSettings.ConcurrentConnections; i++ {
		for _, s := range self.Config.Servers {
			self.writeMessage("Connecting to " + s.ConnectionString)
			go self.handleWrites(&s)
		}
	}
}

func (self *BenchmarkHarness) reportClient() *client.Client {
	clientConfig := &client.ClientConfig{
		Host:       self.Config.StatsServer.ConnectionString,
		Database:   self.Config.StatsServer.Database,
		Username:   self.Config.StatsServer.User,
		Password:   self.Config.StatsServer.Password,
		IsSecure:   self.Config.StatsServer.IsSecure,
		HttpClient: NewHttpClient(self.Config.StatsServer.Timeout.Duration, self.Config.StatsServer.SkipVerify),
	}
	c, _ := client.New(clientConfig)
	return c
}

func (self *BenchmarkHarness) reportResults() {
	c := self.reportClient()

	successColumns := []string{"response_time", "point_count", "series_count"}
	failureColumns := []string{"response_time", "message"}

	startTime := time.Now()
	lastReport := time.Now()
	totalPointCount := 0
	lastReportPointCount := 0
	for {
		select {
		case res := <-self.success:
			pointCount := 0
			seriesCount := len(res.write.Series)
			for _, s := range res.write.Series {
				pointCount += len(s.Points)
			}
			totalPointCount += pointCount
			postedSinceLastReport := totalPointCount - lastReportPointCount
			if postedSinceLastReport > self.Config.OutputAfterCount {
				now := time.Now()
				totalPerSecond := float64(totalPointCount) / now.Sub(startTime).Seconds()
				runPerSecond := float64(postedSinceLastReport) / now.Sub(lastReport).Seconds()
				self.writeMessage(fmt.Sprintf("This Interval: %d points. %.0f per second. Run Total: %d points. %.0f per second.",
					postedSinceLastReport,
					runPerSecond,
					totalPointCount,
					totalPerSecond))
				lastReport = now
				lastReportPointCount = totalPointCount
			}

			s := &client.Series{
				Name:    res.write.LoadDefinition.Name + ".ok",
				Columns: successColumns,
				Points:  [][]interface{}{{res.microseconds / 1000, pointCount, seriesCount}}}
			c.WriteSeries([]*client.Series{s})

		case res := <-self.failure:
			s := &client.Series{
				Name:    res.write.LoadDefinition.Name + ".ok",
				Columns: failureColumns,
				Points:  [][]interface{}{{res.microseconds / 1000, res.err}}}
			c.WriteSeries([]*client.Series{s})
		}
	}
}

func (self *BenchmarkHarness) runLoadDefinition(loadDef *loadDefinition) {
	seriesNames := make([]string, loadDef.SeriesCount, loadDef.SeriesCount)
	for i := 0; i < loadDef.SeriesCount; i++ {
		seriesNames[i] = fmt.Sprintf("%s_%d", loadDef.BaseSeriesName, i)
	}
	columnCount := len(loadDef.IntColumns) + len(loadDef.BoolColumns) + len(loadDef.FloatColumns) + len(loadDef.StringColumns)
	columns := make([]string, 0, columnCount)
	for _, col := range loadDef.IntColumns {
		columns = append(columns, col.Name)
	}
	for _, col := range loadDef.BoolColumns {
		columns = append(columns, col.Name)
	}
	for _, col := range loadDef.FloatColumns {
		columns = append(columns, col.Name)
	}
	for _, col := range loadDef.StringColumns {
		columns = append(columns, col.Name)
	}

	for _, q := range loadDef.Queries {
		query := q
		go self.runQuery(loadDef, seriesNames, &query)
	}

	requestCount := self.Config.LoadSettings.RunPerLoadDefinition

	if requestCount != 0 {
		for i := 0; i < requestCount; i++ {
			self.runLoad(seriesNames, columns, loadDef)
		}
		return
	} else {
		// run forever
		for {
			self.runLoad(seriesNames, columns, loadDef)
		}
	}
}

func (self *BenchmarkHarness) runLoad(seriesNames []string, columns []string, loadDef *loadDefinition) {
	columnCount := len(columns)
	sleepTime, shouldSleep := time.ParseDuration(loadDef.WriteSettings.DelayBetweenPosts)

	pointsPosted := 0
	for j := 0; j < len(seriesNames); j += loadDef.WriteSettings.BatchSeriesSize {
		names := seriesNames[j : j+loadDef.WriteSettings.BatchSeriesSize]
		seriesToPost := make([]*client.Series, len(names), len(names))
		for ind, name := range names {
			s := &client.Series{Name: name, Columns: columns, Points: make([][]interface{}, loadDef.WriteSettings.BatchPointsSize, loadDef.WriteSettings.BatchPointsSize)}
			for pointCount := 0; pointCount < loadDef.WriteSettings.BatchPointsSize; pointCount++ {
				pointsPosted++
				point := make([]interface{}, 0, columnCount)
				for _, col := range loadDef.IntColumns {
					point = append(point, rand.Intn(col.MaxValue))
				}
				for n := 0; n < len(loadDef.BoolColumns); n++ {
					point = append(point, rand.Intn(2) == 0)
				}
				for n := 0; n < len(loadDef.FloatColumns); n++ {
					point = append(point, rand.Float64())
				}
				for _, col := range loadDef.StringColumns {
					if col.RandomLength != 0 {
						point = append(point, self.randomString(col.RandomLength))
					} else {
						point = append(point, col.Values[rand.Intn(len(col.Values))])
					}
				}

				s.Points[pointCount] = point
			}
			seriesToPost[ind] = s
		}
		self.writes <- &LoadWrite{LoadDefinition: loadDef, Series: seriesToPost}
	}
	if shouldSleep == nil {
		time.Sleep(sleepTime)
	}
}

func (self *BenchmarkHarness) randomString(length int) string {
	const alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, length)
	crand.Read(bytes)
	for i, b := range bytes {
		bytes[i] = alphanum[b%byte(len(alphanum))]
	}
	return string(bytes)
}

func (self *BenchmarkHarness) runQuery(loadDef *loadDefinition, seriesNames []string, q *query) {
	sleepTime, err := time.ParseDuration(q.PerformEvery)
	if err != nil {
		panic("Queries must have a perform_every value. Couldn't parse " + q.PerformEvery)
	}
	for {
		if q.FullQuery != "" {
			go self.queryAndReport(loadDef, q, q.FullQuery)
		} else {
			for _, name := range seriesNames {
				go self.queryAndReport(loadDef, q, q.QueryStart+" "+name+" "+q.QueryEnd)
			}
		}
		time.Sleep(sleepTime)
	}
}

func (self *BenchmarkHarness) queryAndReport(loadDef *loadDefinition, q *query, queryString string) {
	s := self.Config.Servers[rand.Intn(len(self.Config.Servers))]
	clientConfig := &client.ClientConfig{
		Host:       s.ConnectionString,
		Database:   self.Config.ClusterCredentials.Database,
		Username:   self.Config.ClusterCredentials.User,
		Password:   self.Config.ClusterCredentials.Password,
		IsSecure:   self.Config.ClusterCredentials.IsSecure,
		HttpClient: NewHttpClient(self.Config.ClusterCredentials.Timeout.Duration, self.Config.ClusterCredentials.SkipVerify),
	}
	c, err := client.New(clientConfig)
	if err != nil {
		// report query fail
	}
	startTime := time.Now()
	results, err := c.Query(queryString)
	microsecondsTaken := time.Now().Sub(startTime).Nanoseconds() / 1000

	if err != nil {
		self.reportQueryFailure(q.Name, queryString, err.Error())
	} else {
		self.reportQuerySuccess(results, q.Name, microsecondsTaken)
	}
}

func (self *BenchmarkHarness) reportQueryFailure(name, query, message string) {
	c := self.reportClient()
	s := &client.Series{
		Name:    name + ".fail",
		Columns: []string{"message"},
		Points:  [][]interface{}{{message}}}
	c.WriteSeries([]*client.Series{s})
	self.writeMessage(fmt.Sprintf("QUERY: %s failed with: %s", query, message))
}

func (self *BenchmarkHarness) reportQuerySuccess(results []*client.Series, seriesName string, microseconds int64) {
	c := self.reportClient()
	seriesCount := len(results)
	pointCount := 0
	for _, series := range results {
		pointCount += len(series.Points)
	}
	millisecondsTaken := microseconds / 1000
	s := &client.Series{
		Name:    seriesName + ".ok",
		Columns: []string{"response_time", "series_returned", "points_returned"},
		Points:  [][]interface{}{{millisecondsTaken, seriesCount, pointCount}}}
	c.WriteSeries([]*client.Series{s})
	self.writeMessage(fmt.Sprintf("QUERY: %s completed in %dms", seriesName, millisecondsTaken))
}

func (self *BenchmarkHarness) handleWrites(s *server) {
	clientConfig := &client.ClientConfig{
		Host:       s.ConnectionString,
		Database:   self.Config.ClusterCredentials.Database,
		Username:   self.Config.ClusterCredentials.User,
		Password:   self.Config.ClusterCredentials.Password,
		IsSecure:   self.Config.ClusterCredentials.IsSecure,
		HttpClient: NewHttpClient(self.Config.ClusterCredentials.Timeout.Duration, self.Config.ClusterCredentials.SkipVerify),
	}
	c, err := client.New(clientConfig)
	if err != nil {
		panic(fmt.Sprintf("Error connecting to server \"%s\": %s", s.ConnectionString, err))
	}
	for {
		write := <-self.writes

		startTime := time.Now()
		err := c.WriteSeries(write.Series)
		microsecondsTaken := time.Now().Sub(startTime).Nanoseconds() / 1000

		if err != nil {
			self.reportFailure(&failureResult{write: write, err: err, microseconds: microsecondsTaken})
		} else {
			self.reportSuccess(&successResult{write: write, microseconds: microsecondsTaken})
		}
	}
}

func (self *BenchmarkHarness) writeMessage(message string) {
	fmt.Println(message)
	self.Config.Log.WriteString(message + "\n")
}

func (self *BenchmarkHarness) reportSuccess(success *successResult) {
	if len(self.success) == MAX_SUCCESS_REPORTS_TO_QUEUE {
		self.writeMessage("Success reporting queue backed up. Dropping report.")
		return
	}
	self.success <- success
}

func (self *BenchmarkHarness) reportFailure(failure *failureResult) {
	self.writeMessage(fmt.Sprint("FAILURE: ", failure))
	self.failure <- failure
}
