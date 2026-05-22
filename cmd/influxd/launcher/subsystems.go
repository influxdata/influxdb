package launcher

// Subsystem names used as labeledCloser labels, /ready check names, and
// /health check names. One canonical string per subsystem, used wherever
// the launcher surfaces that subsystem's identity. The launcher passes
// SubsystemKV into bolt.NewKVStore via bolt.WithCheckName so the bolt
// store's CheckName, the ReadyGate name, the labeledCloser label, and
// the /health response name all derive from the value defined here.
const (
	SubsystemEngine        = "engine"
	SubsystemReplications  = "replications"
	SubsystemQuery         = "query"
	SubsystemInfluxQL      = "influxql"
	SubsystemTaskScheduler = "task-scheduler"
	SubsystemTasks         = "tasks"
	SubsystemScraper       = "scraper"
	SubsystemJaeger        = "jaeger"
	SubsystemPIDFile       = "pidfile"
	SubsystemKV            = "bolt"
	SubsystemSQLite        = "sqlite"
	SubsystemHTTPServer    = "http-server"
	SubsystemShards        = "shards"
)
