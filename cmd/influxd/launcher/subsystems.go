package launcher

// Subsystem names used as labeledCloser labels, /ready check names, and
// /health check names. One canonical string per subsystem, used wherever
// the launcher surfaces that subsystem's identity. The launcher passes
// SubsystemKV into bolt.NewKVStore via bolt.WithCheckName so the bolt
// store's CheckName, the ReadyGate name, the labeledCloser label, and
// the /health response name all derive from the value defined here.
//
// These values are a de-facto public contract: they appear in /health's
// checks[].name, and for the subsystems that gate readiness they also appear
// in /ready's check list and in ReadyCheckNames. Failing initialization
// registers a check under the name of the phase that failed (see
// Launcher.failSubsystem), so every name here can reach a probe. Renaming one
// is an observable change for anything scraping those endpoints.
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

	// Names below this point exist only to attribute a startup failure: the
	// phases they name have no health check of their own once they succeed.
	SubsystemFlagger           = "feature-flags"
	SubsystemAuthorization     = "authorization"
	SubsystemAuthorizationV1   = "authorization-v1"
	SubsystemSecrets           = "secrets"
	SubsystemMetaClient        = "meta-client"
	SubsystemNotificationRules = "notification-rules"
	SubsystemLabels            = "labels"
	SubsystemAPI               = "api"

	// SubsystemMetaStore names the store-type-agnostic part of metadata store
	// setup: the KV migrations and the unknown-store-type case, which run for
	// every --store value. SubsystemKV ("bolt") names one implementation of
	// that role, so attributing a migration failure to it would report a bolt
	// problem on an instance running --store=memory with no bolt file at all.
	// A bolt migration failure therefore reports as meta-store; the message
	// still names the operation, and the migration phase genuinely is not the
	// store-open phase.
	SubsystemMetaStore = "meta-store"
)
