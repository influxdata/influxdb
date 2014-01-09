package main

type benchmarkConfig struct {
	LogFile            string             `toml:"log_file"`
	StatsServer        statsServer        `toml:"stats_server"`
	Servers            []server           `toml:"servers"`
	ClusterCredentials clusterCredentials `toml:"cluster_credentials"`
	LoadSettings       loadSettings       `toml:"load_settings"`
	LoadDefinitions    []loadDefinition   `toml:"load_definitions"`
	Log                *os.File
}

type statsServer struct {
	Host     string `toml:"host"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	Database string `toml:"database"`
}

type clusterCredentials struct {
	Database string `toml:"database"`
	User     string `toml:"user"`
	Password string `toml:"password"`
}

type server struct {
	ConnectionString string `toml:"connection_string"`
}

type loadSettings struct {
	ConcurrentConnections int `toml:"concurrent_connections"`
	RunPerLoadDefinition  int `toml:"runs_per_load_definition"`
}

type loadDefinition struct {
	BaseSeriesName string         `toml:"base_series_name"`
	SeriesCount    int            `toml:"series_count"`
	WriteSettings  writeSettings  `toml:"write_settings"`
	IntColumns     []intColumn    `toml:"int_columns"`
	StringColumns  []stringColumn `toml:"string_columns"`
	FloatColumns   []floatColumn  `toml:"float_columns"`
	BoolColumns    []boolColumn   `toml:"bool_columns"`
	Queries        []query        `toml:"queries"`
}

type writeSettings struct {
	BatchSeriesSize   int    `toml:"batch_series_size"`
	BatchPointsSize   int    `toml:"batch_points_size"`
	DelayBetweenPosts string `toml:"delay_between_posts"`
}

type query struct {
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
