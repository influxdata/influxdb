package chronograf

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/influxdata/influxdb/influxql"
)

// General errors.
const (
	ErrUpstreamTimeout      = Error("request to backend timed out")
	ErrSourceNotFound       = Error("source not found")
	ErrServerNotFound       = Error("server not found")
	ErrLayoutNotFound       = Error("layout not found")
	ErrDashboardNotFound    = Error("dashboard not found")
	ErrUserNotFound         = Error("user not found")
	ErrOrganizationNotFound = Error("organization not found")
	ErrLayoutInvalid        = Error("layout is invalid")
	ErrAlertNotFound        = Error("alert not found")
	ErrAuthentication       = Error("user not authenticated")
	ErrUninitialized        = Error("client uninitialized. Call Open() method")
	ErrInvalidAxis          = Error("Unexpected axis in cell. Valid axes are 'x', 'y', and 'y2'")
)

// Error is a domain error encountered while processing chronograf requests
type Error string

func (e Error) Error() string {
	return string(e)
}

// Logger represents an abstracted structured logging implementation. It
// provides methods to trigger log messages at various alert levels and a
// WithField method to set keys for a structured log message.
type Logger interface {
	Debug(...interface{})
	Info(...interface{})
	Error(...interface{})

	WithField(string, interface{}) Logger

	// Logger can be transformed into an io.Writer.
	// That writer is the end of an io.Pipe and it is your responsibility to close it.
	Writer() *io.PipeWriter
}

// Router is an abstracted Router based on the API provided by the
// julienschmidt/httprouter package.
type Router interface {
	http.Handler
	GET(string, http.HandlerFunc)
	PATCH(string, http.HandlerFunc)
	POST(string, http.HandlerFunc)
	DELETE(string, http.HandlerFunc)
	PUT(string, http.HandlerFunc)

	Handler(string, string, http.Handler)
}

// Assets returns a handler to serve the website.
type Assets interface {
	Handler() http.Handler
}

// Supported time-series databases
const (
	// InfluxDB is the open-source time-series database
	InfluxDB = "influx"
	// InfluxEnteprise is the clustered HA time-series database
	InfluxEnterprise = "influx-enterprise"
	// InfluxRelay is the basic HA layer over InfluxDB
	InfluxRelay = "influx-relay"
)

// TSDBStatus represents the current status of a time series database
type TSDBStatus interface {
	// Connect will connect to the time series using the information in `Source`.
	Connect(ctx context.Context, src *Source) error
	// Ping returns version and TSDB type of time series database if reachable.
	Ping(context.Context) error
	// Version returns the version of the TSDB database
	Version(context.Context) (string, error)
	// Type returns the type of the TSDB database
	Type(context.Context) (string, error)
}

// TimeSeries represents a queryable time series database.
type TimeSeries interface {
	// Query retrieves time series data from the database.
	Query(context.Context, Query) (Response, error)
	// Connect will connect to the time series using the information in `Source`.
	Connect(context.Context, *Source) error
	// UsersStore represents the user accounts within the TimeSeries database
	Users(context.Context) UsersStore
	// Permissions returns all valid names permissions in this database
	Permissions(context.Context) Permissions
	// Roles represents the roles associated with this TimesSeriesDatabase
	Roles(context.Context) (RolesStore, error)
}

// Role is a restricted set of permissions assigned to a set of users.
type Role struct {
	Name           string      `json:"name"`
	Permissions    Permissions `json:"permissions,omitempty"`
	Users          []User      `json:"users,omitempty"`
	OrganizationID string      `json:"organization,string,omitempty"`
}

// RolesStore is the Storage and retrieval of authentication information
type RolesStore interface {
	// All lists all roles from the RolesStore
	All(context.Context) ([]Role, error)
	// Create a new Role in the RolesStore
	Add(context.Context, *Role) (*Role, error)
	// Delete the Role from the RolesStore
	Delete(context.Context, *Role) error
	// Get retrieves a role if name exists.
	Get(ctx context.Context, name string) (*Role, error)
	// Update the roles' users or permissions
	Update(context.Context, *Role) error
}

// Range represents an upper and lower bound for data
type Range struct {
	Upper int64 `json:"upper"` // Upper is the upper bound
	Lower int64 `json:"lower"` // Lower is the lower bound
}

type TemplateVariable interface {
	fmt.Stringer
	Name() string     // returns the variable name
	Precedence() uint // ordinal indicating precedence level for replacement
}

type ExecutableVar interface {
	Exec(string)
}

// TemplateValue is a value use to replace a template in an InfluxQL query
type BasicTemplateValue struct {
	Value    string `json:"value"`    // Value is the specific value used to replace a template in an InfluxQL query
	Type     string `json:"type"`     // Type can be tagKey, tagValue, fieldKey, csv, measurement, database, constant
	Selected bool   `json:"selected"` // Selected states that this variable has been picked to use for replacement
}

// TemplateVar is a named variable within an InfluxQL query to be replaced with Values
type BasicTemplateVar struct {
	Var    string               `json:"tempVar"` // Var is the string to replace within InfluxQL
	Values []BasicTemplateValue `json:"values"`  // Values are the replacement values within InfluxQL
}

func (t BasicTemplateVar) Name() string {
	return t.Var
}

// String converts the template variable into a correct InfluxQL string based
// on its type
func (t BasicTemplateVar) String() string {
	if len(t.Values) == 0 {
		return ""
	}
	switch t.Values[0].Type {
	case "tagKey", "fieldKey", "measurement", "database":
		return `"` + t.Values[0].Value + `"`
	case "tagValue", "timeStamp":
		return `'` + t.Values[0].Value + `'`
	case "csv", "constant":
		return t.Values[0].Value
	default:
		return ""
	}
}

func (t BasicTemplateVar) Precedence() uint {
	return 0
}

type GroupByVar struct {
	Var               string        `json:"tempVar"`                     // the name of the variable as present in the query
	Duration          time.Duration `json:"duration,omitempty"`          // the Duration supplied by the query
	Resolution        uint          `json:"resolution"`                  // the available screen resolution to render the results of this query
	ReportingInterval time.Duration `json:"reportingInterval,omitempty"` // the interval at which data is reported to this series
}

// Exec is responsible for extracting the Duration from the query
func (g *GroupByVar) Exec(query string) {
	whereClause := "WHERE"
	start := strings.Index(query, whereClause)
	if start == -1 {
		// no where clause
		return
	}

	// reposition start to after the 'where' keyword
	durStr := query[start+len(whereClause):]

	// attempt to parse out a relative time range
	dur, err := g.parseRelative(durStr)
	if err == nil {
		// we parsed relative duration successfully
		g.Duration = dur
		return
	}

	dur, err = g.parseAbsolute(durStr)
	if err == nil {
		// we found an absolute time range
		g.Duration = dur
	}
}

// parseRelative locates and extracts a duration value from a fragment of an
// InfluxQL query following the "where" keyword. For example, in the fragment
// "time > now() - 180d GROUP BY :interval:", parseRelative would return a
// duration equal to 180d
func (g *GroupByVar) parseRelative(fragment string) (time.Duration, error) {
	// locate duration literal start
	prefix := "time > now() - "
	start := strings.Index(fragment, prefix)
	if start == -1 {
		return time.Duration(0), errors.New("not a relative duration")
	}

	// reposition to duration literal
	durFragment := fragment[start+len(prefix):]

	// init counters
	pos := 0

	// locate end of duration literal
	for pos < len(durFragment) {
		rn, _ := utf8.DecodeRuneInString(durFragment[pos:])
		if unicode.IsSpace(rn) {
			break
		}
		pos++
	}

	// attempt to parse what we suspect is a duration literal
	dur, err := influxql.ParseDuration(durFragment[:pos])
	if err != nil {
		return dur, err
	}

	return dur, nil
}

// parseAbsolute will determine the duration between two absolute timestamps
// found within an InfluxQL fragment following the "where" keyword. For
// example, the fragement "time > '1985-10-25T00:01:21-0800 and time <
// '1985-10-25T00:01:22-0800'" would yield a duration of 1m'
func (g *GroupByVar) parseAbsolute(fragment string) (time.Duration, error) {
	timePtn := `time\s[>|<]\s'([0-9\-T\:\.Z]+)'` // Playground: http://gobular.com/x/208f66bd-1889-4269-ab47-1efdfeeb63f0
	re, err := regexp.Compile(timePtn)
	if err != nil {
		// this is a developer error and should complain loudly
		panic("Bad Regex: err:" + err.Error())
	}

	if !re.Match([]byte(fragment)) {
		return time.Duration(0), errors.New("absolute duration not found")
	}

	// extract at most two times
	matches := re.FindAll([]byte(fragment), 2)

	// parse out absolute times
	durs := make([]time.Time, 0, 2)
	for _, match := range matches {
		durStr := re.FindSubmatch(match)
		if tm, err := time.Parse(time.RFC3339Nano, string(durStr[1])); err == nil {
			durs = append(durs, tm)
		}
	}

	if len(durs) == 1 {
		durs = append(durs, time.Now())
	}

	// reject more than 2 times found
	if len(durs) != 2 {
		return time.Duration(0), errors.New("must provide exactly two absolute times")
	}

	dur := durs[1].Sub(durs[0])

	return dur, nil
}

func (g *GroupByVar) String() string {
	duration := int64(g.Duration/time.Second) / int64(g.Resolution) * 3
	if duration == 0 {
		duration = 1
	}
	return "time(" + strconv.Itoa(int(duration)) + "s)"
}

func (g *GroupByVar) Name() string {
	return g.Var
}

func (g *GroupByVar) Precedence() uint {
	return 1
}

// TemplateID is the unique ID used to identify a template
type TemplateID string

// Template represents a series of choices to replace TemplateVars within InfluxQL
type Template struct {
	BasicTemplateVar
	ID    TemplateID     `json:"id"`              // ID is the unique ID associated with this template
	Type  string         `json:"type"`            // Type can be fieldKeys, tagKeys, tagValues, CSV, constant, query, measurements, databases
	Label string         `json:"label"`           // Label is a user-facing description of the Template
	Query *TemplateQuery `json:"query,omitempty"` // Query is used to generate the choices for a template
}

// Query retrieves a Response from a TimeSeries.
type Query struct {
	Command      string       `json:"query"`                // Command is the query itself
	DB           string       `json:"db,omitempty"`         // DB is optional and if empty will not be used.
	RP           string       `json:"rp,omitempty"`         // RP is a retention policy and optional; if empty will not be used.
	TemplateVars TemplateVars `json:"tempVars,omitempty"`   // TemplateVars are template variables to replace within an InfluxQL query
	Wheres       []string     `json:"wheres,omitempty"`     // Wheres restricts the query to certain attributes
	GroupBys     []string     `json:"groupbys,omitempty"`   // GroupBys collate the query by these tags
	Resolution   uint         `json:"resolution,omitempty"` // Resolution is the available screen resolution to render query results
	Label        string       `json:"label,omitempty"`      // Label is the Y-Axis label for the data
	Range        *Range       `json:"range,omitempty"`      // Range is the default Y-Axis range for the data
}

// TemplateVars are a heterogeneous collection of different TemplateVariables
// with the capability to decode arbitrary JSON into the appropriate template
// variable type
type TemplateVars []TemplateVariable

func (t *TemplateVars) UnmarshalJSON(text []byte) error {
	// TODO: Need to test that server throws an error when :interval:'s Resolution or ReportingInterval or zero-value
	rawVars := bytes.NewReader(text)
	dec := json.NewDecoder(rawVars)

	// read open bracket
	rawTok, err := dec.Token()
	if err != nil {
		return err
	}

	tok, isDelim := rawTok.(json.Delim)
	if !isDelim || tok != '[' {
		return errors.New("Expected JSON array, but found " + tok.String())
	}

	for dec.More() {
		var halfBakedVar json.RawMessage
		err := dec.Decode(&halfBakedVar)
		if err != nil {
			return err
		}

		var agb GroupByVar
		err = json.Unmarshal(halfBakedVar, &agb)
		if err != nil {
			return err
		}

		// ensure that we really have a GroupByVar
		if agb.Resolution != 0 {
			(*t) = append(*t, &agb)
			continue
		}

		var tvar BasicTemplateVar
		err = json.Unmarshal(halfBakedVar, &tvar)
		if err != nil {
			return err
		}

		// ensure that we really have a BasicTemplateVar
		if len(tvar.Values) != 0 {
			(*t) = append(*t, tvar)
		}
	}
	return nil
}

// DashboardQuery includes state for the query builder.  This is a transition
// struct while we move to the full InfluxQL AST
type DashboardQuery struct {
	Command     string      `json:"query"`                 // Command is the query itself
	Label       string      `json:"label,omitempty"`       // Label is the Y-Axis label for the data
	Range       *Range      `json:"range,omitempty"`       // Range is the default Y-Axis range for the data
	QueryConfig QueryConfig `json:"queryConfig,omitempty"` // QueryConfig represents the query state that is understood by the data explorer
	Source      string      `json:"source"`                // Source is the optional URI to the data source for this queryConfig
}

// TemplateQuery is used to retrieve choices for template replacement
type TemplateQuery struct {
	Command     string `json:"influxql"`     // Command is the query itself
	DB          string `json:"db,omitempty"` // DB is optional and if empty will not be used.
	RP          string `json:"rp,omitempty"` // RP is a retention policy and optional; if empty will not be used.
	Measurement string `json:"measurement"`  // Measurement is the optinally selected measurement for the query
	TagKey      string `json:"tagKey"`       // TagKey is the optionally selected tag key for the query
	FieldKey    string `json:"fieldKey"`     // FieldKey is the optionally selected field key for the query
}

// Response is the result of a query against a TimeSeries
type Response interface {
	MarshalJSON() ([]byte, error)
}

// Source is connection information to a time-series data store.
type Source struct {
	ID                 int    `json:"id,string"`                    // ID is the unique ID of the source
	Name               string `json:"name"`                         // Name is the user-defined name for the source
	Type               string `json:"type,omitempty"`               // Type specifies which kinds of source (enterprise vs oss)
	Username           string `json:"username,omitempty"`           // Username is the username to connect to the source
	Password           string `json:"password,omitempty"`           // Password is in CLEARTEXT
	SharedSecret       string `json:"sharedSecret,omitempty"`       // ShareSecret is the optional signing secret for Influx JWT authorization
	URL                string `json:"url"`                          // URL are the connections to the source
	MetaURL            string `json:"metaUrl,omitempty"`            // MetaURL is the url for the meta node
	InsecureSkipVerify bool   `json:"insecureSkipVerify,omitempty"` // InsecureSkipVerify as true means any certificate presented by the source is accepted.
	Default            bool   `json:"default"`                      // Default specifies the default source for the application
	Telegraf           string `json:"telegraf"`                     // Telegraf is the db telegraf is written to.  By default it is "telegraf"
	Organization       string `json:"organization"`                 // Organization is the organization ID that resource belongs to
}

// SourcesStore stores connection information for a `TimeSeries`
type SourcesStore interface {
	// All returns all sources in the store
	All(context.Context) ([]Source, error)
	// Add creates a new source in the SourcesStore and returns Source with ID
	Add(context.Context, Source) (Source, error)
	// Delete the Source from the store
	Delete(context.Context, Source) error
	// Get retrieves Source if `ID` exists
	Get(ctx context.Context, ID int) (Source, error)
	// Update the Source in the store.
	Update(context.Context, Source) error
}

type DBRP struct {
	DB string `json:"db"`
	RP string `json:"rp"`
}

// AlertRule represents rules for building a tickscript alerting task
type AlertRule struct {
	ID            string          `json:"id,omitempty"`           // ID is the unique ID of the alert
	TICKScript    TICKScript      `json:"tickscript"`             // TICKScript is the raw tickscript associated with this Alert
	Query         *QueryConfig    `json:"query"`                  // Query is the filter of data for the alert.
	Every         string          `json:"every"`                  // Every how often to check for the alerting criteria
	Alerts        []string        `json:"alerts"`                 // Alerts name all the services to notify (e.g. pagerduty)
	AlertNodes    []KapacitorNode `json:"alertNodes,omitempty"`   // AlertNodes define additional arguments to alerts
	Message       string          `json:"message"`                // Message included with alert
	Details       string          `json:"details"`                // Details is generally used for the Email alert.  If empty will not be added.
	Trigger       string          `json:"trigger"`                // Trigger is a type that defines when to trigger the alert
	TriggerValues TriggerValues   `json:"values"`                 // Defines the values that cause the alert to trigger
	Name          string          `json:"name"`                   // Name is the user-defined name for the alert
	Type          string          `json:"type"`                   // Represents the task type where stream is data streamed to kapacitor and batch is queried by kapacitor
	DBRPs         []DBRP          `json:"dbrps"`                  // List of database retention policy pairs the task is allowed to access
	Status        string          `json:"status"`                 // Represents if this rule is enabled or disabled in kapacitor
	Executing     bool            `json:"executing"`              // Whether the task is currently executing
	Error         string          `json:"error"`                  // Any error encountered when kapacitor executes the task
	Created       time.Time       `json:"created"`                // Date the task was first created
	Modified      time.Time       `json:"modified"`               // Date the task was last modified
	LastEnabled   time.Time       `json:"last-enabled,omitempty"` // Date the task was last set to status enabled
}

// TICKScript task to be used by kapacitor
type TICKScript string

// Ticker generates tickscript tasks for kapacitor
type Ticker interface {
	// Generate will create the tickscript to be used as a kapacitor task
	Generate(AlertRule) (TICKScript, error)
}

// TriggerValues specifies the alerting logic for a specific trigger type
type TriggerValues struct {
	Change     string `json:"change,omitempty"`   // Change specifies if the change is a percent or absolute
	Period     string `json:"period,omitempty"`   // Period length of time before deadman is alerted
	Shift      string `json:"shift,omitempty"`    // Shift is the amount of time to look into the past for the alert to compare to the present
	Operator   string `json:"operator,omitempty"` // Operator for alert comparison
	Value      string `json:"value,omitempty"`    // Value is the boundary value when alert goes critical
	RangeValue string `json:"rangeValue"`         // RangeValue is an optional value for range comparisons
}

// Field represent influxql fields and functions from the UI
type Field struct {
	Field string   `json:"field"`
	Funcs []string `json:"funcs"`
}

// GroupBy represents influxql group by tags from the UI
type GroupBy struct {
	Time string   `json:"time"`
	Tags []string `json:"tags"`
}

// DurationRange represents the lower and upper durations of the query config
type DurationRange struct {
	Upper string `json:"upper"`
	Lower string `json:"lower"`
}

// QueryConfig represents UI query from the data explorer
type QueryConfig struct {
	ID              string              `json:"id,omitempty"`
	Database        string              `json:"database"`
	Measurement     string              `json:"measurement"`
	RetentionPolicy string              `json:"retentionPolicy"`
	Fields          []Field             `json:"fields"`
	Tags            map[string][]string `json:"tags"`
	GroupBy         GroupBy             `json:"groupBy"`
	AreTagsAccepted bool                `json:"areTagsAccepted"`
	Fill            string              `json:"fill,omitempty"`
	RawText         *string             `json:"rawText"`
	Range           *DurationRange      `json:"range"`
}

// KapacitorNode adds arguments and properties to an alert
type KapacitorNode struct {
	Name       string              `json:"name"`
	Args       []string            `json:"args"`
	Properties []KapacitorProperty `json:"properties"`
	// In the future we could add chaining methods here.
}

// KapacitorProperty modifies the node they are called on
type KapacitorProperty struct {
	Name string   `json:"name"`
	Args []string `json:"args"`
}

// Server represents a proxy connection to an HTTP server
type Server struct {
	ID           int    // ID is the unique ID of the server
	SrcID        int    // SrcID of the data source
	Name         string // Name is the user-defined name for the server
	Username     string // Username is the username to connect to the server
	Password     string // Password is in CLEARTEXT
	URL          string // URL are the connections to the server
	Active       bool   // Is this the active server for the source?
	Organization string // Organization is the organization ID that resource belongs to
}

// ServersStore stores connection information for a `Server`
type ServersStore interface {
	// All returns all servers in the store
	All(context.Context) ([]Server, error)
	// Add creates a new source in the ServersStore and returns Server with ID
	Add(context.Context, Server) (Server, error)
	// Delete the Server from the store
	Delete(context.Context, Server) error
	// Get retrieves Server if `ID` exists
	Get(ctx context.Context, ID int) (Server, error)
	// Update the Server in the store.
	Update(context.Context, Server) error
}

// ID creates uniq ID string
type ID interface {
	// Generate creates a unique ID string
	Generate() (string, error)
}

const (
	// AllScope grants permission for all databases.
	AllScope Scope = "all"
	// DBScope grants permissions for a specific database
	DBScope Scope = "database"
)

// Permission is a specific allowance for User or Role bound to a
// scope of the data source
type Permission struct {
	Scope   Scope      `json:"scope"`
	Name    string     `json:"name,omitempty"`
	Allowed Allowances `json:"allowed"`
}

// Permissions represent the entire set of permissions a User or Role may have
type Permissions []Permission

// Allowances defines what actions a user can have on a scoped permission
type Allowances []string

// Scope defines the location of access of a permission
type Scope string

// User represents an authenticated user.
type User struct {
	ID                  uint64      `json:"id,string,omitempty"`
	Name                string      `json:"name"`
	Passwd              string      `json:"password,omitempty"`
	Permissions         Permissions `json:"permissions,omitempty"`
	Roles               []Role      `json:"roles,omitempty"`
	Provider            string      `json:"provider,omitempty"`
	Scheme              string      `json:"scheme,omitempty"`
	CurrentOrganization string      `json:"currentOrganization,omitempty"`
}

// UserQuery represents the attributes that a user may be retrieved by.
// It is predominantly used in the UsersStore.Get method.
type UserQuery struct {
	ID       *uint64
	Name     *string
	Provider *string
	Scheme   *string
}

// UsersStore is the Storage and retrieval of authentication information
type UsersStore interface {
	// All lists all users from the UsersStore
	All(context.Context) ([]User, error)
	// Create a new User in the UsersStore
	Add(context.Context, *User) (*User, error)
	// Delete the User from the UsersStore
	Delete(context.Context, *User) error
	// Get retrieves a user if name exists.
	Get(ctx context.Context, q UserQuery) (*User, error)
	// Update the user's permissions or roles
	Update(context.Context, *User) error
}

// Database represents a database in a time series source
type Database struct {
	Name          string `json:"name"`                    // a unique string identifier for the database
	Duration      string `json:"duration,omitempty"`      // the duration (when creating a default retention policy)
	Replication   int32  `json:"replication,omitempty"`   // the replication factor (when creating a default retention policy)
	ShardDuration string `json:"shardDuration,omitempty"` // the shard duration (when creating a default retention policy)
}

// RetentionPolicy represents a retention policy in a time series source
type RetentionPolicy struct {
	Name          string `json:"name"`                    // a unique string identifier for the retention policy
	Duration      string `json:"duration,omitempty"`      // the duration
	Replication   int32  `json:"replication,omitempty"`   // the replication factor
	ShardDuration string `json:"shardDuration,omitempty"` // the shard duration
	Default       bool   `json:"isDefault,omitempty"`     // whether the RP should be the default
}

// Databases represents a databases in a time series source
type Databases interface {
	// All lists all databases
	AllDB(context.Context) ([]Database, error)
	Connect(context.Context, *Source) error
	CreateDB(context.Context, *Database) (*Database, error)
	DropDB(context.Context, string) error
	AllRP(context.Context, string) ([]RetentionPolicy, error)
	CreateRP(context.Context, string, *RetentionPolicy) (*RetentionPolicy, error)
	UpdateRP(context.Context, string, string, *RetentionPolicy) (*RetentionPolicy, error)
	DropRP(context.Context, string, string) error
}

// DashboardID is the dashboard ID
type DashboardID int

// Dashboard represents all visual and query data for a dashboard
type Dashboard struct {
	ID           DashboardID     `json:"id"`
	Cells        []DashboardCell `json:"cells"`
	Templates    []Template      `json:"templates"`
	Name         string          `json:"name"`
	Organization string          `json:"organization"` // Organization is the organization ID that resource belongs to
}

// Axis represents the visible extents of a visualization
type Axis struct {
	Bounds       []string `json:"bounds"` // bounds are an arbitrary list of client-defined strings that specify the viewport for a cell
	LegacyBounds [2]int64 `json:"-"`      // legacy bounds are for testing a migration from an earlier version of axis
	Label        string   `json:"label"`  // label is a description of this Axis
	Prefix       string   `json:"prefix"` // Prefix represents a label prefix for formatting axis values
	Suffix       string   `json:"suffix"` // Suffix represents a label suffix for formatting axis values
	Base         string   `json:"base"`   // Base represents the radix for formatting axis values
	Scale        string   `json:"scale"`  // Scale is the axis formatting scale. Supported: "log", "linear"
}

// DashboardCell holds visual and query information for a cell
type DashboardCell struct {
	ID      string           `json:"i"`
	X       int32            `json:"x"`
	Y       int32            `json:"y"`
	W       int32            `json:"w"`
	H       int32            `json:"h"`
	Name    string           `json:"name"`
	Queries []DashboardQuery `json:"queries"`
	Axes    map[string]Axis  `json:"axes"`
	Type    string           `json:"type"`
}

// DashboardsStore is the storage and retrieval of dashboards
type DashboardsStore interface {
	// All lists all dashboards from the DashboardStore
	All(context.Context) ([]Dashboard, error)
	// Create a new Dashboard in the DashboardStore
	Add(context.Context, Dashboard) (Dashboard, error)
	// Delete the Dashboard from the DashboardStore if `ID` exists.
	Delete(context.Context, Dashboard) error
	// Get retrieves a dashboard if `ID` exists.
	Get(ctx context.Context, id DashboardID) (Dashboard, error)
	// Update replaces the dashboard information
	Update(context.Context, Dashboard) error
}

// Cell is a rectangle and multiple time series queries to visualize.
type Cell struct {
	X       int32           `json:"x"`
	Y       int32           `json:"y"`
	W       int32           `json:"w"`
	H       int32           `json:"h"`
	I       string          `json:"i"`
	Name    string          `json:"name"`
	Queries []Query         `json:"queries"`
	Axes    map[string]Axis `json:"axes"`
	Type    string          `json:"type"`
}

// Layout is a collection of Cells for visualization
type Layout struct {
	ID           string `json:"id"`
	Application  string `json:"app"`
	Measurement  string `json:"measurement"`
	Autoflow     bool   `json:"autoflow"`
	Cells        []Cell `json:"cells"`
	Organization string `json:"organization"` // Organization is the organization ID that resource belongs to
}

// LayoutsStore stores dashboards and associated Cells
type LayoutsStore interface {
	// All returns all dashboards in the store
	All(context.Context) ([]Layout, error)
	// Add creates a new dashboard in the LayoutsStore
	Add(context.Context, Layout) (Layout, error)
	// Delete the dashboard from the store
	Delete(context.Context, Layout) error
	// Get retrieves Layout if `ID` exists
	Get(ctx context.Context, ID string) (Layout, error)
	// Update the dashboard in the store.
	Update(context.Context, Layout) error
}

// Organization is a group of resources under a common name
type Organization struct {
	ID   uint64 `json:"id"`
	Name string `json:"name"`
}

// OrganizationQuery represents the attributes that a user may be retrieved by.
// It is predominantly used in the OrganizationsStore.Get method.
type OrganizationQuery struct {
	ID   *uint64
	Name *string
}

// OrganizationsStore is the storage and retrieval of Organizations
type OrganizationsStore interface {
	// Add creates a new Organization
	Add(context.Context, *Organization) (*Organization, error)
	// All lists all Organizations in the OrganizationsStore
	All(context.Context) ([]Organization, error)
	// Delete removes an Organization from the OrganizationsStore
	Delete(context.Context, *Organization) error
	// Get retrieves an Organization from the OrganizationsStore
	Get(context.Context, OrganizationQuery) (*Organization, error)
	// Update updates an Organization in the OrganizationsStore
	Update(context.Context, *Organization) error
}
