package chronograf

import (
	"context"
	"net/http"
	"time"
)

// General errors.
const (
	ErrUpstreamTimeout   = Error("request to backend timed out")
	ErrSourceNotFound    = Error("source not found")
	ErrServerNotFound    = Error("server not found")
	ErrLayoutNotFound    = Error("layout not found")
	ErrDashboardNotFound = Error("dashboard not found")
	ErrUserNotFound      = Error("user not found")
	ErrLayoutInvalid     = Error("layout is invalid")
	ErrAlertNotFound     = Error("alert not found")
	ErrAuthentication    = Error("user not authenticated")
	ErrUninitialized     = Error("client uninitialized. Call Open() method")
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
	Warn(...interface{})
	Error(...interface{})
	Fatal(...interface{})
	Panic(...interface{})

	WithField(string, interface{}) Logger
}

// Assets returns a handler to serve the website.
type Assets interface {
	Handler() http.Handler
}

// TimeSeries represents a queryable time series database.
type TimeSeries interface {
	// Query retrieves time series data from the database.
	Query(context.Context, Query) (Response, error)
	// Connect will connect to the time series using the information in `Source`.
	Connect(context.Context, *Source) error
}

// Range represents an upper and lower bound for data
type Range struct {
	Upper int64 `json:"upper"` // Upper is the upper bound
	Lower int64 `json:"lower"` // Lower is the lower bound
}

// Query retrieves a Response from a TimeSeries.
type Query struct {
	Command  string   `json:"query"`              // Command is the query itself
	DB       string   `json:"db,omitempty"`       // DB is optional and if empty will not be used.
	RP       string   `json:"rp,omitempty"`       // RP is a retention policy and optional; if empty will not be used.
	Wheres   []string `json:"wheres,omitempty"`   // Wheres restricts the query to certain attributes
	GroupBys []string `json:"groupbys,omitempty"` // GroupBys collate the query by these tags
	Label    string   `json:"label,omitempty"`    // Label is the Y-Axis label for the data
	Range    *Range   `json:"range,omitempty"`    // Range is the default Y-Axis range for the data
}

// Response is the result of a query against a TimeSeries
type Response interface {
	MarshalJSON() ([]byte, error)
}

// Source is connection information to a time-series data store.
type Source struct {
	ID                 int    `json:"id,omitempty,string"`          // ID is the unique ID of the source
	Name               string `json:"name"`                         // Name is the user-defined name for the source
	Type               string `json:"type,omitempty"`               // Type specifies which kinds of source (enterprise vs oss)
	Username           string `json:"username,omitempty"`           // Username is the username to connect to the source
	Password           string `json:"password,omitempty"`           // Password is in CLEARTEXT
	URL                string `json:"url"`                          // URL are the connections to the source
	InsecureSkipVerify bool   `json:"insecureSkipVerify,omitempty"` // InsecureSkipVerify as true means any certificate presented by the source is accepted.
	Default            bool   `json:"default"`                      // Default specifies the default source for the application
	Telegraf           string `json:"telegraf"`                     // Telegraf is the db telegraf is written to.  By default it is "telegraf"
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

// AlertRule represents rules for building a tickscript alerting task
type AlertRule struct {
	ID            string          `json:"id,omitempty"`         // ID is the unique ID of the alert
	Query         QueryConfig     `json:"query"`                // Query is the filter of data for the alert.
	Every         string          `json:"every"`                // Every how often to check for the alerting criteria
	Alerts        []string        `json:"alerts"`               // Alerts name all the services to notify (e.g. pagerduty)
	AlertNodes    []KapacitorNode `json:"alertNodes,omitempty"` // AlertNodes define additional arguments to alerts
	Message       string          `json:"message"`              // Message included with alert
	Details       string          `json:"details"`              // Details is generally used for the Email alert.  If empty will not be added.
	Trigger       string          `json:"trigger"`              // Trigger is a type that defines when to trigger the alert
	TriggerValues TriggerValues   `json:"values"`               // Defines the values that cause the alert to trigger
	Name          string          `json:"name"`                 // Name is the user-defined name for the alert
}

// AlertRulesStore stores rules for building tickscript alerting tasks
type AlertRulesStore interface {
	// All returns all rules in the store for the given source and kapacitor id
	All(ctx context.Context, sourceID, kapaID int) ([]AlertRule, error)
	// Add creates a new rule in the AlertRulesStore and returns AlertRule with ID for a given source and kapacitor id
	Add(ctx context.Context, sourceID, kapaID int, rule AlertRule) (AlertRule, error)
	// Delete the AlertRule from the store for a given source and kapacitor ID
	Delete(ctx context.Context, sourceID, kapaID int, rule AlertRule) error
	// Get retrieves AlertRule if `ID` exists within a given source and kapacitor id
	Get(ctx context.Context, sourceID, kapaID int, ID string) (AlertRule, error)
	// Update the AlertRule in the store within a given source and kapacitor id
	Update(ctx context.Context, sourceID, kapaID int, rule AlertRule) error
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
	Change     string `json:"change,omitempty"`     // Change specifies if the change is a percent or absolute
	Period     string `json:"period,omitempty"`     // Period length of time before deadman is alerted
	Shift      string `json:"shift,omitempty"`      // Shift is the amount of time to look into the past for the alert to compare to the present
	Operator   string `json:"operator,omitempty"`   // Operator for alert comparison
	Value      string `json:"value,omitempty"`      // Value is the boundary value when alert goes critical
	RangeValue string `json:"rangeValue,omitempty"` // RangeValue is an optional value for range comparisons
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
	RawText         string              `json:"rawText,omitempty"`
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
	ID       int    // ID is the unique ID of the server
	SrcID    int    // SrcID of the data source
	Name     string // Name is the user-defined name for the server
	Username string // Username is the username to connect to the server
	Password string // Password is in CLEARTEXT
	URL      string // URL are the connections to the server
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

// UserID is a unique ID for a source user.
type UserID int

// User represents an authenticated user.
type User struct {
	ID    UserID `json:"id"`
	Email string `json:"email"`
}

// UsersStore is the Storage and retrieval of authentication information
type UsersStore interface {
	// Create a new User in the UsersStore
	Add(context.Context, *User) (*User, error)
	// Delete the User from the UsersStore
	Delete(context.Context, *User) error
	// Get retrieves a user if `ID` exists.
	Get(ctx context.Context, ID UserID) (*User, error)
	// Update the user's permissions or roles
	Update(context.Context, *User) error
	// FindByEmail will retrieve a user by email address.
	FindByEmail(ctx context.Context, Email string) (*User, error)
}

// DashboardID is the dashboard ID
type DashboardID int

// Dashboard represents all visual and query data for a dashboard
type Dashboard struct {
	ID    DashboardID     `json:"id"`
	Cells []DashboardCell `json:"cells"`
	Name  string          `json:"name"`
}

// DashboardCell holds visual and query information for a cell
type DashboardCell struct {
	X       int32   `json:"x"`
	Y       int32   `json:"y"`
	W       int32   `json:"w"`
	H       int32   `json:"h"`
	Name    string  `json:"name"`
	Queries []Query `json:"queries"`
	Type    string  `json:"type"`
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
	X       int32   `json:"x"`
	Y       int32   `json:"y"`
	W       int32   `json:"w"`
	H       int32   `json:"h"`
	I       string  `json:"i"`
	Name    string  `json:"name"`
	Queries []Query `json:"queries"`
	Type    string  `json:"type"`
}

// Layout is a collection of Cells for visualization
type Layout struct {
	ID          string `json:"id"`
	Application string `json:"app"`
	Measurement string `json:"measurement"`
	Autoflow    bool   `json:"autoflow"`
	Cells       []Cell `json:"cells"`
}

// LayoutStore stores dashboards and associated Cells
type LayoutStore interface {
	// All returns all dashboards in the store
	All(context.Context) ([]Layout, error)
	// Add creates a new dashboard in the LayoutStore
	Add(context.Context, Layout) (Layout, error)
	// Delete the dashboard from the store
	Delete(context.Context, Layout) error
	// Get retrieves Layout if `ID` exists
	Get(ctx context.Context, ID string) (Layout, error)
	// Update the dashboard in the store.
	Update(context.Context, Layout) error
}

// Principal is any entity that can be authenticated
type Principal string

// PrincipalKey is used to pass principal
// via context.Context to request-scoped
// functions.
const PrincipalKey Principal = "principal"

// Authenticator represents a service for authenticating users.
type Authenticator interface {
	// Authenticate returns User associated with token if successful.
	Authenticate(ctx context.Context, token string) (Principal, error)
	// Token generates a valid token for Principal lasting a duration
	Token(context.Context, Principal, time.Duration) (string, error)
}

// TokenExtractor extracts tokens from http requests
type TokenExtractor interface {
	// Extract will return the token or an error.
	Extract(r *http.Request) (string, error)
}
