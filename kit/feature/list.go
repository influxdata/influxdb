// Code generated by the feature package; DO NOT EDIT.

package feature

var appMetrics = MakeBoolFlag(
	"App Metrics",
	"appMetrics",
	"Bucky, Monitoring Team",
	false,
	Permanent,
	true,
)

// AppMetrics - Send UI Telementry to Tools cluster - should always be false in OSS
func AppMetrics() BoolFlag {
	return appMetrics
}

var groupWindowAggregateTranspose = MakeBoolFlag(
	"Group Window Aggregate Transpose",
	"groupWindowAggregateTranspose",
	"Query Team",
	false,
	Temporary,
	false,
)

// GroupWindowAggregateTranspose - Enables the GroupWindowAggregateTransposeRule for all enabled window aggregates
func GroupWindowAggregateTranspose() BoolFlag {
	return groupWindowAggregateTranspose
}

var newLabels = MakeBoolFlag(
	"New Label Package",
	"newLabels",
	"Alirie Gray",
	false,
	Temporary,
	false,
)

// NewLabelPackage - Enables the refactored labels api
func NewLabelPackage() BoolFlag {
	return newLabels
}

var memoryOptimizedFill = MakeBoolFlag(
	"Memory Optimized Fill",
	"memoryOptimizedFill",
	"Query Team",
	false,
	Temporary,
	false,
)

// MemoryOptimizedFill - Enable the memory optimized fill()
func MemoryOptimizedFill() BoolFlag {
	return memoryOptimizedFill
}

var memoryOptimizedSchemaMutation = MakeBoolFlag(
	"Memory Optimized Schema Mutation",
	"memoryOptimizedSchemaMutation",
	"Query Team",
	false,
	Temporary,
	false,
)

// MemoryOptimizedSchemaMutation - Enable the memory optimized schema mutation functions
func MemoryOptimizedSchemaMutation() BoolFlag {
	return memoryOptimizedSchemaMutation
}

var queryTracing = MakeBoolFlag(
	"Query Tracing",
	"queryTracing",
	"Query Team",
	false,
	Permanent,
	false,
)

// QueryTracing - Turn on query tracing for queries that are sampled
func QueryTracing() BoolFlag {
	return queryTracing
}

var bandPlotType = MakeBoolFlag(
	"Band Plot Type",
	"bandPlotType",
	"Monitoring Team",
	true,
	Temporary,
	true,
)

// BandPlotType - Enables the creation of a band plot in Dashboards
func BandPlotType() BoolFlag {
	return bandPlotType
}

var mosaicGraphType = MakeBoolFlag(
	"Mosaic Graph Type",
	"mosaicGraphType",
	"Monitoring Team",
	true,
	Temporary,
	true,
)

// MosaicGraphType - Enables the creation of a mosaic graph in Dashboards
func MosaicGraphType() BoolFlag {
	return mosaicGraphType
}

var notebooks = MakeBoolFlag(
	"Notebooks",
	"notebooks",
	"Monitoring Team",
	false,
	Temporary,
	true,
)

// Notebooks - Determine if the notebook feature's route and navbar icon are visible to the user
func Notebooks() BoolFlag {
	return notebooks
}

var notebooksApi = MakeBoolFlag(
	"Notebooks Service API",
	"notebooksApi",
	"Edge Team",
	false,
	Temporary,
	true,
)

// NotebooksServiceApi - Enable the Equivalent notebooksd Service API
func NotebooksServiceApi() BoolFlag {
	return notebooksApi
}

var injectLatestSuccessTime = MakeBoolFlag(
	"Inject Latest Success Time",
	"injectLatestSuccessTime",
	"Compute Team",
	false,
	Temporary,
	false,
)

// InjectLatestSuccessTime - Inject the latest successful task run timestamp into a Task query extern when executing.
func InjectLatestSuccessTime() BoolFlag {
	return injectLatestSuccessTime
}

var enforceOrgDashboardLimits = MakeBoolFlag(
	"Enforce Organization Dashboard Limits",
	"enforceOrgDashboardLimits",
	"Compute Team",
	false,
	Temporary,
	false,
)

// EnforceOrganizationDashboardLimits - Enforces the default limit params for the dashboards api when orgs are set
func EnforceOrganizationDashboardLimits() BoolFlag {
	return enforceOrgDashboardLimits
}

var timeFilterFlags = MakeBoolFlag(
	"Time Filter Flags",
	"timeFilterFlags",
	"Compute Team",
	false,
	Temporary,
	true,
)

// TimeFilterFlags - Filter task run list based on before and after flags
func TimeFilterFlags() BoolFlag {
	return timeFilterFlags
}

var axisTicksGenerator = MakeBoolFlag(
	"Axis Tick Generator",
	"axisTicksGenerator",
	"Monitoring Team",
	true,
	Temporary,
	true,
)

// AxisTickGenerator - Allows for controlling how many axis ticks there are on a graph
func AxisTickGenerator() BoolFlag {
	return axisTicksGenerator
}

var csvUploader = MakeBoolFlag(
	"UI CSV Uploader",
	"csvUploader",
	"Monitoring Team",
	true,
	Temporary,
	true,
)

// UiCsvUploader - Adds the ability to upload data from a CSV file to a bucket
func UiCsvUploader() BoolFlag {
	return csvUploader
}

var editTelegrafs = MakeBoolFlag(
	"Editable Telegraf Configurations",
	"editTelegrafs",
	"Monitoring Team",
	true,
	Temporary,
	true,
)

// EditableTelegrafConfigurations - Edit telegraf configurations from the UI
func EditableTelegrafConfigurations() BoolFlag {
	return editTelegrafs
}

var legendOrientation = MakeBoolFlag(
	"Legend Orientation in the UI",
	"legendOrientation",
	"Monitoring Team",
	true,
	Temporary,
	true,
)

// LegendOrientationInTheUi - Change the appearance of the legend
func LegendOrientationInTheUi() BoolFlag {
	return legendOrientation
}

var cursorAtEOF = MakeBoolFlag(
	"Default Monaco Selection to EOF",
	"cursorAtEOF",
	"Monitoring Team",
	false,
	Temporary,
	true,
)

// DefaultMonacoSelectionToEof - Positions the cursor at the end of the line(s) when using the monaco editor
func DefaultMonacoSelectionToEof() BoolFlag {
	return cursorAtEOF
}

var refreshSingleCell = MakeBoolFlag(
	"Refresh Single Cell",
	"refreshSingleCell",
	"Monitoring Team",
	true,
	Temporary,
	true,
)

// RefreshSingleCell - Refresh a single cell on the dashboard rather than the entire dashboard
func RefreshSingleCell() BoolFlag {
	return refreshSingleCell
}

var typeAheadVariableDropdown = MakeBoolFlag(
	"Type Ahead Dropdowns for Variables",
	"typeAheadVariableDropdown",
	"Monitoring Team",
	false,
	Temporary,
	true,
)

// TypeAheadDropdownsForVariables - Enables type ahead dropdowns for variables
func TypeAheadDropdownsForVariables() BoolFlag {
	return typeAheadVariableDropdown
}

var annotations = MakeBoolFlag(
	"Annotations UI",
	"annotations",
	"Monitoring Team",
	false,
	Temporary,
	true,
)

// AnnotationsUi - Management, display, and manual addition of Annotations from the UI
func AnnotationsUi() BoolFlag {
	return annotations
}

var all = []Flag{
	appMetrics,
	groupWindowAggregateTranspose,
	newLabels,
	memoryOptimizedFill,
	memoryOptimizedSchemaMutation,
	queryTracing,
	bandPlotType,
	mosaicGraphType,
	notebooks,
	notebooksApi,
	injectLatestSuccessTime,
	enforceOrgDashboardLimits,
	timeFilterFlags,
	axisTicksGenerator,
	csvUploader,
	editTelegrafs,
	legendOrientation,
	cursorAtEOF,
	refreshSingleCell,
	typeAheadVariableDropdown,
	annotations,
}

var byKey = map[string]Flag{
	"appMetrics":                    appMetrics,
	"groupWindowAggregateTranspose": groupWindowAggregateTranspose,
	"newLabels":                     newLabels,
	"memoryOptimizedFill":           memoryOptimizedFill,
	"memoryOptimizedSchemaMutation": memoryOptimizedSchemaMutation,
	"queryTracing":                  queryTracing,
	"bandPlotType":                  bandPlotType,
	"mosaicGraphType":               mosaicGraphType,
	"notebooks":                     notebooks,
	"notebooksApi":                  notebooksApi,
	"injectLatestSuccessTime":       injectLatestSuccessTime,
	"enforceOrgDashboardLimits":     enforceOrgDashboardLimits,
	"timeFilterFlags":               timeFilterFlags,
	"axisTicksGenerator":            axisTicksGenerator,
	"csvUploader":                   csvUploader,
	"editTelegrafs":                 editTelegrafs,
	"legendOrientation":             legendOrientation,
	"cursorAtEOF":                   cursorAtEOF,
	"refreshSingleCell":             refreshSingleCell,
	"typeAheadVariableDropdown":     typeAheadVariableDropdown,
	"annotations":                   annotations,
}
