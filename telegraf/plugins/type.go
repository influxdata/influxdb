package plugins

// Type is a telegraf plugin type.
type Type string

// available types.
const (
	Input      Type = "input"      // Input is an input plugin.
	Output     Type = "output"     // Output is an output plugin.
	Processor  Type = "processor"  // Processor is a processor plugin.
	Aggregator Type = "aggregator" // Aggregator is an aggregator plugin.
)

// Config interface for all plugins.
type Config interface {
	// TOML encodes to toml string
	TOML() string
	// UnmarshalTOML decodes the parsed data to the object
	UnmarshalTOML(data interface{}) error
	// Type is the plugin type
	Type() Type
	// PluginName is the string value of telegraf plugin package name.
	PluginName() string
}
