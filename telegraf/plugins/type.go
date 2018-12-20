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
