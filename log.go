package mrfusion

// Logger represents an abstracted structured logging implementation. It
// provides methods to trigger log messages at various alert levels and a
// WithField method to set keys for a structured log message.
type Logger interface {
	Info(...interface{})
	Warn(...interface{})
	Debug(...interface{})
	Panic(...interface{})
	Error(...interface{})

	WithField(string, interface{}) Logger
}
