package kapacitor

// ErrNotChronoTickscript signals a TICKscript that cannot be parsed into
// chronograf data structure.
const ErrNotChronoTickscript = Error("TICKscript not built with chronograf builder")

// Error are kapacitor errors due to communication or processing of TICKscript to kapacitor
type Error string

func (e Error) Error() string {
	return string(e)
}
