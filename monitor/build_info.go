package monitor

// system captures build diagnostics
type build struct {
	Version string
	Commit  string
	Branch  string
}

func (b *build) Diagnostics() (*Diagnostic, error) {
	diagnostics := map[string]interface{}{
		"Version": b.Version,
		"Commit":  b.Commit,
		"Branch":  b.Branch,
	}

	return DiagnosticFromMap(diagnostics), nil
}
