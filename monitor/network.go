package monitor

import (
	"os"
)

// network captures network statistics and implements the monitor client interface
type network struct{}

// Statistics returns the statistics for the network type
func (n *network) Statistics() (map[string]interface{}, error) {
	return nil, nil
}

func (n *network) Diagnostics() (*Diagnostic, error) {
	h, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	diagnostics := map[string]interface{}{
		"hostname": h,
	}

	return DiagnosticFromMap(diagnostics), nil
}
