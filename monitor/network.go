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

func (n *network) Diagnostics() ([]string, [][]interface{}, error) {
	h, err := os.Hostname()
	if err != nil {
		return nil, nil, err
	}

	diagnostics := map[string]interface{}{
		"hostname": h,
	}

	a := make([]string, 0, len(diagnostics))
	b := []interface{}{}
	for k, v := range diagnostics {
		a = append(a, k)
		b = append(b, v)
	}
	return a, [][]interface{}{b}, nil
}
