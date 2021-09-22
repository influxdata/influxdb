//go:build !windows
// +build !windows

package tsm1

import (
	"fmt"
	"os"
)

// copyOrLink - allow substitution of a file copy for a hard link when running on Windows systems.
func copyOrLink(oldPath, newPath string) error {
	if err := os.Link(oldPath, newPath); err != nil {
		return fmt.Errorf("error creating hard link for backup from %s to %s: %q", oldPath, newPath, err)
	}
	return nil
}
