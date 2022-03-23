//go:build !windows

package upgrade

// Zap only has problems with Windows paths, so this is a no-op on Unix systems.
func (o *logOptions) zapSafeLogPath() (string, error) {
	return o.logPath, nil
}
