//go:build !assets
// +build !assets

package static

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"

	"go.uber.org/zap"
)

// asset returns swaggerData if present;
// otherwise it looks in the following locations in this order:
// the location specified in environment variable INFLUXDB_SWAGGER_YML_PATH;
// <path to executable>/../../http/swagger.yml (binary built with make but without assets tag);
// <path to executable>/http/swagger.yml (user ran go build ./cmd/influxd && ./influxd);
// ./http/swagger.yml (user ran go run ./cmd/influxd).
//
// None of these lookups happen in production builds, which have the assets build tag.
func (s *swaggerLoader) asset(swaggerData []byte, _ error) ([]byte, error) {
	if len(swaggerData) > 0 {
		return swaggerData, nil
	}

	path := findSwaggerPath(s.log)
	if path == "" {
		// Couldn't find it.
		return nil, errors.New("this developer binary not built with assets, and could not locate swagger.yml on disk")
	}

	b, err := ioutil.ReadFile(path)
	if err != nil {
		s.log.Info("Unable to load swagger.yml from disk", zap.String("path", path), zap.Error(err))
		return nil, errors.New("this developer binary not built with assets, and unable to load swagger.yml from disk")
	}

	s.log.Info("Successfully loaded swagger.yml", zap.String("path", path))

	return b, nil
}

// findSwaggerPath makes a best-effort to find the path of the swagger file on disk.
// If it can't find the path, it returns the empty string.
func findSwaggerPath(log *zap.Logger) string {
	// First, look for environment variable pointing at swagger.
	path := os.Getenv("INFLUXDB_VALID_SWAGGER_PATH")
	if path != "" {
		// Environment variable set.
		return path
	}

	log.Info("INFLUXDB_VALID_SWAGGER_PATH not set; falling back to checking relative paths")

	// Get the path to the executable so we can do a relative lookup.
	execPath, err := os.Executable()
	if err != nil {
		// Give up.
		log.Info("Can't determine path of currently running executable", zap.Error(err))
		return ""
	}

	execDir := filepath.Dir(execPath)

	// Assume the executable is in bin/$OS/, i.e. the developer built with make, but somehow without assets.
	path = filepath.Join(execDir, "..", "..", "http", "swagger.yml")
	if _, err := os.Stat(path); err == nil {
		// Looks like we found it.
		return path
	}

	// We didn't build from make... maybe the developer ran something like "go build ./cmd/influxd && ./influxd".
	path = filepath.Join(execDir, "http", "swagger.yml")
	if _, err := os.Stat(path); err == nil {
		// Looks like we found it.
		return path
	}

	// Maybe they're in the influxdb root, and ran something like "go run ./cmd/influxd".
	wd, err := os.Getwd()
	if err == nil {
		path = filepath.Join(wd, "http", "swagger.yml")
		if _, err := os.Stat(path); err == nil {
			// Looks like we found it.
			return path
		}
	}

	log.Info("Couldn't guess path to swagger definition")
	return ""
}
