package filestore

import (
	"os"
	"strings"
)

var env map[string]string

// environ returns a map of all environment variables in the running process
func environ() map[string]string {
	if env == nil {
		env = make(map[string]string)
		envVars := os.Environ()
		for _, envVar := range envVars {
			kv := strings.SplitN(envVar, "=", 2)
			if len(kv) != 2 {
				continue
			}
			env[kv[0]] = kv[1]
		}
	}
	return env
}
