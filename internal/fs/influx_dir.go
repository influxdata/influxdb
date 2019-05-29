package fs

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strings"
)

// InfluxDir retrieves the influxdbv2 directory.
func InfluxDir() (string, error) {
	var dir string
	// By default, store meta and data files in current users home directory
	u, err := user.Current()
	if err == nil {
		dir = u.HomeDir
	} else if home := os.Getenv("HOME"); home != "" {
		dir = home
	} else {
		wd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		dir = wd
	}
	dir = filepath.Join(dir, ".influxdbv2")

	return dir, nil
}

// BoltFile returns the path to the bolt file for influxdb
func BoltFile() (string, error) {
	dir, err := InfluxDir()
	if err != nil {
		return "", err
	}
	var file string
	filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if file != "" {
			return fmt.Errorf("bolt file found")
		}

		if strings.Contains(p, ".bolt") {
			file = p
		}

		return nil
	})

	if file == "" {
		return "", fmt.Errorf("bolt file not found")
	}

	return file, nil
}

// ExpandFiles expands any directories listed in paths and filters out any
// paths which do not match the regular expression pattern.
func ExpandFiles(paths []string, pattern string) ([]string, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	var other []string
	for _, path := range paths {
		if err := filepath.Walk(path, func(path string, fi os.FileInfo, err error) error {
			if !fi.IsDir() && re.MatchString(path) {
				other = append(other, path)
			}
			return err
		}); err != nil {
			return nil, err
		}
	}
	return other, nil
}
