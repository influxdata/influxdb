package filestore

import (
	"bytes"
	"html/template"
)

// templated returns all files templated using data
func templated(data interface{}, filenames ...string) ([]byte, error) {
	t, err := template.ParseFiles(filenames...)
	if err != nil {
		return nil, err
	}
	var b bytes.Buffer
	// If a key in the file exists but is not in the data we
	// immediately fail with a missing key error
	err = t.Option("missingkey=error").Execute(&b, data)
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

// templatedFromEnv returns all files templated against environment variables
func templatedFromEnv(filenames ...string) ([]byte, error) {
	return templated(environ(), filenames...)
}
