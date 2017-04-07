package dist

import (
	"net/http"
	"os"
)

// Dir functions like http.Dir except returns the content of a default file if not found.
type Dir struct {
	Default string
	dir     http.Dir
}

// NewDir constructs a Dir with a default file
func NewDir(dir, def string) Dir {
	return Dir{
		Default: def,
		dir:     http.Dir(dir),
	}
}

// Open will return the file in the dir if it exists, or, the Default file otherwise.
func (d Dir) Open(name string) (http.File, error) {
	f, err := d.dir.Open(name)
	if err != nil {
		f, err = os.Open(d.Default)
		if err != nil {
			return nil, err
		}
		return f, nil
	}
	return f, err
}
