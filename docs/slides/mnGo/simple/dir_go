// +build OMIT
import (
	"net/http"
	"os"
)

type Dir struct {
	Default string
	dir     http.Dir
}

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

// OMIT END
