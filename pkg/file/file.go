package file

import (
	"fmt"
	"io"
	"os"

	"github.com/influxdata/influxdb/pkg/errors"
)

func copyFile(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}

	out, err := os.Create(dst)
	if err != nil {
		return err
	}

	defer errors.Capture(&err, out.Close)()

	defer errors.Capture(&err, in.Close)()

	if _, err = io.Copy(out, in); err != nil {
		return err
	}

	return out.Sync()
}

// MoveFileWithReplacement copies the file contents at `src` to `dst`.
// and deletes `src` on success.
//
// If the file at `dst` already exists, it will be truncated and its contents
// overwritten.
func MoveFileWithReplacement(src, dst string) error {
	if err := copyFile(src, dst); err != nil {
		return fmt.Errorf("copy: %w", err)
	}

	return os.Remove(src)
}
