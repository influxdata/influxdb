package internal

import (
	"io"
	"os"
)

// IsCharacterDevice returns true if the supplied reader is a character device (a terminal)
func IsCharacterDevice(reader io.Reader) bool {
	file, isFile := reader.(*os.File)
	if !isFile {
		return false
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) == os.ModeCharDevice
}
