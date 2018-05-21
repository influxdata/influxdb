package idfile

import (
	"io/ioutil"
	"os"

	uuid "github.com/satori/go.uuid"
)

// ID will read the id from the file or create and generate one.
func ID(filepath string) (string, error) {
	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		return newID(filepath)
	}
	octets, err := ioutil.ReadFile(filepath)
	if err != nil {
		return "", err
	}
	u, err := uuid.FromBytes(octets)
	if err != nil {
		return newID(filepath)
	}
	return u.String(), nil
}

func newID(filepath string) (string, error) {
	id := uuid.NewV4().Bytes()
	err := ioutil.WriteFile(filepath, id, 0600)
	return string(id), err
}
