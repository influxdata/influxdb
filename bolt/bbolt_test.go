package bolt_test

import (
	"context"
	"errors"
	"io/ioutil"
	"os"

	"github.com/influxdata/platform/bolt"
	"golang.org/x/crypto/bcrypt"
)

func NewTestClient() (*bolt.Client, func(), error) {
	c := bolt.NewClient()
	bolt.HashCost = bcrypt.MinCost

	f, err := ioutil.TempFile("", "influxdata-platform-bolt-")
	if err != nil {
		return nil, nil, errors.New("unable to open temporary boltdb file")
	}
	f.Close()

	c.Path = f.Name()

	if err := c.Open(context.TODO()); err != nil {
		return nil, nil, err
	}

	close := func() {
		c.Close()
		os.Remove(c.Path)
	}

	return c, close, nil
}
