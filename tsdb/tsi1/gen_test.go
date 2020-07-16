//go:generate sh -c "curl -L https://github.com/influxdata/testdata/raw/master/tsi1testdata.tar.gz | tar xz"
package tsi1_test

import "os"

func init() {
	if _, err := os.Stat("./testdata"); err != nil {
		panic("Run go generate to download testdata directory.")
	}
}
