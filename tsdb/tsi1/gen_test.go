//go:generate sh -c "curl -L https://github.com/influxdata/testdata/raw/2020.07.17.0/tsi1testdata.tar.gz | tar xz"
package tsi1_test

import (
	"fmt"
	"os"
)

func init() {
	if _, err := os.Stat("./testdata"); err != nil {
		fmt.Println("Run go generate to download testdata directory.")
		os.Exit(1)
	}
}
