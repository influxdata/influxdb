package main

import (
	"io/ioutil"
	"log"
	"os"

	yaml "github.com/ghodss/yaml"
)

// yaml2json reads yaml data from stdin and outputs json to stdout.
// There are no arguments or flags.
func main() {
	in, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		log.Fatal(err)
	}
	out, err := yaml.YAMLToJSON(in)
	if err != nil {
		log.Fatal(err)
	}
	_, err = os.Stdout.Write(out)
	if err != nil {
		log.Fatal(err)
	}
}
