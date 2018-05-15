package main

import (
	"fmt"
	"golang.org/x/text/unicode/norm"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
)

func normalizeString(s string) []byte {
	result := norm.NFC.String(strings.TrimSpace(s))
	re := regexp.MustCompile(`\r?\n`)
	return []byte(re.ReplaceAllString(result, "\r\n"))
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("usage: normalize <filename>")
		return
	}
	fname := os.Args[1]
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		fmt.Printf("could not open file %s", fname)
		return
	}
	data = normalizeString(string(data))
	ioutil.WriteFile(fname, data, 0644)
}
