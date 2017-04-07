// +build OMIT
package main

import (
	"log"
	"net/http"
)

func main() {
	log.Fatal(http.ListenAndServe(":8888", http.FileServer(&Dir{
		Default: "src/ui/index.html",
		dir:     http.Dir("src/ui"),
	})))
}
