package pkger

import (
	"fmt"
	"os"
	"path"
	"testing"
)

var (
	missedTemplateCacheCounter int64
	availableTemplateFiles     = map[string][]byte{}
)

func TestMain(m *testing.M) {
	// this is to prime the files so we don't have to keep reading from disk for each test
	// cuts runtime of tests down by 80% on current mac
	files, _ := os.ReadDir("testdata")
	for _, f := range files {
		relativeName := path.Join("testdata", f.Name())
		b, err := os.ReadFile(relativeName)
		if err == nil {
			availableTemplateFiles[relativeName] = b
		}
	}
	exitCode := m.Run()
	if missedTemplateCacheCounter > 0 {
		fmt.Println("templates that missed cache: ", missedTemplateCacheCounter)
	}
	os.Exit(exitCode)
}
