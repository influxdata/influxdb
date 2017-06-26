package server

import (
	"testing"
)

func TestNewCustomLinks(t *testing.T) {
	customLinks := map[string]string{
		"cubeapple": "https://cube.apple",
	}
	if _, err := NewCustomLinks(customLinks); err != nil {
		t.Errorf("Unknown error in NewCustomLinks: %v", err)
	}

	customLinks = map[string]string{
		"": "https://cube.apple",
	}
	if _, err := NewCustomLinks(customLinks); err == nil {
		t.Error("Expected error: CustomLink missing key for Name")
	}

	customLinks = map[string]string{
		"cubeapple": "",
	}
	if _, err := NewCustomLinks(customLinks); err == nil {
		t.Error("Expected error: CustomLink missing value for URL")
	}

	customLinks = map[string]string{
		"cubeapple": ":k%8a#",
	}
	if _, err := NewCustomLinks(customLinks); err == nil {
		t.Error("Expected error: missing protocol scheme")
	}
}
