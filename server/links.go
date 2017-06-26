package server

import (
	"errors"
	"net/url"
)

type getExternalLinksResponse struct {
	StatusFeed  *string      `json:"statusFeed,omitempty"` // Location of the a JSON Feed for client's Status page News Feed
	CustomLinks []CustomLink `json:"custom,omitempty"`     // Any custom external links for client's User menu
}

// CustomLink is a handler that returns a custom link to be used in server's routes response, within ExternalLinks
type CustomLink struct {
	Name string `json:"name,omitempty"`
	URL  string `json:"url,omitempty"`
}

// NewCustomLinks transforms `--custom-link` CLI flag data or `CUSTOM_LINKS` ENV
// var data into a data structure that the Chronograf client will expect
func NewCustomLinks(links map[string]string) ([]CustomLink, error) {
	var customLinks []CustomLink
	for name, link := range links {
		if name == "" {
			return customLinks, errors.New("CustomLink missing key for Name")
		}
		if link == "" {
			return customLinks, errors.New("CustomLink missing value for URL")
		}
		_, err := url.Parse(link)
		if err != nil {
			return customLinks, err
		}

		customLinks = append(customLinks, CustomLink{
			Name: name,
			URL:  link,
		})
	}
	return customLinks, nil
}
