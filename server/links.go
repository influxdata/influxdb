package server

// NewCustomLinks transforms `--custom-link` CLI flag data or `CUSTOM_LINKS` ENV
// var data into a data structure that the Chronograf client will expect
func NewCustomLinks(links map[string]string) ([]CustomLink, error) {
	var customLinks []CustomLink
	for name, link := range links {
		customLinks = append(customLinks, CustomLink{
			Name: name,
			Url:  link,
		})
	}
	return customLinks, nil
}
