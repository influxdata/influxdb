package server

type Google struct {
    OAuth2Provider
	Domains          []string // Optional google email domain checking
}
