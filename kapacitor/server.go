package kapacitor

import (
	"context"

	client "github.com/influxdata/kapacitor/client/v1"
)

type Server struct {
	URL      string
	Username string
	Password string
}

const (
	templatePrefix  = "chronograf_v1_"
	templatePattern = "chronograf_v1_*"
)

// Template plus its read-only attributes.
type Template struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	TICKscript string `json:"script"`
}

func (s *Server) Templates(ctx context.Context) ([]Template, error) {
	var creds *client.Credentials
	if s.Username != "" {
		creds = &client.Credentials{
			Method:   client.UserAuthentication,
			Username: s.Username,
			Password: s.Password,
		}
	}

	kapa, err := client.New(client.Config{
		URL:         s.URL,
		Credentials: creds,
	})
	if err != nil {
		return nil, err
	}

	templates, err := kapa.ListTemplates(&client.ListTemplatesOptions{
		Pattern: templatePattern,
	})
	if err != nil {
		return nil, err
	}

	res := []Template{}
	for _, t := range templates {
		res = append(res, Template{
			ID:         t.ID,
			Type:       t.Type.String(),
			TICKscript: t.TICKscript,
		})
	}
	return res, nil
}
