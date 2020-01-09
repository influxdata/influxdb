package endpoints

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/influxdata/influxdb"
	pctx "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/notification/endpoint"
	"github.com/influxdata/influxdb/pkg/api"
)

const prefix = "/api/v2/notificationEndpoints"

type HTTPServer struct {
	chi.Router

	api *api.API
	svc influxdb.NotificationEndpointService
}

func NewHTTPServer(api *api.API, svc influxdb.NotificationEndpointService) *HTTPServer {
	svr := &HTTPServer{
		api: api,
		svc: svc,
	}

	r := chi.NewRouter()
	r.Use(
		middleware.Recoverer,
		middleware.RequestID,
		middleware.RealIP,
	)
	{
		r.Route(svr.Prefix(), func(r chi.Router) {
			r.Post("/", svr.createEndpoint)
		})
	}
	svr.Router = r

	return svr
}

func (s *HTTPServer) Prefix() string {
	return prefix
}

type ReqPostEndpoint struct {
	Description string   `json:"description"`
	Labels      []string `json:"labels"`
	Name        string   `json:"name"`
	OrgID       string   `json:"orgID"`
	Status      string   `json:"status"`
	Type        string   `json:"type"`
	URL         string   `json:"url"`

	// http specifics
	AuthMethod      string               `json:"authMethod"`
	ContentTemplate string               `json:"contentTemplate"`
	Headers         map[string]string    `json:"headers"`
	Method          string               `json:"method"`
	Password        influxdb.SecretField `json:"password"`
	Token           influxdb.SecretField `json:"token"`
	Username        influxdb.SecretField `json:"username"`

	// PagerDuty specifics
	ClientURL  string               `json:"clientURL"`
	RoutingKey influxdb.SecretField `json:"routingKey"`
}

func (r ReqPostEndpoint) toNotificationEndpoint() (influxdb.NotificationEndpoint, error) {
	base := influxdb.EndpointBase{
		Name:        r.Name,
		Description: r.Description,
		Status:      influxdb.Status(r.Status),
	}

	orgID, err := influxdb.IDFromString(r.OrgID)
	if err != nil {
		return nil, err
	}
	base.OrgID = *orgID

	for _, l := range r.Labels {
		base.Labels = append(base.Labels, influxdb.Label{Name: l})
	}

	switch r.Type {
	case TypeHTTP:
		return &endpoint.HTTP{
			EndpointBase:    base,
			AuthMethod:      r.AuthMethod,
			ContentTemplate: r.ContentTemplate,
			Headers:         r.Headers,
			Method:          r.Method,
			Password:        r.Password,
			Username:        r.Username,
			Token:           r.Token,
			URL:             r.URL,
		}, nil
	case TypePagerDuty:
		return &endpoint.PagerDuty{
			EndpointBase: base,
			ClientURL:    r.ClientURL,
			RoutingKey:   r.RoutingKey,
		}, nil
	case TypeSlack:
		return &endpoint.Slack{
			EndpointBase: base,
			URL:          r.URL,
			Token:        r.Token,
		}, nil
	default:
		return nil, &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  fmt.Sprintf("invalid type %q provided; valid type must be 1 in [%s]"+r.Type, strings.Join(availableTypes, ", ")),
		}
	}
}

type (
	respHTTP struct {
		AuthMethod      string               `json:"authMethod"`
		AuthToken       influxdb.SecretField `json:"token"`
		ContentTemplate string               `json:"contentTemplate"`
		Headers         map[string]string    `json:"headers"`
		Method          string               `json:"method"`
		Password        influxdb.SecretField `json:"password"`
		URL             string               `json:"url"`
		Username        influxdb.SecretField `json:"username"`
	}

	respPagerDuty struct {
		ClientURL  string               `json:"clientURL"`
		RoutingKey influxdb.SecretField `json:"routingKey"`
	}

	respSlack struct {
		Token      influxdb.SecretField `json:"token"`
		WebhookURL string               `json:"url"`
	}
)

type RespPostEndpoint struct {
	influxdb.EndpointBase
	Type  string `json:"type"`
	Links Links  `json:"links"`

	http      respHTTP
	pagerDuty respPagerDuty
	slack     respSlack
}

func (r RespPostEndpoint) MarshalJSON() ([]byte, error) {
	switch r.Type {
	case TypeHTTP:
		return json.Marshal(struct {
			influxdb.EndpointBase
			Type  string `json:"type"`
			Links Links  `json:"links"`
			respHTTP
		}{
			EndpointBase: r.EndpointBase,
			Type:         r.Type,
			Links:        r.Links,
			respHTTP:     r.http,
		})
	case TypePagerDuty:
		return json.Marshal(struct {
			influxdb.EndpointBase
			Type  string `json:"type"`
			Links Links  `json:"links"`
			respPagerDuty
		}{
			EndpointBase:  r.EndpointBase,
			Type:          r.Type,
			Links:         r.Links,
			respPagerDuty: r.pagerDuty,
		})
	case TypeSlack:
		return json.Marshal(struct {
			influxdb.EndpointBase
			Type  string `json:"type"`
			Links Links  `json:"links"`
			respSlack
		}{
			EndpointBase: r.EndpointBase,
			Type:         r.Type,
			Links:        r.Links,
			respSlack:    r.slack,
		})
	default:
		return nil, errors.New("invalid endpoint type")
	}
}

func toRespPostEndpoint(edp influxdb.NotificationEndpoint) RespPostEndpoint {
	resp := RespPostEndpoint{
		EndpointBase: *edp.Base(),
		Type:         edp.Type(),
		Links:        Links(edp.Base().ID),
	}
	switch t := edp.(type) {
	case *endpoint.HTTP:
		resp.http = respHTTP{
			AuthMethod:      t.AuthMethod,
			ContentTemplate: t.ContentTemplate,
			Headers:         t.Headers,
			Method:          t.Method,
			Password:        t.Password,
			AuthToken:       t.Token,
			URL:             t.URL,
			Username:        t.Username,
		}
		if t.Headers == nil {
			t.Headers = make(map[string]string)
		}
	case *endpoint.PagerDuty:
		resp.pagerDuty = respPagerDuty{
			ClientURL:  t.ClientURL,
			RoutingKey: t.RoutingKey,
		}
	case *endpoint.Slack:
		resp.slack = respSlack{
			Token:      t.Token,
			WebhookURL: t.URL,
		}
	}

	return resp
}

func (s *HTTPServer) createEndpoint(w http.ResponseWriter, r *http.Request) {
	var reqBody ReqPostEndpoint
	if err := s.api.DecodeJSON(r.Body, &reqBody); err != nil {
		s.api.Err(w, err)
		return
	}
	defer r.Body.Close()

	edp, err := reqBody.toNotificationEndpoint()
	if err != nil {
		s.api.Err(w, err)
		return
	}

	auth, err := pctx.GetAuthorizer(r.Context())
	if err != nil {
		s.api.Err(w, err)
		return
	}

	if err := s.svc.Create(r.Context(), auth.GetUserID(), edp); err != nil {
		s.api.Err(w, err)
		return
	}

	s.api.Respond(w, http.StatusCreated, toRespPostEndpoint(edp))
}

type Links uint64

func (e Links) Self() string {
	return path.Join(prefix, influxdb.ID(e).String())
}

func (e Links) Labels() string {
	return path.Join(e.Self(), "labels")
}

func (e Links) Members() string {
	return path.Join(e.Self(), "members")
}

func (e Links) Owners() string {
	return path.Join(e.Self(), "owners")
}

func (e Links) MarshalJSON() ([]byte, error) {
	m := map[string]string{
		"self":    e.Self(),
		"labels":  e.Labels(),
		"members": e.Members(),
		"owners":  e.Owners(),
	}
	return json.Marshal(m)
}

func (e *Links) UnmarshalJSON(b []byte) error {
	var f struct {
		Self string `json:"self"`
	}
	if err := json.Unmarshal(b, &f); err != nil {
		return err
	}

	lastIdx := strings.LastIndex(f.Self, "/")
	if lastIdx == -1 || lastIdx == len(f.Self)-1 {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Msg:  "invalid link provided",
		}
	}

	id, err := influxdb.IDFromString(f.Self[lastIdx+1:])
	if err != nil {
		return &influxdb.Error{
			Code: influxdb.EInvalid,
			Err:  err,
		}
	}
	*e = Links(*id)

	return nil
}
