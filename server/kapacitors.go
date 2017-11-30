package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/bouk/httprouter"
	"github.com/influxdata/chronograf"
	kapa "github.com/influxdata/chronograf/kapacitor"
)

type postKapacitorRequest struct {
	Name               *string `json:"name"`               // User facing name of kapacitor instance.; Required: true
	URL                *string `json:"url"`                // URL for the kapacitor backend (e.g. http://localhost:9092);/ Required: true
	Username           string  `json:"username,omitempty"` // Username for authentication to kapacitor
	Password           string  `json:"password,omitempty"`
	InsecureSkipVerify bool    `json:"insecureSkipVerify,omitempty"` // InsecureSkipVerify as true means any certificate presented by the kapacitor is accepted.
	Active             bool    `json:"active"`
}

func (p *postKapacitorRequest) Valid() error {
	if p.Name == nil || p.URL == nil {
		return fmt.Errorf("name and url required")
	}

	url, err := url.ParseRequestURI(*p.URL)
	if err != nil {
		return fmt.Errorf("invalid source URI: %v", err)
	}
	if len(url.Scheme) == 0 {
		return fmt.Errorf("Invalid URL; no URL scheme defined")
	}

	return nil
}

type kapaLinks struct {
	Proxy string `json:"proxy"` // URL location of proxy endpoint for this source
	Self  string `json:"self"`  // Self link mapping to this resource
	Rules string `json:"rules"` // Rules link for defining roles alerts for kapacitor
	Tasks string `json:"tasks"` // Tasks link to define a task against the proxy
	Ping  string `json:"ping"`  // Ping path to kapacitor
}

type kapacitor struct {
	ID                 int       `json:"id,string"`          // Unique identifier representing a kapacitor instance.
	Name               string    `json:"name"`               // User facing name of kapacitor instance.
	URL                string    `json:"url"`                // URL for the kapacitor backend (e.g. http://localhost:9092)
	Username           string    `json:"username,omitempty"` // Username for authentication to kapacitor
	Password           string    `json:"password,omitempty"`
	InsecureSkipVerify bool      `json:"insecureSkipVerify,omitempty"` // InsecureSkipVerify as true means any certificate presented by the kapacitor is accepted.
	Active             bool      `json:"active"`
	Links              kapaLinks `json:"links"` // Links are URI locations related to kapacitor
}

// NewKapacitor adds valid kapacitor store store.
func (h *Service) NewKapacitor(w http.ResponseWriter, r *http.Request) {
	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	ctx := r.Context()
	_, err = h.SourcesStore.Get(ctx, srcID)
	if err != nil {
		notFound(w, srcID, h.Logger)
		return
	}

	var req postKapacitorRequest
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, h.Logger)
		return
	}
	if err := req.Valid(); err != nil {
		invalidData(w, err, h.Logger)
		return
	}

	srv := chronograf.Server{
		SrcID:              srcID,
		Name:               *req.Name,
		Username:           req.Username,
		Password:           req.Password,
		InsecureSkipVerify: req.InsecureSkipVerify,
		URL:                *req.URL,
		Active:             req.Active,
	}

	if srv, err = h.ServersStore.Add(ctx, srv); err != nil {
		msg := fmt.Errorf("Error storing kapacitor %v: %v", req, err)
		unknownErrorWithMessage(w, msg, h.Logger)
		return
	}

	res := newKapacitor(srv)
	w.Header().Add("Location", res.Links.Self)
	encodeJSON(w, http.StatusCreated, res, h.Logger)
}

func newKapacitor(srv chronograf.Server) kapacitor {
	httpAPISrcs := "/chronograf/v1/sources"
	return kapacitor{
		ID:                 srv.ID,
		Name:               srv.Name,
		Username:           srv.Username,
		URL:                srv.URL,
		Active:             srv.Active,
		InsecureSkipVerify: srv.InsecureSkipVerify,
		Links: kapaLinks{
			Self:  fmt.Sprintf("%s/%d/kapacitors/%d", httpAPISrcs, srv.SrcID, srv.ID),
			Proxy: fmt.Sprintf("%s/%d/kapacitors/%d/proxy", httpAPISrcs, srv.SrcID, srv.ID),
			Rules: fmt.Sprintf("%s/%d/kapacitors/%d/rules", httpAPISrcs, srv.SrcID, srv.ID),
			Tasks: fmt.Sprintf("%s/%d/kapacitors/%d/proxy?path=/kapacitor/v1/tasks", httpAPISrcs, srv.SrcID, srv.ID),
			Ping:  fmt.Sprintf("%s/%d/kapacitors/%d/proxy?path=/kapacitor/v1/ping", httpAPISrcs, srv.SrcID, srv.ID),
		},
	}
}

type kapacitors struct {
	Kapacitors []kapacitor `json:"kapacitors"`
}

// Kapacitors retrieves all kapacitors from store.
func (h *Service) Kapacitors(w http.ResponseWriter, r *http.Request) {
	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	ctx := r.Context()
	mrSrvs, err := h.ServersStore.All(ctx)
	if err != nil {
		Error(w, http.StatusInternalServerError, "Error loading kapacitors", h.Logger)
		return
	}

	srvs := []kapacitor{}
	for _, srv := range mrSrvs {
		if srv.SrcID == srcID {
			srvs = append(srvs, newKapacitor(srv))
		}
	}

	res := kapacitors{
		Kapacitors: srvs,
	}

	encodeJSON(w, http.StatusOK, res, h.Logger)
}

// KapacitorsID retrieves a kapacitor with ID from store.
func (h *Service) KapacitorsID(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	ctx := r.Context()
	srv, err := h.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id, h.Logger)
		return
	}

	res := newKapacitor(srv)
	encodeJSON(w, http.StatusOK, res, h.Logger)
}

// RemoveKapacitor deletes kapacitor from store.
func (h *Service) RemoveKapacitor(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	ctx := r.Context()
	srv, err := h.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id, h.Logger)
		return
	}

	if err = h.ServersStore.Delete(ctx, srv); err != nil {
		unknownErrorWithMessage(w, err, h.Logger)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type patchKapacitorRequest struct {
	Name               *string `json:"name,omitempty"`     // User facing name of kapacitor instance.
	URL                *string `json:"url,omitempty"`      // URL for the kapacitor
	Username           *string `json:"username,omitempty"` // Username for kapacitor auth
	Password           *string `json:"password,omitempty"`
	InsecureSkipVerify *bool   `json:"insecureSkipVerify,omitempty"` // InsecureSkipVerify as true means any certificate presented by the kapacitor is accepted.
	Active             *bool   `json:"active"`
}

func (p *patchKapacitorRequest) Valid() error {
	if p.URL != nil {
		url, err := url.ParseRequestURI(*p.URL)
		if err != nil {
			return fmt.Errorf("invalid source URI: %v", err)
		}
		if len(url.Scheme) == 0 {
			return fmt.Errorf("Invalid URL; no URL scheme defined")
		}
	}
	return nil
}

// UpdateKapacitor incrementally updates a kapacitor definition in the store
func (h *Service) UpdateKapacitor(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	ctx := r.Context()
	srv, err := h.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id, h.Logger)
		return
	}

	var req patchKapacitorRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, h.Logger)
		return
	}

	if err := req.Valid(); err != nil {
		invalidData(w, err, h.Logger)
		return
	}

	if req.Name != nil {
		srv.Name = *req.Name
	}
	if req.URL != nil {
		srv.URL = *req.URL
	}
	if req.Password != nil {
		srv.Password = *req.Password
	}
	if req.Username != nil {
		srv.Username = *req.Username
	}
	if req.InsecureSkipVerify != nil {
		srv.InsecureSkipVerify = *req.InsecureSkipVerify
	}
	if req.Active != nil {
		srv.Active = *req.Active
	}

	if err := h.ServersStore.Update(ctx, srv); err != nil {
		msg := fmt.Sprintf("Error updating kapacitor ID %d", id)
		Error(w, http.StatusInternalServerError, msg, h.Logger)
		return
	}

	res := newKapacitor(srv)
	encodeJSON(w, http.StatusOK, res, h.Logger)
}

// KapacitorRulesPost proxies POST to kapacitor
func (h *Service) KapacitorRulesPost(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	ctx := r.Context()
	srv, err := h.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id, h.Logger)
		return
	}

	c := kapa.NewClient(srv.URL, srv.Username, srv.Password, srv.InsecureSkipVerify)

	var req chronograf.AlertRule
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, h.Logger)
		return
	}
	// TODO: validate this data
	/*
		if err := req.Valid(); err != nil {
			invalidData(w, err)
			return
		}
	*/

	task, err := c.Create(ctx, req)
	if err != nil {
		Error(w, http.StatusInternalServerError, err.Error(), h.Logger)
		return
	}
	res := newAlertResponse(task, srv.SrcID, srv.ID)
	w.Header().Add("Location", res.Links.Self)
	encodeJSON(w, http.StatusCreated, res, h.Logger)
}

type alertLinks struct {
	Self      string `json:"self"`
	Kapacitor string `json:"kapacitor"`
	Output    string `json:"output"`
}

type alertResponse struct {
	chronograf.AlertRule
	Links alertLinks `json:"links"`
}

// newAlertResponse formats task into an alertResponse
func newAlertResponse(task *kapa.Task, srcID, kapaID int) *alertResponse {
	res := &alertResponse{
		AlertRule: task.Rule,
		Links: alertLinks{
			Self:      fmt.Sprintf("/chronograf/v1/sources/%d/kapacitors/%d/rules/%s", srcID, kapaID, task.ID),
			Kapacitor: fmt.Sprintf("/chronograf/v1/sources/%d/kapacitors/%d/proxy?path=%s", srcID, kapaID, url.QueryEscape(task.Href)),
			Output:    fmt.Sprintf("/chronograf/v1/sources/%d/kapacitors/%d/proxy?path=%s", srcID, kapaID, url.QueryEscape(task.HrefOutput)),
		},
	}

	if res.AlertHandlers.Alerta == nil {
		res.AlertHandlers.Alerta = []*chronograf.Alerta{}
	}

	for i, a := range res.AlertHandlers.Alerta {
		if a.Service == nil {
			a.Service = []string{}
			res.AlertHandlers.Alerta[i] = a
		}
	}

	if res.AlertHandlers.Email == nil {
		res.AlertHandlers.Email = []*chronograf.Email{}
	}

	for i, a := range res.AlertHandlers.Email {
		if a.To == nil {
			a.To = []string{}
			res.AlertHandlers.Email[i] = a
		}
	}

	if res.AlertHandlers.Exec == nil {
		res.AlertHandlers.Exec = []*chronograf.Exec{}
	}

	for i, a := range res.AlertHandlers.Exec {
		if a.Command == nil {
			a.Command = []string{}
			res.AlertHandlers.Exec[i] = a
		}
	}

	if res.AlertHandlers.HipChat == nil {
		res.AlertHandlers.HipChat = []*chronograf.HipChat{}
	}

	if res.AlertHandlers.Log == nil {
		res.AlertHandlers.Log = []*chronograf.Log{}
	}

	if res.AlertHandlers.OpsGenie == nil {
		res.AlertHandlers.OpsGenie = []*chronograf.OpsGenie{}
	}

	for i, a := range res.AlertHandlers.OpsGenie {
		if a.Teams == nil {
			a.Teams = []string{}
			res.AlertHandlers.OpsGenie[i] = a
		}

		if a.Recipients == nil {
			a.Recipients = []string{}
			res.AlertHandlers.OpsGenie[i] = a
		}
	}

	if res.AlertHandlers.PagerDuty == nil {
		res.AlertHandlers.PagerDuty = []*chronograf.PagerDuty{}
	}

	if res.AlertHandlers.Posts == nil {
		res.AlertHandlers.Posts = []*chronograf.Post{}
	}

	for i, a := range res.AlertHandlers.Posts {
		if a.Headers == nil {
			a.Headers = map[string]string{}
			res.AlertHandlers.Posts[i] = a
		}
	}

	if res.AlertHandlers.Pushover == nil {
		res.AlertHandlers.Pushover = []*chronograf.Pushover{}
	}

	if res.AlertHandlers.Sensu == nil {
		res.AlertHandlers.Sensu = []*chronograf.Sensu{}
	}

	for i, a := range res.AlertHandlers.Sensu {
		if a.Handlers == nil {
			a.Handlers = []string{}
			res.AlertHandlers.Sensu[i] = a
		}
	}

	if res.AlertHandlers.Slack == nil {
		res.AlertHandlers.Slack = []*chronograf.Slack{}
	}

	if res.AlertHandlers.Talk == nil {
		res.AlertHandlers.Talk = []*chronograf.Talk{}
	}

	if res.AlertHandlers.TCPs == nil {
		res.AlertHandlers.TCPs = []*chronograf.TCP{}
	}

	if res.AlertHandlers.Telegram == nil {
		res.AlertHandlers.Telegram = []*chronograf.Telegram{}
	}

	if res.AlertHandlers.VictorOps == nil {
		res.AlertHandlers.VictorOps = []*chronograf.VictorOps{}
	}

	if res.Query != nil {
		if res.Query.ID == "" {
			res.Query.ID = res.ID
		}

		if res.Query.Fields == nil {
			res.Query.Fields = make([]chronograf.Field, 0)
		}

		if res.Query.GroupBy.Tags == nil {
			res.Query.GroupBy.Tags = make([]string, 0)
		}

		if res.Query.Tags == nil {
			res.Query.Tags = make(map[string][]string)
		}
	}
	return res
}

// ValidRuleRequest checks if the requested rule change is valid
func ValidRuleRequest(rule chronograf.AlertRule) error {
	if rule.Query == nil {
		return fmt.Errorf("invalid alert rule: no query defined")
	}
	var hasFuncs bool
	for _, f := range rule.Query.Fields {
		if f.Type == "func" && len(f.Args) > 0 {
			hasFuncs = true
		}
	}
	// All kapacitor rules with functions must have a window that is applied
	// every amount of time
	if rule.Every == "" && hasFuncs {
		return fmt.Errorf(`invalid alert rule: functions require an "every" window`)
	}
	return nil
}

// KapacitorRulesPut proxies PATCH to kapacitor
func (h *Service) KapacitorRulesPut(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	ctx := r.Context()
	srv, err := h.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id, h.Logger)
		return
	}

	tid := httprouter.GetParamFromContext(ctx, "tid")
	c := kapa.NewClient(srv.URL, srv.Username, srv.Password, srv.InsecureSkipVerify)
	var req chronograf.AlertRule
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, h.Logger)
		return
	}
	// TODO: validate this data
	/*
		if err := req.Valid(); err != nil {
			invalidData(w, err)
			return
		}
	*/

	// Check if the rule exists and is scoped correctly
	if _, err = c.Get(ctx, tid); err != nil {
		if err == chronograf.ErrAlertNotFound {
			notFound(w, id, h.Logger)
			return
		}
		Error(w, http.StatusInternalServerError, err.Error(), h.Logger)
		return
	}

	// Replace alert completely with this new alert.
	req.ID = tid
	task, err := c.Update(ctx, c.Href(tid), req)
	if err != nil {
		Error(w, http.StatusInternalServerError, err.Error(), h.Logger)
		return
	}
	res := newAlertResponse(task, srv.SrcID, srv.ID)
	encodeJSON(w, http.StatusOK, res, h.Logger)
}

// KapacitorStatus is the current state of a running task
type KapacitorStatus struct {
	Status string `json:"status"`
}

// Valid check if the kapacitor status is enabled or disabled
func (k *KapacitorStatus) Valid() error {
	if k.Status == "enabled" || k.Status == "disabled" {
		return nil
	}
	return fmt.Errorf("Invalid Kapacitor status: %s", k.Status)
}

// KapacitorRulesStatus proxies PATCH to kapacitor to enable/disable tasks
func (h *Service) KapacitorRulesStatus(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	ctx := r.Context()
	srv, err := h.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id, h.Logger)
		return
	}

	tid := httprouter.GetParamFromContext(ctx, "tid")
	c := kapa.NewClient(srv.URL, srv.Username, srv.Password, srv.InsecureSkipVerify)

	var req KapacitorStatus
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		invalidJSON(w, h.Logger)
		return
	}
	if err := req.Valid(); err != nil {
		invalidData(w, err, h.Logger)
		return
	}

	// Check if the rule exists and is scoped correctly
	_, err = c.Get(ctx, tid)
	if err != nil {
		if err == chronograf.ErrAlertNotFound {
			notFound(w, id, h.Logger)
			return
		}
		Error(w, http.StatusInternalServerError, err.Error(), h.Logger)
		return
	}

	var task *kapa.Task
	if req.Status == "enabled" {
		task, err = c.Enable(ctx, c.Href(tid))
	} else {
		task, err = c.Disable(ctx, c.Href(tid))
	}

	if err != nil {
		Error(w, http.StatusInternalServerError, err.Error(), h.Logger)
		return
	}

	res := newAlertResponse(task, srv.SrcID, srv.ID)
	encodeJSON(w, http.StatusOK, res, h.Logger)
}

// KapacitorRulesGet retrieves all rules
func (h *Service) KapacitorRulesGet(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	ctx := r.Context()
	srv, err := h.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id, h.Logger)
		return
	}

	c := kapa.NewClient(srv.URL, srv.Username, srv.Password, srv.InsecureSkipVerify)
	tasks, err := c.All(ctx)
	if err != nil {
		Error(w, http.StatusInternalServerError, err.Error(), h.Logger)
		return
	}

	res := allAlertsResponse{
		Rules: []*alertResponse{},
	}
	for _, task := range tasks {
		ar := newAlertResponse(task, srv.SrcID, srv.ID)
		res.Rules = append(res.Rules, ar)
	}
	encodeJSON(w, http.StatusOK, res, h.Logger)
}

type allAlertsResponse struct {
	Rules []*alertResponse `json:"rules"`
}

// KapacitorRulesID retrieves specific task
func (h *Service) KapacitorRulesID(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	ctx := r.Context()
	srv, err := h.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id, h.Logger)
		return
	}
	tid := httprouter.GetParamFromContext(ctx, "tid")

	c := kapa.NewClient(srv.URL, srv.Username, srv.Password, srv.InsecureSkipVerify)

	// Check if the rule exists within scope
	task, err := c.Get(ctx, tid)
	if err != nil {
		if err == chronograf.ErrAlertNotFound {
			notFound(w, id, h.Logger)
			return
		}
		Error(w, http.StatusInternalServerError, err.Error(), h.Logger)
		return
	}

	res := newAlertResponse(task, srv.SrcID, srv.ID)
	encodeJSON(w, http.StatusOK, res, h.Logger)
}

// KapacitorRulesDelete proxies DELETE to kapacitor
func (h *Service) KapacitorRulesDelete(w http.ResponseWriter, r *http.Request) {
	id, err := paramID("kid", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	srcID, err := paramID("id", r)
	if err != nil {
		Error(w, http.StatusUnprocessableEntity, err.Error(), h.Logger)
		return
	}

	ctx := r.Context()
	srv, err := h.ServersStore.Get(ctx, id)
	if err != nil || srv.SrcID != srcID {
		notFound(w, id, h.Logger)
		return
	}

	c := kapa.NewClient(srv.URL, srv.Username, srv.Password, srv.InsecureSkipVerify)

	tid := httprouter.GetParamFromContext(ctx, "tid")
	// Check if the rule is linked to this server and kapacitor
	if _, err := c.Get(ctx, tid); err != nil {
		if err == chronograf.ErrAlertNotFound {
			notFound(w, id, h.Logger)
			return
		}
		Error(w, http.StatusInternalServerError, err.Error(), h.Logger)
		return
	}
	if err := c.Delete(ctx, c.Href(tid)); err != nil {
		Error(w, http.StatusInternalServerError, err.Error(), h.Logger)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
