package pkger

import (
	"context"
	"encoding/json"
	"net/http"

	ihttp "github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
)

// HTTPRemoteService provides an http client that is fluent in all things template.
type HTTPRemoteService struct {
	Client *httpc.Client
}

var _ SVC = (*HTTPRemoteService)(nil)

func (s *HTTPRemoteService) InitStack(ctx context.Context, userID platform.ID, stack StackCreate) (Stack, error) {
	reqBody := ReqCreateStack{
		OrgID:       stack.OrgID.String(),
		Name:        stack.Name,
		Description: stack.Description,
		URLs:        stack.TemplateURLs,
	}

	var respBody RespStack
	err := s.Client.
		PostJSON(reqBody, RoutePrefixStacks).
		DecodeJSON(&respBody).
		Do(ctx)
	if err != nil {
		return Stack{}, err
	}

	return convertRespStackToStack(respBody)
}

func (s *HTTPRemoteService) UninstallStack(ctx context.Context, identifiers struct{ OrgID, UserID, StackID platform.ID }) (Stack, error) {
	var respBody RespStack
	err := s.Client.
		Post(httpc.BodyEmpty, RoutePrefixStacks, identifiers.StackID.String(), "/uninstall").
		QueryParams([2]string{"orgID", identifiers.OrgID.String()}).
		DecodeJSON(&respBody).
		Do(ctx)
	if err != nil {
		return Stack{}, err
	}

	return convertRespStackToStack(respBody)
}

func (s *HTTPRemoteService) DeleteStack(ctx context.Context, identifiers struct{ OrgID, UserID, StackID platform.ID }) error {
	return s.Client.
		Delete(RoutePrefixStacks, identifiers.StackID.String()).
		QueryParams([2]string{"orgID", identifiers.OrgID.String()}).
		Do(ctx)
}

func (s *HTTPRemoteService) ListStacks(ctx context.Context, orgID platform.ID, f ListFilter) ([]Stack, error) {
	queryParams := [][2]string{{"orgID", orgID.String()}}
	for _, name := range f.Names {
		queryParams = append(queryParams, [2]string{"name", name})
	}
	for _, stackID := range f.StackIDs {
		queryParams = append(queryParams, [2]string{"stackID", stackID.String()})
	}

	var resp RespListStacks
	err := s.Client.
		Get(RoutePrefixStacks).
		QueryParams(queryParams...).
		DecodeJSON(&resp).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]Stack, 0, len(resp.Stacks))
	for _, st := range resp.Stacks {
		stack, err := convertRespStackToStack(st)
		if err != nil {
			continue
		}
		out = append(out, stack)
	}
	return out, nil
}

func (s *HTTPRemoteService) ReadStack(ctx context.Context, id platform.ID) (Stack, error) {
	var respBody RespStack
	err := s.Client.
		Get(RoutePrefixStacks, id.String()).
		DecodeJSON(&respBody).
		Do(ctx)
	if err != nil {
		return Stack{}, err
	}
	return convertRespStackToStack(respBody)
}

func (s *HTTPRemoteService) UpdateStack(ctx context.Context, upd StackUpdate) (Stack, error) {
	reqBody := ReqUpdateStack{
		Name:         upd.Name,
		Description:  upd.Description,
		TemplateURLs: upd.TemplateURLs,
	}
	for _, r := range upd.AdditionalResources {
		reqBody.AdditionalResources = append(reqBody.AdditionalResources, ReqUpdateStackResource{
			ID:       r.ID.String(),
			MetaName: r.MetaName,
			Kind:     r.Kind,
		})
	}

	var respBody RespStack
	err := s.Client.
		PatchJSON(reqBody, RoutePrefixStacks, upd.ID.String()).
		DecodeJSON(&respBody).
		Do(ctx)
	if err != nil {
		return Stack{}, err
	}

	return convertRespStackToStack(respBody)
}

// Export will produce a template from the parameters provided.
func (s *HTTPRemoteService) Export(ctx context.Context, opts ...ExportOptFn) (*Template, error) {
	opt, err := exportOptFromOptFns(opts)
	if err != nil {
		return nil, err
	}

	var orgIDs []ReqExportOrgIDOpt
	for _, org := range opt.OrgIDs {
		orgIDs = append(orgIDs, ReqExportOrgIDOpt{
			OrgID: org.OrgID.String(),
			Filters: struct {
				ByLabel        []string `json:"byLabel"`
				ByResourceKind []Kind   `json:"byResourceKind"`
			}{
				ByLabel:        org.LabelNames,
				ByResourceKind: org.ResourceKinds,
			},
		})
	}

	reqBody := ReqExport{
		StackID:   opt.StackID.String(),
		OrgIDs:    orgIDs,
		Resources: opt.Resources,
	}

	var newTemplate *Template
	err = s.Client.
		PostJSON(reqBody, RoutePrefixTemplates, "/export").
		Decode(func(resp *http.Response) error {
			t, err := Parse(EncodingJSON, FromReader(resp.Body, "export"))
			newTemplate = t
			return err
		}).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	if err := newTemplate.Validate(ValidWithoutResources()); err != nil {
		return nil, err
	}
	return newTemplate, nil
}

// DryRun provides a dry run of the template application. The template will be marked verified
// for later calls to Apply. This func will be run on an Apply if it has not been run
// already.
func (s *HTTPRemoteService) DryRun(ctx context.Context, orgID, userID platform.ID, opts ...ApplyOptFn) (ImpactSummary, error) {
	return s.apply(ctx, orgID, true, opts...)
}

// Apply will apply all the resources identified in the provided template. The entire template will be applied
// in its entirety. If a failure happens midway then the entire template will be rolled back to the state
// from before the template was applied.
func (s *HTTPRemoteService) Apply(ctx context.Context, orgID, userID platform.ID, opts ...ApplyOptFn) (ImpactSummary, error) {
	return s.apply(ctx, orgID, false, opts...)
}

func (s *HTTPRemoteService) apply(ctx context.Context, orgID platform.ID, dryRun bool, opts ...ApplyOptFn) (ImpactSummary, error) {
	opt := applyOptFromOptFns(opts...)

	var rawTemplate ReqRawTemplate
	for _, t := range opt.Templates {
		b, err := t.Encode(EncodingJSON)
		if err != nil {
			return ImpactSummary{}, err
		}
		rawTemplate.Template = b
		rawTemplate.Sources = t.sources
		rawTemplate.ContentType = EncodingJSON.String()
	}

	reqBody := ReqApply{
		OrgID:       orgID.String(),
		DryRun:      dryRun,
		EnvRefs:     opt.EnvRefs,
		Secrets:     opt.MissingSecrets,
		RawTemplate: rawTemplate,
	}
	if opt.StackID != 0 {
		stackID := opt.StackID.String()
		reqBody.StackID = &stackID
	}

	for act := range opt.ResourcesToSkip {
		b, err := json.Marshal(act)
		if err != nil {
			return ImpactSummary{}, influxErr(errors.EInvalid, err)
		}
		reqBody.RawActions = append(reqBody.RawActions, ReqRawAction{
			Action:     string(ActionTypeSkipResource),
			Properties: b,
		})
	}
	for kind := range opt.KindsToSkip {
		b, err := json.Marshal(ActionSkipKind{Kind: kind})
		if err != nil {
			return ImpactSummary{}, influxErr(errors.EInvalid, err)
		}
		reqBody.RawActions = append(reqBody.RawActions, ReqRawAction{
			Action:     string(ActionTypeSkipKind),
			Properties: b,
		})
	}

	var resp RespApply
	err := s.Client.
		PostJSON(reqBody, RoutePrefixTemplates, "/apply").
		DecodeJSON(&resp).
		StatusFn(func(resp *http.Response) error {
			// valid response code when the template itself has parser errors.
			// we short circuit on that and allow that response to pass through
			// but consume the initial implementation if that does not hold.
			if resp.StatusCode == http.StatusUnprocessableEntity {
				return nil
			}
			return ihttp.CheckError(resp)
		}).
		Do(ctx)
	if err != nil {
		return ImpactSummary{}, err
	}

	impact := ImpactSummary{
		Sources: resp.Sources,
		Diff:    resp.Diff,
		Summary: resp.Summary,
	}

	if stackID, err := platform.IDFromString(resp.StackID); err == nil {
		impact.StackID = *stackID
	}

	return impact, NewParseError(resp.Errors...)
}

func convertRespStackToStack(respStack RespStack) (Stack, error) {
	newStack := Stack{
		CreatedAt: respStack.CreatedAt,
	}
	id, err := platform.IDFromString(respStack.ID)
	if err != nil {
		return Stack{}, err
	}
	newStack.ID = *id

	orgID, err := platform.IDFromString(respStack.OrgID)
	if err != nil {
		return Stack{}, err
	}
	newStack.OrgID = *orgID

	events := respStack.Events
	if len(events) == 0 && !respStack.UpdatedAt.IsZero() {
		events = append(events, respStack.RespStackEvent)
	}

	for _, respEv := range events {
		ev, err := convertRespStackEvent(respEv)
		if err != nil {
			return Stack{}, err
		}
		newStack.Events = append(newStack.Events, ev)
	}

	return newStack, nil
}

func convertRespStackEvent(ev RespStackEvent) (StackEvent, error) {
	res, err := convertRespStackResources(ev.Resources)
	if err != nil {
		return StackEvent{}, err
	}

	eventType := StackEventCreate
	switch ev.EventType {
	case "uninstall", "delete": // delete is included to maintain backwards compatibility
		eventType = StackEventUninstalled
	case "update":
		eventType = StackEventUpdate
	}

	return StackEvent{
		EventType:    eventType,
		Name:         ev.Name,
		Description:  ev.Description,
		Resources:    res,
		Sources:      ev.Sources,
		TemplateURLs: ev.URLs,
		UpdatedAt:    ev.UpdatedAt,
	}, nil
}

func convertRespStackResources(resources []RespStackResource) ([]StackResource, error) {
	out := make([]StackResource, 0, len(resources))
	for _, r := range resources {
		sr := StackResource{
			APIVersion: r.APIVersion,
			MetaName:   r.MetaName,
			Kind:       r.Kind,
		}
		for _, a := range r.Associations {
			sr.Associations = append(sr.Associations, StackResourceAssociation(a))
		}

		resID, err := platform.IDFromString(r.ID)
		if err != nil {
			return nil, influxErr(errors.EInternal, err)
		}
		sr.ID = *resID

		out = append(out, sr)
	}
	return out, nil
}
