package pkger

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/pkg/httpc"
)

// HTTPRemoteService provides an http client that is fluent in all things pkger.
type HTTPRemoteService struct {
	Client *httpc.Client
}

var _ SVC = (*HTTPRemoteService)(nil)

func (s *HTTPRemoteService) InitStack(ctx context.Context, userID influxdb.ID, stack Stack) (Stack, error) {
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

func (s *HTTPRemoteService) DeleteStack(ctx context.Context, identifiers struct{ OrgID, UserID, StackID influxdb.ID }) error {
	return s.Client.
		Delete(RoutePrefixStacks, identifiers.StackID.String()).
		QueryParams([2]string{"orgID", identifiers.OrgID.String()}).
		Do(ctx)
}

func (s *HTTPRemoteService) ListStacks(ctx context.Context, orgID influxdb.ID, f ListFilter) ([]Stack, error) {
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

func (s *HTTPRemoteService) ReadStack(ctx context.Context, id influxdb.ID) (Stack, error) {
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

// Export will produce a pkg from the parameters provided.
func (s *HTTPRemoteService) Export(ctx context.Context, opts ...ExportOptFn) (*Pkg, error) {
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

	var newPkg *Pkg
	err = s.Client.
		PostJSON(reqBody, RoutePrefixTemplates, "/export").
		Decode(func(resp *http.Response) error {
			pkg, err := Parse(EncodingJSON, FromReader(resp.Body, "export"))
			newPkg = pkg
			return err
		}).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	if err := newPkg.Validate(ValidWithoutResources()); err != nil {
		return nil, err
	}
	return newPkg, nil
}

// DryRun provides a dry run of the pkg application. The pkg will be marked verified
// for later calls to Apply. This func will be run on an Apply if it has not been run
// already.
func (s *HTTPRemoteService) DryRun(ctx context.Context, orgID, userID influxdb.ID, opts ...ApplyOptFn) (ImpactSummary, error) {
	return s.apply(ctx, orgID, true, opts...)
}

// Apply will apply all the resources identified in the provided pkg. The entire pkg will be applied
// in its entirety. If a failure happens midway then the entire pkg will be rolled back to the state
// from before the pkg was applied.
func (s *HTTPRemoteService) Apply(ctx context.Context, orgID, userID influxdb.ID, opts ...ApplyOptFn) (ImpactSummary, error) {
	return s.apply(ctx, orgID, false, opts...)
}

func (s *HTTPRemoteService) apply(ctx context.Context, orgID influxdb.ID, dryRun bool, opts ...ApplyOptFn) (ImpactSummary, error) {
	opt := applyOptFromOptFns(opts...)

	var rawPkg ReqRawTemplate
	for _, pkg := range opt.Pkgs {
		b, err := pkg.Encode(EncodingJSON)
		if err != nil {
			return ImpactSummary{}, err
		}
		rawPkg.Pkg = b
		rawPkg.Sources = pkg.sources
		rawPkg.ContentType = EncodingJSON.String()
	}

	reqBody := ReqApply{
		OrgID:       orgID.String(),
		DryRun:      dryRun,
		EnvRefs:     opt.EnvRefs,
		Secrets:     opt.MissingSecrets,
		RawTemplate: rawPkg,
	}
	if opt.StackID != 0 {
		stackID := opt.StackID.String()
		reqBody.StackID = &stackID
	}

	for act := range opt.ResourcesToSkip {
		b, err := json.Marshal(act)
		if err != nil {
			return ImpactSummary{}, influxErr(influxdb.EInvalid, err)
		}
		reqBody.RawActions = append(reqBody.RawActions, ReqRawAction{
			Action:     string(ActionTypeSkipResource),
			Properties: b,
		})
	}
	for kind := range opt.KindsToSkip {
		b, err := json.Marshal(ActionSkipKind{Kind: kind})
		if err != nil {
			return ImpactSummary{}, influxErr(influxdb.EInvalid, err)
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
		Do(ctx)
	if err != nil {
		return ImpactSummary{}, err
	}

	impact := ImpactSummary{
		Sources: resp.Sources,
		Diff:    resp.Diff,
		Summary: resp.Summary,
	}

	if stackID, err := influxdb.IDFromString(resp.StackID); err == nil {
		impact.StackID = *stackID
	}

	return impact, NewParseError(resp.Errors...)
}

func convertRespStackToStack(respStack RespStack) (Stack, error) {
	newStack := Stack{
		Name:         respStack.Name,
		Description:  respStack.Description,
		Sources:      respStack.Sources,
		TemplateURLs: respStack.URLs,
		Resources:    make([]StackResource, 0, len(respStack.Resources)),
		CRUDLog:      respStack.CRUDLog,
	}
	for _, r := range respStack.Resources {
		sr := StackResource{
			APIVersion: r.APIVersion,
			MetaName:   r.MetaName,
			Kind:       r.Kind,
		}
		for _, a := range r.Associations {
			sra := StackResourceAssociation{
				Kind:     a.Kind,
				MetaName: a.MetaName,
			}
			if sra.MetaName == "" && a.PkgName != nil {
				sra.MetaName = *a.PkgName
			}
			sr.Associations = append(sr.Associations, sra)
		}

		resID, err := influxdb.IDFromString(r.ID)
		if err != nil {
			return Stack{}, influxErr(influxdb.EInternal, err)
		}
		sr.ID = *resID

		if sr.MetaName == "" && r.PkgName != nil {
			sr.MetaName = *r.PkgName
		}
		newStack.Resources = append(newStack.Resources, sr)
	}

	id, err := influxdb.IDFromString(respStack.ID)
	if err != nil {
		return Stack{}, err
	}
	newStack.ID = *id

	orgID, err := influxdb.IDFromString(respStack.OrgID)
	if err != nil {
		return Stack{}, err
	}
	newStack.OrgID = *orgID

	return newStack, nil
}
