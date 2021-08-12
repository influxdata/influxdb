package check

import (
	"encoding/json"
	"time"

	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/ast/astutil"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/notification/flux"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
)

var _ influxdb.Check = &Custom{}

// Custom is the custom check.
type Custom struct {
	ID          platform.ID             `json:"id,omitempty"`
	Name        string                  `json:"name"`
	Description string                  `json:"description,omitempty"`
	OwnerID     platform.ID             `json:"ownerID,omitempty"`
	OrgID       platform.ID             `json:"orgID,omitempty"`
	Query       influxdb.DashboardQuery `json:"query"`
	TaskID      platform.ID             `json:"taskID,omitempty"`
	CreatedAt   time.Time               `json:"createdAt"`
	UpdatedAt   time.Time               `json:"updatedAt"`
}

// flux example for threshold check for reference:

// package main
// import "influxdata/influxdb/monitor"
// import "influxdata/influxdb/v1"

// data = from(bucket: "_tasks")
// 	|> range(start: -1m)
// 	|> filter(fn: (r) => r._measurement == "runs")
// 	|> filter(fn: (r) => r._field == "finishedAt")
// 	|> aggregateWindow(every: 1m fn: mean, createEmpty: false)

// option task = {
// 	name: "Name this Check",
// 	every: 1m,
// 	offset: 0s
// }

// check = {
// 	_check_id: "undefined",
// 	_check_name: "Name this Check",
// 	_type: "custom",
// 	tags: {a: "b",c: "d"}
// }

// warn = (r) =>(r.finishedAt> 20)
// crit = (r) =>(r.finishedAt> 20)
// info = (r) =>(r.finishedAt> 20)

// messageFn = (r) =>("Check: ${ r._check_name } is: ${ r._level }")

// data
// 	|> v1.fieldsAsCols()
// 	|> monitor.check(data: check, messageFn:messageFn, warn:warn, crit:crit, info:info)

// GenerateFlux returns the check query text directly
func (c Custom) GenerateFlux(lang fluxlang.FluxLanguageService) (string, error) {
	return c.Query.Text, nil
}

// sanitizeFlux modifies the check query text to include correct _check_id param in check object
func (c Custom) sanitizeFlux(lang fluxlang.FluxLanguageService) (string, error) {
	p, err := query.Parse(lang, c.Query.Text)
	if p == nil {
		return "", err
	} else if errs := ast.GetErrors(p); len(errs) != 0 {
		return "", multiError(errs)
	}

	ast.Visit(p, func(n ast.Node) {
		if variableAssign, ok := n.(*ast.VariableAssignment); ok && variableAssign.ID.Name == "check" {
			if objectExp, ok := variableAssign.Init.(*ast.ObjectExpression); ok {
				idx := -1
				for i, prop := range objectExp.Properties {
					if prop.Key.Key() == "_check_id" {
						idx = i
						break
					}
				}

				idProp := flux.Property("_check_id", flux.String(c.ID.String()))
				if idx >= 0 {
					objectExp.Properties[idx] = idProp
				} else {
					objectExp.Properties = append(objectExp.Properties, idProp)
				}
			}
		}
	})

	return astutil.Format(p.Files[0])
}

func propertyHasValue(prop *ast.Property, key string, value string) bool {
	stringLit, ok := prop.Value.(*ast.StringLiteral)
	return ok && prop.Key.Key() == key && stringLit.Value == value
}

func (c *Custom) hasRequiredTaskOptions(lang fluxlang.FluxLanguageService) (err error) {

	p, err := query.Parse(lang, c.Query.Text)
	if p == nil {
		return err
	}

	hasOptionTask := false
	hasName := false
	nameMatchesCheck := false
	hasEvery := false
	hasOffset := false

	ast.Visit(p, func(n ast.Node) {
		if option, ok := n.(*ast.OptionStatement); ok {
			if variableAssign, ok := option.Assignment.(*ast.VariableAssignment); ok && variableAssign.ID.Name == "task" {
				hasOptionTask = true
				if objectExp, ok := variableAssign.Init.(*ast.ObjectExpression); ok {
					for _, prop := range objectExp.Properties {
						if prop.Key.Key() == "name" {
							hasName = true
							if propertyHasValue(prop, "name", c.Name) {
								nameMatchesCheck = true
							}
						}
						if prop.Key.Key() == "every" {
							hasEvery = true
						}
						if prop.Key.Key() == "offset" {
							hasOffset = true
						}
					}
				}
			}
		}
	})
	if !hasOptionTask {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Custom flux missing task option statement",
		}
	}
	if !hasName {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Custom flux missing name parameter from task option statement",
		}
	}
	if hasName && !nameMatchesCheck {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Name parameter from task option statement must match check name",
		}
	}
	if !hasEvery {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Custom flux missing every parameter from task option statement",
		}
	}
	if !hasOffset {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Custom flux missing offset parameter from task option statement",
		}
	}
	return nil
}

func (c *Custom) hasRequiredCheckParameters(lang fluxlang.FluxLanguageService) (err error) {
	p, err := query.Parse(lang, c.Query.Text)
	if p == nil {
		return err
	}

	hasCheckObject := false
	checkNameMatches := false
	checkTypeIsCustom := false

	ast.Visit(p, func(n ast.Node) {
		if variableAssign, ok := n.(*ast.VariableAssignment); ok && variableAssign.ID.Name == "check" {
			hasCheckObject = true
			if objectExp, ok := variableAssign.Init.(*ast.ObjectExpression); ok {
				for _, prop := range objectExp.Properties {
					if propertyHasValue(prop, "_check_name", c.Name) {
						checkNameMatches = true
					}
					if propertyHasValue(prop, "_type", "custom") {
						checkTypeIsCustom = true
					}
				}
			}
		}
	})

	if !hasCheckObject {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "Custom flux must have an object called 'check'",
		}
	}
	if !checkNameMatches {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "_check_name parameter on check object must match check name",
		}
	}
	if !checkTypeIsCustom {
		return &errors.Error{
			Code: errors.EInvalid,
			Msg:  "_type parameter on check object must be set to 'custom'",
		}
	}
	return nil
}

// Valid checks whether check flux is valid, returns error if invalid
func (c *Custom) Valid(lang fluxlang.FluxLanguageService) error {

	if err := c.hasRequiredCheckParameters(lang); err != nil {
		return err
	}

	if err := c.hasRequiredTaskOptions(lang); err != nil {
		return err
	}

	// add or replace _check_id parameter on the check object
	script, err := c.sanitizeFlux(lang)
	if err != nil {
		return err
	}

	c.Query.Text = script

	return nil
}

type customAlias Custom

// MarshalJSON implement json.Marshaler interface.
func (c Custom) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			customAlias
			Type string `json:"type"`
		}{
			customAlias: customAlias(c),
			Type:        c.Type(),
		})
}

// Type returns the type of the check.
func (c Custom) Type() string {
	return "custom"
}

// ClearPrivateData remove any data that we don't want to be exposed publicly.
func (c *Custom) ClearPrivateData() {
	c.TaskID = 0
}

// SetTaskID sets the taskID for a check.
func (c *Custom) SetTaskID(id platform.ID) {
	c.TaskID = id
}

// GetTaskID retrieves the task ID for a check.
func (c *Custom) GetTaskID() platform.ID {
	return c.TaskID
}

// GetOwnerID gets the ownerID associated with a Check.
func (c *Custom) GetOwnerID() platform.ID {
	return c.OwnerID
}

// SetOwnerID sets the taskID for a check.
func (c *Custom) SetOwnerID(id platform.ID) {
	c.OwnerID = id
}

// SetCreatedAt sets the creation time for a check
func (c *Custom) SetCreatedAt(now time.Time) {
	c.CreatedAt = now
}

// SetUpdatedAt sets the update time for a check
func (c *Custom) SetUpdatedAt(now time.Time) {
	c.UpdatedAt = now
}

// SetID sets the primary key for a check
func (c *Custom) SetID(id platform.ID) {
	c.ID = id
}

// SetOrgID is SetOrgID
func (c *Custom) SetOrgID(id platform.ID) {
	c.OrgID = id
}

// SetName implements influxdb.Updator interface
func (c *Custom) SetName(name string) {
	c.Name = name
}

// SetDescription is SetDescription
func (c *Custom) SetDescription(description string) {
	c.Description = description
}

// GetID is GetID
func (c *Custom) GetID() platform.ID {
	return c.ID
}

// GetCRUDLog gets crudLog
func (c *Custom) GetCRUDLog() influxdb.CRUDLog {
	return influxdb.CRUDLog{CreatedAt: c.CreatedAt, UpdatedAt: c.UpdatedAt}
}

// GetOrgID gets the orgID associated with the Check
func (c *Custom) GetOrgID() platform.ID {
	return c.OrgID
}

// GetName implements influxdb.Getter interface.
func (c *Custom) GetName() string {
	return c.Name
}

// GetDescription is GetDescription
func (c *Custom) GetDescription() string {
	return c.Description
}
