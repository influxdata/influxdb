import React, {PropTypes} from 'react'

import TemplateVariableRow from 'src/dashboards/components/template_variables/Row'

const TemplateVariableTable = ({
  source,
  templates,
  onRunQuerySuccess,
  onRunQueryFailure,
  onDelete,
  tempVarAlreadyExists,
}) =>
  <div className="template-variable-manager--table">
    {templates.length
      ? <div className="template-variable-manager--table-container">
          <div className="template-variable-manager--table-heading">
            <div className="tvm--col-1">Variable</div>
            <div className="tvm--col-2">Type</div>
            <div className="tvm--col-3">Definition / Values</div>
            <div className="tvm--col-4" />
          </div>
          <div className="template-variable-manager--table-rows">
            {templates.map(t =>
              <TemplateVariableRow
                key={t.id}
                source={source}
                template={t}
                onRunQuerySuccess={onRunQuerySuccess}
                onRunQueryFailure={onRunQueryFailure}
                onDelete={onDelete}
                tempVarAlreadyExists={tempVarAlreadyExists}
              />
            )}
          </div>
        </div>
      : <div className="generic-empty-state">
          <h4 style={{margin: '60px 0'}} className="no-user-select">
            You have no Template Variables, why not create one?
          </h4>
        </div>}
  </div>

const {arrayOf, bool, func, shape, string} = PropTypes

TemplateVariableTable.propTypes = {
  source: shape({
    links: shape({
      proxy: string,
    }),
  }).isRequired,
  templates: arrayOf(
    shape({
      type: string.isRequired,
      tempVar: string.isRequired,
      query: shape({
        db: string,
        influxql: string,
        measurement: string,
        tagKey: string,
      }),
      values: arrayOf(
        shape({
          value: string.isRequired,
          type: string.isRequired,
          selected: bool.isRequired,
        })
      ).isRequired,
    })
  ),
  onRunQuerySuccess: func.isRequired,
  onRunQueryFailure: func.isRequired,
  onDelete: func.isRequired,
  tempVarAlreadyExists: func.isRequired,
}

export default TemplateVariableTable
