import React, {PropTypes} from 'react'
import OnClickOutside from 'react-onclickoutside'
import TemplateVariableTable
  from 'src/dashboards/components/TemplateVariableTable'

const TemplateVariableManager = ({
  onClose,
  onEditTemplateVariables,
  onRunTemplateVariableQuery,
  templates,
}) => (
  <div className="template-variable-manager">
    <div className="template-variable-manager--header">
      <div className="page-header__left">
        Template Variables
      </div>
      <div className="page-header__right">
        <button className="btn btn-primary btn-sm">Add Variable</button>
        <button
          className="btn btn-primary btn-sm"
          onClick={onEditTemplateVariables}
        >Save Template
        </button>
        <span
          className="icon remove"
          onClick={onClose}
          style={{cursor: 'pointer'}}
        />
      </div>
    </div>
    <div className="template-variable-manager--body">
      <TemplateVariableTable
        templates={templates}
        onRunTemplateVariableQuery={onRunTemplateVariableQuery}
      />
    </div>
  </div>
)

const {arrayOf, bool, func, shape, string} = PropTypes

TemplateVariableManager.propTypes = {
  onClose: func.isRequired,
  onEditTemplateVariables: func.isRequired,
  onRunTemplateVariableQuery: func.isRequired,
  templates: arrayOf(
    shape({
      type: string.isRequired,
      label: string.isRequired,
      tempVar: string.isRequired,
      query: shape({
        db: string.isRequired,
        influxql: string.isRequired,
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
}

export default OnClickOutside(TemplateVariableManager)
