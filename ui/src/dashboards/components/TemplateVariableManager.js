import React, {PropTypes} from 'react'
import OnClickOutside from 'react-onclickoutside'
import TemplateVariableTable
  from 'src/dashboards/components/TemplateVariableTable'

const TemplateVariableManager = ({
  onClose,
  onEditTemplateVariable,
  templates,
}) => (
  <div className="template-variable-manager">
    <div className="template-variable-manager--header">
      <div className="page-header__left">
        Template Variables
      </div>
      <div className="page-header__right">
        <button className="btn btn-primary btn-sm">Add Variable</button>
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
        onEditTemplateVariable={onEditTemplateVariable}
      />
    </div>
  </div>
)

const {arrayOf, bool, func, shape, string} = PropTypes

TemplateVariableManager.propTypes = {
  onClose: func.isRequired,
  onEditTemplateVariable: func.isRequired,
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
