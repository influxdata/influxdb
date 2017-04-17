import React, {PropTypes} from 'react'
import OnClickOutside from 'react-onclickoutside'
import TemplateVariableTable
  from 'src/dashboards/components/TemplateVariableTable'

const TemplateVariableManager = ({onClose, templates}) => (
  <div className="template-variable-manager">
    <div className="template-variable-manager--header">
      <div className="page-header__left">
        Template Variables
      </div>
      <div className="page-header__right">
        <button className="btn btn-primary btn-sm">New Variable</button>
        <button className="btn btn-success btn-sm">Save Changes</button>
        <span
          className="icon remove"
          onClick={onClose}
          style={{cursor: 'pointer'}}
        />
      </div>
    </div>
    <div className="template-variable-manager--body">
      <TemplateVariableTable templates={templates} />
    </div>
  </div>
)

const {arrayOf, func, shape, string} = PropTypes

TemplateVariableManager.propTypes = {
  onClose: func.isRequired,
  templates: arrayOf(
    shape({
      type: string.isRequired,
      label: string.isRequired,
      code: string.isRequired,
      query: shape({
        db: string.isRequired,
        text: string.isRequired,
      }),
      values: arrayOf(string.isRequired),
    })
  ),
}

export default OnClickOutside(TemplateVariableManager)
