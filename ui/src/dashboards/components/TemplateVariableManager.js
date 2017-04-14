import React, {PropTypes} from 'react'
import OnClickOutside from 'react-onclickoutside'

const TemplateVariableManager = ({onClose}) => (
  <div className="template-variable-manager">
    <div className="template-variable-manager--header">
      <div className="page-header__left">
        Template Variables
      </div>
      <div className="page-header__right">
        <button className="btn btn-primary btn-sm">New Variable</button>
        <button className="btn btn-success btn-sm">Save Changes</button>
        <span className="icon remove" onClick={onClose}></span>
      </div>
    </div>
    <div className="template-variable-manager--body"></div>
  </div>
)

const {
  func,
} = PropTypes

TemplateVariableManager.propTypes = {
  onClose: func.isRequired,
}

export default OnClickOutside(TemplateVariableManager)
