import React, {PropTypes} from 'react'

import Dropdown from 'shared/components/Dropdown'

const WriteDataForm = ({onClose}) => (
  <div className="template-variable-manager">
    <div className="template-variable-manager--header">
      <div className="page-header__left">
        <h1 className="page-header__title">Write Data To</h1>
        <button className="btn btn-default btn-sm">Name</button>
      </div>
      <div className="page-header__right">
        <span className="page-header__dismiss" onClick={onClose} />
      </div>
    </div>
    <div className="template-variable-manager--body" />
  </div>
)

const {func} = PropTypes

WriteDataForm.propTypes = {
  onClose: func.isRequired,
}

export default WriteDataForm
