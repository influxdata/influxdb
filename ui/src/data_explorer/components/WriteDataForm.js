import React, {PropTypes} from 'react'
import classnames from 'classnames'

const WriteDataForm = ({onClose}) => (
  <div className="template-variable-manager">
    <div className="template-variable-manager--header">
      <div className="page-header__left">
        <h1 className="page-header__title">Write Data To</h1>
      </div>
      <div className="page-header__right">
        <button className="btn btn-primary btn-sm" type="button">
          Add Variable
        </button>
        <button
          className={classnames('btn btn-success btn-sm', {
            disabled: !true,
          })}
          type="button"
        >
          Save Changes
        </button>
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
