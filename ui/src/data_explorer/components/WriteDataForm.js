import React, {PropTypes} from 'react'

const WriteDataForm = ({onClose}) => (
  <div>
    <a onClick={onClose}>Click Me</a>
  </div>
)

const {func} = PropTypes

WriteDataForm.propTypes = {
  onClose: func.isRequired,
}

export default WriteDataForm
