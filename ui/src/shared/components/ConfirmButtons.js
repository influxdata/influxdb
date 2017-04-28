import React, {PropTypes} from 'react'

const ConfirmButtons = ({onConfirm, item, onCancel}) => (
  <div className="confirm-buttons">
    <button className="btn btn-xs btn-info" onClick={() => onCancel(item)}>
      <span className="icon remove" />
    </button>
    <button className="btn btn-xs btn-success" onClick={() => onConfirm(item)}>
      <span className="icon checkmark" />
    </button>
  </div>
)

const {func, oneOfType, shape, string} = PropTypes

ConfirmButtons.propTypes = {
  onConfirm: func.isRequired,
  item: oneOfType([shape(), string]),
  onCancel: func.isRequired,
}

export default ConfirmButtons
