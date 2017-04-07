import React, {PropTypes} from 'react'

const ConfirmButtons = ({onConfirm, item, onCancel}) => (
  <div className="confirm-buttons">
    <button
      className="btn btn-xs btn-info"
      onClick={() => onCancel(item)}
    >
      <span className="icon remove"></span>
    </button>
    <button
      className="btn btn-xs btn-success"
      onClick={() => onConfirm(item)}
    >
      <span className="icon checkmark"></span>
    </button>
  </div>
)

const {
  func,
  shape,
} = PropTypes

ConfirmButtons.propTypes = {
  onConfirm: func.isRequired,
  item: shape({}),
  onCancel: func.isRequired,
}

export default ConfirmButtons
