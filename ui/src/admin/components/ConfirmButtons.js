import React, {PropTypes} from 'react'

const ConfirmButtons = ({onConfirm, item, onCancel}) => (
  <div>
    <button
      className="btn btn-xs btn-primary"
      onClick={() => onConfirm(item)}
    >
      <span className="icon checkmark"></span>
    </button>
    <button
      className="btn btn-xs btn-default"
      onClick={() => onCancel(item)}
    >
      <span className="icon remove"></span>
    </button>
  </div>
)

const {
  func,
  shape,
} = PropTypes

ConfirmButtons.propTypes = {
  onConfirm: func.isRequired,
  item: shape({}).isRequired,
  onCancel: func.isRequired,
}

export default ConfirmButtons
