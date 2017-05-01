import React, {PropTypes} from 'react'

const YesNoButtons = ({onConfirm, onCancel}) => (
  <div>
    <button className="btn btn-xs btn-info" onClick={onCancel}>
      <span className="icon remove" />
    </button>
    <button className="btn btn-xs btn-success" onClick={onConfirm}>
      <span className="icon checkmark" />
    </button>
  </div>
)

const {func} = PropTypes

YesNoButtons.propTypes = {
  onConfirm: func.isRequired,
  onCancel: func.isRequired,
}

export default YesNoButtons
