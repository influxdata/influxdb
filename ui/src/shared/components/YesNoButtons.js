import React, {PropTypes} from 'react'
import classnames from 'classnames'

const YesNoButtons = ({onConfirm, onCancel, buttonSize}) =>
  <div>
    <button
      className={classnames('btn btn-square btn-info', {
        [buttonSize]: buttonSize,
      })}
      onClick={onCancel}
    >
      <span className="icon remove" />
    </button>
    <button
      className={classnames('btn btn-square btn-success', {
        [buttonSize]: buttonSize,
      })}
      onClick={onConfirm}
    >
      <span className="icon checkmark" />
    </button>
  </div>

const {func, string} = PropTypes

YesNoButtons.propTypes = {
  onConfirm: func.isRequired,
  onCancel: func.isRequired,
  buttonSize: string,
}
YesNoButtons.defaultProps = {
  buttonSize: 'btn-sm',
}

export default YesNoButtons
