import React, {PropTypes} from 'react'
import classnames from 'classnames'

const ConfirmButtons = ({onConfirm, item, onCancel, buttonSize, isDisabled}) =>
  <div className="confirm-buttons">
    <button
      className={classnames('btn btn-info btn-square', {
        [buttonSize]: buttonSize,
      })}
      onClick={() => onCancel(item)}
    >
      <span className="icon remove" />
    </button>
    <button
      className={classnames('btn btn-success btn-square', {
        [buttonSize]: buttonSize,
      })}
      disabled={isDisabled}
      title={isDisabled ? 'Cannot Save' : 'Save'}
      onClick={() => onConfirm(item)}
    >
      <span className="icon checkmark" />
    </button>
  </div>

const {func, oneOfType, shape, string, bool} = PropTypes

ConfirmButtons.propTypes = {
  onConfirm: func.isRequired,
  item: oneOfType([shape(), string]),
  onCancel: func.isRequired,
  buttonSize: string,
  isDisabled: bool,
}

ConfirmButtons.defaultProps = {
  buttonSize: 'btn-sm',
}
export default ConfirmButtons
