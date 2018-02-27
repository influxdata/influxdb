import React, {PropTypes, Component} from 'react'
import classnames from 'classnames'

import OnClickOutside from 'shared/components/OnClickOutside'

class ConfirmButtons extends Component {
  constructor(props) {
    super(props)
  }

  handleConfirm = item => () => {
    this.props.onConfirm(item)
  }

  handleCancel = item => () => {
    this.props.onCancel(item)
  }

  handleClickOutside = () => {
    this.props.onClickOutside(this.props.item)
  }

  render() {
    const {
      item,
      buttonSize,
      isDisabled,
      confirmLeft,
      confirmHoverText,
    } = this.props
    const hoverText = confirmHoverTextText || 'Save'

    return confirmLeft
      ? <div className="confirm-buttons">
          <button
            className={classnames('btn btn-success btn-square', {
              [buttonSize]: buttonSize,
            })}
            disabled={isDisabled}
            title={isDisabled ? `Cannot ${hoverText}` : hoverText}
            onClick={this.handleConfirm(item)}
          >
            <span className="icon checkmark" />
          </button>
          <button
            className={classnames('btn btn-info btn-square', {
              [buttonSize]: buttonSize,
            })}
            onClick={this.handleCancel(item)}
          >
            <span className="icon remove" />
          </button>
        </div>
      : <div className="confirm-buttons">
          <button
            className={classnames('btn btn-info btn-square', {
              [buttonSize]: buttonSize,
            })}
            onClick={this.handleCancel(item)}
          >
            <span className="icon remove" />
          </button>
          <button
            className={classnames('btn btn-success btn-square', {
              [buttonSize]: buttonSize,
            })}
            disabled={isDisabled}
            title={isDisabled ? `Cannot ${hoverText}` : hoverText}
            onClick={this.handleConfirm(item)}
          >
            <span className="icon checkmark" />
          </button>
        </div>
  }
}

const {func, oneOfType, shape, string, bool} = PropTypes

ConfirmButtons.propTypes = {
  onConfirm: func.isRequired,
  item: oneOfType([shape(), string]),
  onCancel: func.isRequired,
  buttonSize: string,
  isDisabled: bool,
  onClickOutside: func,
  confirmLeft: bool,
  confirmHoverText: string,
}

ConfirmButtons.defaultProps = {
  buttonSize: 'btn-sm',
  onClickOutside: () => {},
}
export default OnClickOutside(ConfirmButtons)
