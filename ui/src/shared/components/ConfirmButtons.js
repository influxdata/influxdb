import React, {PropTypes, Component} from 'react'
import classnames from 'classnames'

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

  render() {
    const {item, buttonSize, isDisabled} = this.props

    return (
      <div className="confirm-buttons">
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
          title={isDisabled ? 'Cannot Save' : 'Save'}
          onClick={this.handleConfirm(item)}
        >
          <span className="icon checkmark" />
        </button>
      </div>
    )
  }
}

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
