import React, {Component, PropTypes} from 'react'

import OnClickOutside from 'shared/components/OnClickOutside'

class ConfirmButton extends Component {
  constructor(props) {
    super(props)

    this.state = {
      expanded: false,
    }
  }

  handleButtonClick = () => {
    if (this.props.disabled) {
      return
    }
    this.setState({expanded: !this.state.expanded})
  }

  handleConfirmClick = () => {
    this.setState({expanded: false})
    this.props.confirmAction()
  }

  handleClickOutside() {
    this.setState({expanded: false})
  }

  render() {
    const {text, confirmText, type, size, square, icon, disabled} = this.props
    const {expanded} = this.state

    const classname = `confirm-button btn ${type} ${size}${square
      ? ' btn-square'
      : ''}${disabled ? ' disabled' : ''}${expanded ? ' expanded' : ''}`

    return (
      <div className={classname} onClick={this.handleButtonClick}>
        {icon && <span className={`icon ${icon}`} />}
        {text && text}
        <div className="confirm-button--tooltip">
          <div
            className="confirm-button--confirmation"
            onClick={this.handleConfirmClick}
          >
            {confirmText}
          </div>
        </div>
      </div>
    )
  }
}

const {bool, func, string} = PropTypes

ConfirmButton.defaultProps = {
  confirmText: 'Confirm',
  type: 'btn-default',
  size: 'btn-sm',
  square: true,
}
ConfirmButton.propTypes = {
  text: string,
  confirmText: string,
  confirmAction: func.isRequired,
  type: string,
  size: string,
  square: bool,
  icon: string,
  disabled: bool,
}

export default OnClickOutside(ConfirmButton)
