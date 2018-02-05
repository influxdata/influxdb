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

  handleClickOutside = () => {
    this.setState({expanded: false})
  }

  calculatePosition = () => {
    if (!this.buttonDiv || !this.tooltipDiv) {
      return ''
    }

    const windowWidth = window.innerWidth
    const buttonRect = this.buttonDiv.getBoundingClientRect()
    const tooltipRect = this.tooltipDiv.getBoundingClientRect()

    const rightGap = windowWidth - buttonRect.right

    if (tooltipRect.width / 2 > rightGap) {
      return 'left'
    }

    return 'bottom'
  }

  render() {
    const {
      text,
      confirmText,
      type,
      size,
      square,
      icon,
      disabled,
      customClass,
    } = this.props
    const {expanded} = this.state

    const customClassString = customClass ? ` ${customClass}` : ''
    const squareString = square ? ' btn-square' : ''
    const expandedString = expanded ? ' active' : ''
    const disabledString = disabled ? ' disabled' : ''

    const classname = `confirm-button btn ${type} ${size}${customClassString}${squareString}${expandedString}${disabledString}`

    return (
      <div
        className={classname}
        onClick={this.handleButtonClick}
        ref={r => (this.buttonDiv = r)}
      >
        {icon && <span className={`icon ${icon}`} />}
        {text && text}
        <div className={`confirm-button--tooltip ${this.calculatePosition()}`}>
          <div
            className="confirm-button--confirmation"
            onClick={this.handleConfirmClick}
            ref={r => (this.tooltipDiv = r)}
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
  square: false,
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
  customClass: string,
}

export default OnClickOutside(ConfirmButton)
