import React, {PureComponent} from 'react'
import {ClickOutside} from 'src/shared/components/ClickOutside'

interface Props {
  text?: string
  confirmText?: string
  confirmAction: () => void
  type?: string
  size?: string
  square?: boolean
  icon?: string
  disabled?: boolean
  customClass?: string
}

interface State {
  expanded: boolean
}

class ConfirmButton extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    confirmText: 'Confirm',
    type: 'btn-default',
    size: 'btn-sm',
    square: false,
  }

  private buttonDiv: HTMLDivElement
  private tooltipDiv: HTMLDivElement

  constructor(props) {
    super(props)

    this.state = {
      expanded: false,
    }
  }

  public render() {
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
      <ClickOutside onClickOutside={this.handleClickOutside}>
        <div
          className={classname}
          onClick={this.handleButtonClick}
          ref={r => (this.buttonDiv = r)}
        >
          {icon && <span className={`icon ${icon}`} />}
          {text && text}
          <div
            className={`confirm-button--tooltip ${this.calculatePosition()}`}
          >
            <div
              className="confirm-button--confirmation"
              onClick={this.handleConfirmClick}
              ref={r => (this.tooltipDiv = r)}
            >
              {confirmText}
            </div>
          </div>
        </div>
      </ClickOutside>
    )
  }

  private handleButtonClick = () => {
    if (this.props.disabled) {
      return
    }
    this.setState({expanded: !this.state.expanded})
  }

  private handleConfirmClick = () => {
    this.setState({expanded: false})
    this.props.confirmAction()
  }

  private handleClickOutside = () => {
    this.setState({expanded: false})
  }

  private calculatePosition = () => {
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
}

export default ConfirmButton
