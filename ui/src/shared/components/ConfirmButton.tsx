import React, {PureComponent} from 'react'
import {ClickOutside} from 'src/shared/components/ClickOutside'
import {ErrorHandling} from 'src/shared/decorators/errors'

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

@ErrorHandling
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
    const {text, confirmText, icon} = this.props

    return (
      <ClickOutside onClickOutside={this.handleClickOutside}>
        <div
          className={this.className}
          onClick={this.handleButtonClick}
          ref={r => (this.buttonDiv = r)}
        >
          {icon && <span className={`icon ${icon}`} />}
          {text && text}
          <div className={`confirm-button--tooltip ${this.calculatedPosition}`}>
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

  private get className() {
    const {type, size, square, disabled, customClass} = this.props
    const {expanded} = this.state

    const customClassString = customClass ? ` ${customClass}` : ''
    const squareString = square ? ' btn-square' : ''
    const expandedString = expanded ? ' active' : ''
    const disabledString = disabled ? ' disabled' : ''

    return `confirm-button btn ${type} ${size}${customClassString}${squareString}${expandedString}${disabledString}`
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

  private get calculatedPosition() {
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
