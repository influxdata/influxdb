import React, {PureComponent} from 'react'
import classnames from 'classnames'
import {ClickOutside} from 'src/shared/components/ClickOutside'
import {ErrorHandling} from 'src/shared/decorators/errors'

type Position = 'top' | 'bottom' | 'left' | 'right'

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
  position?: Position
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
          title={confirmText}
        >
          {icon && <span className={`icon ${icon}`} />}
          {text && text}
          <div className={this.tooltipClassName}>
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

  private get className(): string {
    const {type, size, square, disabled, customClass} = this.props
    const {expanded} = this.state

    return classnames(`confirm-button btn ${type} ${size}`, {
      [customClass]: customClass,
      'btn-square': square,
      active: expanded,
      disabled,
    })
  }

  private get tooltipClassName(): string {
    const {position} = this.props

    if (position) {
      return `confirm-button--tooltip ${position}`
    }

    if (!this.buttonDiv || !this.tooltipDiv) {
      return 'confirm-button--tooltip bottom'
    }

    const windowWidth = window.innerWidth
    const buttonRect = this.buttonDiv.getBoundingClientRect()
    const tooltipRect = this.tooltipDiv.getBoundingClientRect()

    const rightGap = windowWidth - buttonRect.right

    if (tooltipRect.width / 2 > rightGap) {
      return 'confirm-button--tooltip left'
    }

    return 'confirm-button--tooltip bottom'
  }
}

export default ConfirmButton
