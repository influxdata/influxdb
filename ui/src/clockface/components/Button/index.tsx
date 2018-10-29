// Libraries
import React, {Component, MouseEvent} from 'react'
import classnames from 'classnames'

// Types
import {
  ComponentStatus,
  ComponentColor,
  ComponentSize,
  ButtonShape,
  IconFont,
  ButtonType,
} from 'src/clockface/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  text?: string
  onClick?: (e?: MouseEvent<HTMLButtonElement>) => void
  color?: ComponentColor
  size?: ComponentSize
  shape?: ButtonShape
  icon?: IconFont
  status?: ComponentStatus
  titleText?: string
  active?: boolean
  tabIndex?: number
  customClass?: string
  type?: ButtonType
}

@ErrorHandling
class Button extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    color: ComponentColor.Default,
    size: ComponentSize.Small,
    shape: ButtonShape.Default,
    status: ComponentStatus.Default,
    active: false,
    type: ButtonType.Submit,
  }

  public render() {
    const {onClick, text, titleText, tabIndex, type} = this.props

    return (
      <button
        className={this.className}
        disabled={this.disabled}
        onClick={onClick}
        title={titleText || text}
        tabIndex={!!tabIndex ? tabIndex : 0}
        type={type}
      >
        {this.icon}
        {this.text}
        {this.statusIndicator}
      </button>
    )
  }

  private get icon(): JSX.Element {
    const {icon} = this.props

    if (icon) {
      return <span className={`button-icon icon ${icon}`} />
    }

    return null
  }

  private get text(): string {
    const {text, shape} = this.props

    if (shape === ButtonShape.Square) {
      return null
    }

    return text
  }

  private get disabled(): boolean {
    const {status} = this.props

    return (
      status === ComponentStatus.Disabled || status === ComponentStatus.Loading
    )
  }

  private get statusIndicator(): JSX.Element {
    const {status, size} = this.props

    if (status === ComponentStatus.Loading) {
      return <div className={`button-spinner button-spinner--${size}`} />
    }

    return null
  }

  private get className(): string {
    const {color, size, shape, status, active, customClass} = this.props

    return classnames(`button button-${size} button-${color}`, {
      'button-square': shape === ButtonShape.Square,
      'button-stretch': shape === ButtonShape.StretchToFit,
      'button--loading': status === ComponentStatus.Loading,
      'button--disabled': status === ComponentStatus.Disabled,
      active,
      [customClass]: customClass,
    })
  }
}

export default Button
