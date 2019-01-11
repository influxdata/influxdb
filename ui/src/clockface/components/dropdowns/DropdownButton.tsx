// Libraries
import React, {Component, MouseEvent} from 'react'
import classnames from 'classnames'

// Types
import {
  ComponentStatus,
  ComponentColor,
  ComponentSize,
  IconFont,
  DropdownChild,
} from 'src/clockface/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: DropdownChild
  onClick: (e: MouseEvent<HTMLElement>) => void
  status?: ComponentStatus
  active?: boolean
  color?: ComponentColor
  size?: ComponentSize
  icon?: IconFont
  title?: string
}

@ErrorHandling
class DropdownButton extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    color: ComponentColor.Default,
    size: ComponentSize.Small,
    status: ComponentStatus.Default,
    active: false,
  }

  public render() {
    const {onClick, children, title} = this.props
    return (
      <button
        className={this.classname}
        onClick={onClick}
        disabled={this.isDisabled}
        title={title}
      >
        {this.icon}
        <span className="dropdown--selected">{children}</span>
        {this.caret}
      </button>
    )
  }

  private get caret(): JSX.Element {
    const {active} = this.props

    if (active) {
      return <span className="dropdown--caret icon caret-up" />
    }

    return <span className="dropdown--caret icon caret-down" />
  }

  private get isDisabled(): boolean {
    const {status} = this.props

    const isDisabled = [
      ComponentStatus.Disabled,
      ComponentStatus.Loading,
      ComponentStatus.Error,
    ].includes(status)

    return isDisabled
  }

  private get classname(): string {
    const {active, color, size} = this.props

    return classnames('dropdown--button button', {
      'button-stretch': true,
      'button-disabled': this.isDisabled,
      [`button-${color}`]: color,
      [`button-${size}`]: size,
      active,
    })
  }

  private get icon(): JSX.Element {
    const {icon} = this.props

    if (icon) {
      return <span className={`dropdown--icon icon ${icon}`} />
    }

    return null
  }
}

export default DropdownButton
