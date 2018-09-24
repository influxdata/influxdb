// Libraries
import React, {Component} from 'react'
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
  onClick: () => void
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
    const {onClick, status, children, title} = this.props
    return (
      <button
        className={this.classname}
        onClick={onClick}
        disabled={status === ComponentStatus.Disabled}
        title={title}
      >
        {this.icon}
        <span className="dropdown--selected">{children}</span>
        <span className="dropdown--caret icon caret-down" />
      </button>
    )
  }

  private get classname(): string {
    const {status, active, color, size} = this.props

    return classnames('dropdown--button button', {
      'button-stretch': true,
      [`button-${color}`]: color,
      [`button-${size}`]: size,
      disabled: status === ComponentStatus.Disabled,
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
