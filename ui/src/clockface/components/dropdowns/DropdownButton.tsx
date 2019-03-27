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
  ButtonType,
} from '@influxdata/clockface'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: DropdownChild
  onClick: (e: MouseEvent<HTMLElement>) => void
  status: ComponentStatus
  color: ComponentColor
  size: ComponentSize
  active: boolean
  icon?: IconFont
  title?: string
  testID?: string
}

@ErrorHandling
class DropdownButton extends Component<Props> {
  public static defaultProps = {
    color: ComponentColor.Default,
    size: ComponentSize.Small,
    status: ComponentStatus.Default,
    active: false,
  }

  public render() {
    const {onClick, children, title, testID} = this.props
    return (
      <button
        className={this.classname}
        onClick={onClick}
        disabled={this.isDisabled}
        title={title}
        type={ButtonType.Button}
        data-testid={testID}
      >
        {this.icon}
        <span className="dropdown--selected">{children}</span>
        <span className="dropdown--caret icon caret-down" />
      </button>
    )
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
