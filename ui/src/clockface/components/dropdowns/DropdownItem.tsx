// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Types
import {DropdownChild} from 'src/clockface/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  id: string
  children: DropdownChild
  value: any
  selected?: boolean
  checkbox?: boolean
  onClick?: (value: any) => void
}

@ErrorHandling
class DropdownItem extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    checkbox: false,
    selected: false,
  }

  public render(): JSX.Element {
    const {selected, checkbox} = this.props

    return (
      <div
        className={classnames('dropdown--item', {
          active: selected,
          'multi-select--item': checkbox,
        })}
        onClick={this.handleClick}
      >
        {this.checkBox}
        {this.dot}
        {this.childElements}
      </div>
    )
  }

  private handleClick = (): void => {
    const {onClick, value} = this.props

    onClick(value)
  }

  private get checkBox(): JSX.Element {
    const {checkbox} = this.props

    if (checkbox) {
      return <div className="dropdown-item--checkbox" />
    }

    return null
  }

  private get dot(): JSX.Element {
    const {checkbox, selected} = this.props

    if (selected && !checkbox) {
      return <div className="dropdown-item--dot" />
    }
  }

  private get childElements(): JSX.Element {
    const {children} = this.props

    return <div className="dropdown-item--children">{children}</div>
  }
}

export default DropdownItem
