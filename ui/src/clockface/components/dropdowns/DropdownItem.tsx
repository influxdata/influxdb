// Libraries
import React, {Component, MouseEvent} from 'react'
import classnames from 'classnames'

// Types
import {DropdownChild} from 'src/clockface/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  id: string
  children: DropdownChild
  value: any
  selected: boolean
  checkbox: boolean
  onClick?: (value: any) => void
  testID?: string
}

@ErrorHandling
class DropdownItem extends Component<Props> {
  public static defaultProps = {
    checkbox: false,
    selected: false,
  }

  public render(): JSX.Element {
    const {selected, checkbox, id} = this.props

    return (
      <div
        className={classnames('dropdown--item', {
          checked: selected && checkbox,
          active: selected && !checkbox,
          'multi-select--item': checkbox,
        })}
        data-testid={`dropdown--item ${id}`}
        onClick={this.handleClick}
      >
        {this.checkBox}
        {this.childElements}
        {this.dot}
      </div>
    )
  }

  private handleClick = (e: MouseEvent<HTMLElement>): void => {
    e.preventDefault()
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
