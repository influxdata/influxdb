// Libraries
import React, {Component, MouseEvent} from 'react'
import classnames from 'classnames'

interface Props {
  disabled?: boolean
  onClick: (e?: MouseEvent<HTMLLIElement>) => void
  children: JSX.Element[] | JSX.Element | string
}

class RightClickMenuItem extends Component<Props> {
  public render() {
    const {children} = this.props

    return (
      <li className={this.className} onClick={this.handleClick}>
        {children}
      </li>
    )
  }

  private handleClick = (e: MouseEvent<HTMLLIElement>) => {
    const {onClick, disabled} = this.props

    if (disabled) {
      e.stopPropagation()
      return
    }

    onClick(e)
  }

  private get className(): string {
    const {disabled} = this.props

    return classnames('right-click--menu-item', {disabled})
  }
}

export default RightClickMenuItem
