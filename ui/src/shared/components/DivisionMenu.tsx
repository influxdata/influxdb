import React, {PureComponent} from 'react'
import uuid from 'uuid'
import classnames from 'classnames'
import {ClickOutside} from 'src/shared/components/ClickOutside'

export interface MenuItem {
  text: string
  action: () => void
}

interface Props {
  menuItems: MenuItem[]
}

interface State {
  expanded: boolean
}

class DivisionMenu extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      expanded: false,
    }
  }

  public render() {
    const {expanded} = this.state

    return (
      <ClickOutside onClickOutside={this.handleCollapseMenu}>
        <div className={this.menuClass}>
          <button className={this.buttonClass} onClick={this.handleExpandMenu}>
            <span className="icon caret-down" />
          </button>
          {expanded && this.renderMenu}
        </div>
      </ClickOutside>
    )
  }

  private handleExpandMenu = (): void => {
    this.setState({expanded: true})
  }

  private handleCollapseMenu = (): void => {
    this.setState({expanded: false})
  }

  private handleMenuItemClick = action => (): void => {
    this.setState({expanded: false})
    action()
  }

  private get menuClass(): string {
    const {expanded} = this.state

    return classnames('dropdown threesizer--menu', {open: expanded})
  }

  private get buttonClass(): string {
    const {expanded} = this.state

    return classnames('btn btn-sm btn-square btn-default', {
      active: expanded,
    })
  }

  private get renderMenu(): JSX.Element {
    const {menuItems} = this.props
    return (
      <ul className="dropdown-menu">
        {menuItems.map(item => (
          <li
            key={uuid.v4()}
            className="dropdown-item"
            onClick={this.handleMenuItemClick(item.action)}
          >
            <a href="#">{item.text}</a>
          </li>
        ))}
      </ul>
    )
  }
}

export default DivisionMenu
