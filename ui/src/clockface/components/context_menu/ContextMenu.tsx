// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Components
import ContextMenuItem from 'src/clockface/components/context_menu/ContextMenuItem'
import {ClickOutside} from 'src/shared/components/ClickOutside'

// Types
import {IconFont} from 'src/clockface/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element | JSX.Element[]
  icon: IconFont
}

interface State {
  isExpanded: boolean
}

@ErrorHandling
class CellMenu extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      isExpanded: false,
    }
  }

  public render() {
    const {icon} = this.props

    return (
      <ClickOutside onClickOutside={this.handleCollapseMenu}>
        <div className="context-menu--container">
          <button
            className={this.toggleClassName}
            onClick={this.handleExpandMenu}
          >
            <span className={`context-menu--icon icon ${icon}`} />
          </button>
          {this.menu}
        </div>
      </ClickOutside>
    )
  }

  private handleExpandMenu = (): void => {
    this.setState({isExpanded: true})
  }

  private handleCollapseMenu = (): void => {
    this.setState({isExpanded: false})
  }

  private get menu(): JSX.Element {
    const {children} = this.props

    return (
      <div className={this.menuClassName}>
        <div className="context-menu--list">
          {React.Children.map(children, (child: JSX.Element) => {
            if (child.type === ContextMenuItem) {
              return (
                <ContextMenuItem
                  {...child.props}
                  onCollapseMenu={this.handleCollapseMenu}
                />
              )
            } else {
              throw new Error('Expected children of type <ContextMenu.Item />')
            }
          })}
        </div>
      </div>
    )
  }

  private get menuClassName(): string {
    const {isExpanded} = this.state

    return classnames('context-menu--list-container', {open: isExpanded})
  }

  private get toggleClassName(): string {
    const {isExpanded} = this.state

    return classnames('context-menu--toggle', {active: isExpanded})
  }
}

export default CellMenu
