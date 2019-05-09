// Libraries
import React, {PureComponent} from 'react'
import classnames from 'classnames'

// Components
import ContextMenu from 'src/clockface/components/context_menu/ContextMenu'
import ContextMenuItem from 'src/clockface/components/context_menu/ContextMenuItem'

// Types
import {Alignment} from 'src/clockface/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element | JSX.Element[]
  align: Alignment
  className?: string
}

interface State {
  boostZIndex: boolean
}

@ErrorHandling
class Context extends PureComponent<Props, State> {
  public static defaultProps = {
    align: Alignment.Right,
  }

  public static Menu = ContextMenu
  public static Item = ContextMenuItem

  constructor(props: Props) {
    super(props)

    this.state = {
      boostZIndex: false,
    }
  }

  public render() {
    const {children} = this.props

    return (
      <div className={this.className}>
        {React.Children.map(children, (child: JSX.Element) => {
          if (child.type === ContextMenu) {
            return (
              <ContextMenu
                {...child.props}
                onBoostZIndex={this.handleBoostZIndex}
              />
            )
          } else {
            return child
          }
        })}
      </div>
    )
  }

  private handleBoostZIndex = (boostZIndex: boolean): void => {
    this.setState({boostZIndex})
  }

  private get className(): string {
    const {align, className} = this.props
    const {boostZIndex} = this.state

    return classnames('context-menu', {
      [`${className}`]: className,
      'context-menu--boost-z': boostZIndex,
      'context-menu--align-left': align === Alignment.Left,
      'context-menu--align-right': align === Alignment.Right,
    })
  }
}

export default Context
