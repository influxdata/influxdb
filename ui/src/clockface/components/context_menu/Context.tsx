// Libraries
import React, {PureComponent} from 'react'
import classnames from 'classnames'

// Components
import ContextMenu from 'src/clockface/components/context_menu/ContextMenu'
import ContextMenuItem from 'src/clockface/components/context_menu/ContextMenuItem'

// Types
import {Alignment} from 'src/clockface/types'

// Styles
import './ContextMenu.scss'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  align?: Alignment
  children: JSX.Element | JSX.Element[]
}

@ErrorHandling
class CellContext extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    align: Alignment.Right,
  }

  public static Menu = ContextMenu
  public static Item = ContextMenuItem

  public render() {
    const {children} = this.props

    return <div className={this.className}>{children}</div>
  }

  private get className(): string {
    const {align} = this.props

    return classnames('context-menu', {
      'context-menu--align-left': align === Alignment.Left,
      'context-menu--align-right': align === Alignment.Right,
    })
  }
}

export default CellContext
