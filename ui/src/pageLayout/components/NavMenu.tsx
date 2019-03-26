// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import NavMenuItem from 'src/pageLayout/components/NavMenuItem'
import NavMenuSubItem from 'src/pageLayout/components/NavMenuSubItem'

// Types
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element[]
}

@ErrorHandling
class NavMenu extends PureComponent<Props> {
  public static Item = NavMenuItem
  public static SubItem = NavMenuSubItem

  constructor(props) {
    super(props)
  }

  public render() {
    const {children} = this.props

    return (
      <nav className="nav" data-testid="nav">
        {children}
      </nav>
    )
  }
}

export default NavMenu
