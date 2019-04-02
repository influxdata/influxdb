// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import NavMenuItem from 'src/pageLayout/components/NavMenuItem'
import NavMenuSubItem from 'src/pageLayout/components/NavMenuSubItem'

// Types
import {ErrorHandling} from 'src/shared/decorators/errors'

interface PassedProps {
  children: JSX.Element[]
  className?: string
}

interface DefaultProps {
  testID: string
}

type Props = PassedProps & Partial<DefaultProps>

@ErrorHandling
class NavMenu extends PureComponent<Props> {
  public static defaultProps = {
    testID: 'nav-menu',
  }

  public static Item = NavMenuItem
  public static SubItem = NavMenuSubItem

  constructor(props) {
    super(props)
  }

  public render() {
    const {children, testID} = this.props

    const className = this.props.className
      ? `nav ${this.props.className}`
      : 'nav'

    return (
      <nav className={className} data-testid={testID}>
        {children}
      </nav>
    )
  }
}

export default NavMenu
