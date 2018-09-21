// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {NavMenuItem} from 'src/side_nav/components/NavMenuItem'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  navItems: NavItem[]
}

interface NavItem {
  title: string
  link: string
  icon: string
  location: string
  highlightWhen: string[]
}

@ErrorHandling
class NavMenu extends PureComponent<Props> {
  constructor(props) {
    super(props)
  }

  public render() {
    const {navItems} = this.props

    return (
      <nav className="nav">
        {navItems.map(({highlightWhen, icon, link, location}) => (
          <NavMenuItem
            highlightWhen={highlightWhen}
            link={link}
            location={location}
          >
            <span className={`icon ${icon}`} />
          </NavMenuItem>
        ))}
      </nav>
    )
  }
}

export default NavMenu
