// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import NavMenuItem from 'src/pageLayout/components/NavMenuItem'
import Avatar from 'src/shared/components/avatar/Avatar'

// Types
import {NavItem, NavItemType} from 'src/pageLayout/containers/Nav'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {IconFont} from 'src/clockface'

interface Props {
  navItems: NavItem[]
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
        {navItems.map(
          ({title, highlightWhen, icon, link, location, type, image}) => (
            <NavMenuItem
              key={`navigation--${title}`}
              title={title}
              highlightWhen={highlightWhen}
              link={link}
              location={location}
            >
              {this.renderIconOrGraphic(type, icon, image)}
            </NavMenuItem>
          )
        )}
      </nav>
    )
  }

  private renderIconOrGraphic = (
    type: NavItemType,
    icon?: IconFont,
    image?: string
  ): JSX.Element => {
    if (type === NavItemType.Avatar) {
      return (
        <Avatar
          imageURI={image}
          diameterPixels={30}
          customClass="nav--avatar"
        />
      )
    } else if (type === NavItemType.Icon) {
      return <span className={`icon sidebar--icon ${icon}`} />
    }
  }
}

export default NavMenu
