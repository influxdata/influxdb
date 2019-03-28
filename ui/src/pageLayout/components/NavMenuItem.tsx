// Libraries
import React, {SFC} from 'react'
import {Link} from 'react-router'
import classnames from 'classnames'
import {get} from 'lodash'

// Types
import {IconFont} from 'src/clockface'

interface Props {
  icon: IconFont
  title: string
  link: string
  children?: JSX.Element | JSX.Element[]
  location: string
  highlightPaths: string[]
}

const NavMenuItem: SFC<Props> = ({
  icon,
  title,
  link,
  children,
  location,
  highlightPaths,
}) => {
  const parentPath = get(location.split('/'), '1', '')
  const isActive = highlightPaths.some(path => path === parentPath)

  return (
    <div
      className={classnames('nav--item', {active: isActive})}
      data-testid={`nav-menu-item ${icon}`}
    >
      <Link className="nav--item-icon" to={link}>
        <span className={`icon sidebar--icon ${icon}`} />
      </Link>
      <div className="nav--item-menu">
        <Link className="nav--item-header" to={link}>
          {title}
        </Link>
        {children}
      </div>
    </div>
  )
}

export default NavMenuItem
