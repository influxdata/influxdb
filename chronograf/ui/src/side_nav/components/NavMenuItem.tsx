// Libraries
import React, {SFC} from 'react'
import {Link} from 'react-router'
import classnames from 'classnames'
import _ from 'lodash'

interface Props {
  title: string
  link: string
  children: JSX.Element
  location: string
  highlightWhen: string[]
}

const NavMenuItem: SFC<Props> = ({
  title,
  link,
  children,
  location,
  highlightWhen,
}) => {
  const {length} = _.intersection(_.split(location, '/'), highlightWhen)
  const isActive = !!length

  return (
    <div className={classnames('nav--item', {active: isActive})}>
      <Link className="nav--item-icon" to={link}>
        {children}
      </Link>
      <div className="nav--item-menu">
        <Link className="nav--item-header" to={link}>
          {title}
        </Link>
      </div>
    </div>
  )
}

export default NavMenuItem
