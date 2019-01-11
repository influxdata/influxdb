// Libraries
import React, {SFC} from 'react'
import {Link} from 'react-router'
import classnames from 'classnames'
import _ from 'lodash'

interface Props {
  title: string
  link: string
  location: string
  highlightWhen: string[]
}

const NavMenuSubItem: SFC<Props> = ({title, link, location, highlightWhen}) => {
  const {length} = _.intersection(_.split(location, '/'), highlightWhen)
  const isActive = !!length

  return (
    <Link className={classnames('nav--sub-item', {active: isActive})} to={link}>
      {title}
    </Link>
  )
}

export default NavMenuSubItem
