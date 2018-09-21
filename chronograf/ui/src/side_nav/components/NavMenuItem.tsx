// Libraries
import React, {PureComponent, SFC, ReactNode, ReactElement} from 'react'
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

const NavListItem: SFC<Props> = ({
  title,
  link,
  children,
  location,
  highlightWhen,
}) => {
  const {length} = _.intersection(_.split(location, '/'), highlightWhen)
  const isActive = !!length


  <Link
    className={classnames('sidebar-menu--item', {active: isActive})}
    to={link}
  >
    <div>
      
    </div>
  </Link>
  )
}

interface NavHeaderProps {
  link?: string
  title?: string
  useAnchor?: string
}

const NavHeader: SFC<NavHeaderProps> = ({link, title, useAnchor}) => {
  // Some nav items, such as Logout, need to hit an external link rather
  // than simply route to an internal page. Anchor tags serve that purpose.
  return useAnchor ? (
    <a className="sidebar-menu--heading" href={link}>
      {title}
    </a>
  ) : (
    <Link className="sidebar-menu--heading" to={link}>
      {title}
    </Link>
  )
}

interface NavBlockProps {
  children?: ReactNode
  link?: string
  icon: string
  location?: string
  className?: string
  highlightWhen: string[]
}

class NavBlock extends PureComponent<NavBlockProps> {
  public render() {
    const {location, className, highlightWhen} = this.props
    const {length} = _.intersection(_.split(location, '/'), highlightWhen)
    const isActive = !!length

    const children = React.Children.map(
      this.props.children,
      (child: ReactElement<any>) => {
        // FIXME
        if (child && String(child.type) === String(NavListItem)) {
          return React.cloneElement(child, {location})
        }

        return child
      }
    )

    return (
      <div
        className={classnames('sidebar--item', className, {active: isActive})}
      >
        {this.renderSquare()}
        <div className="sidebar-menu">
          {children}
          <div className="sidebar-menu--triangle" />
        </div>
      </div>
    )
  }

  private renderSquare() {
    const {link, icon} = this.props

    if (!link) {
      return (
        <div className="sidebar--square">
          <div className={`sidebar--icon icon ${icon}`} />
        </div>
      )
    }

    return (
      <Link className="sidebar--square" to={link}>
        <div className={`sidebar--icon icon ${icon}`} />
      </Link>
    )
  }
}

export {NavBlock, NavHeader, NavListItem}
