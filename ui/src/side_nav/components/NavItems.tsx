import React, {PureComponent, SFC, ReactNode, ReactElement} from 'react'
import {Link} from 'react-router'
import classnames from 'classnames'

interface NavListItemProps {
  link: string
  location?: string
  useAnchor?: boolean
  isExternal?: boolean
  children?: ReactNode
}

const NavListItem: SFC<NavListItemProps> = ({
  link,
  children,
  location,
  useAnchor,
  isExternal,
}) => {
  const isActive = location.startsWith(link)
  return useAnchor ? (
    <a
      className={classnames('sidebar-menu--item', {active: isActive})}
      href={link}
      target={isExternal ? '_blank' : '_self'}
    >
      {children}
    </a>
  ) : (
    <Link
      className={classnames('sidebar-menu--item', {active: isActive})}
      to={link}
    >
      {children}
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
  matcher?: string
}

class NavBlock extends PureComponent<NavBlockProps> {
  public render() {
    const {location, className} = this.props
    const isActive = React.Children.toArray(this.props.children).find(
      (child: ReactElement<any>) => {
        return location.startsWith(child.props.link) // if location is undefined, this will fail silently
      }
    )

    const children = React.Children.map(
      this.props.children,
      (child: ReactElement<any>) => {
        if (child && child.type === NavListItem) {
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
