import React, {PropTypes} from 'react'
import {Link} from 'react-router'
import classnames from 'classnames'

const {bool, node, string} = PropTypes

const NavListItem = React.createClass({
  propTypes: {
    link: string.isRequired,
    children: node,
    location: string,
    useAnchor: bool,
    isExternal: bool,
  },

  render() {
    const {link, children, location, useAnchor, isExternal} = this.props
    const isActive = location.startsWith(link)

    return useAnchor
      ? <a
          className={classnames('sidebar-menu--item', {active: isActive})}
          href={link}
          target={isExternal ? '_blank' : '_self'}
        >
          {children}
        </a>
      : <Link
          className={classnames('sidebar-menu--item', {active: isActive})}
          to={link}
        >
          {children}
        </Link>
  },
})

const NavHeader = React.createClass({
  propTypes: {
    link: string,
    title: string,
    useAnchor: bool,
  },
  render() {
    const {link, title, useAnchor} = this.props

    // Some nav items, such as Logout, need to hit an external link rather
    // than simply route to an internal page. Anchor tags serve that purpose.
    return useAnchor
      ? <a className="sidebar-menu--heading" href={link}>
          {title}
        </a>
      : <Link className="sidebar-menu--heading" to={link}>
          {title}
        </Link>
  },
})

const NavBlock = React.createClass({
  propTypes: {
    children: node,
    link: string,
    icon: string.isRequired,
    location: string,
    className: string,
  },

  render() {
    const {location, className} = this.props

    const isActive = React.Children.toArray(this.props.children).find(child => {
      return location.startsWith(child.props.link)
    })

    const children = React.Children.map(this.props.children, child => {
      if (child && child.type === NavListItem) {
        return React.cloneElement(child, {location})
      }

      return child
    })

    return (
      <div
        className={classnames('sidebar--item', className, {active: isActive})}
      >
        {this.renderSquare()}
        <div className="sidebar-menu">
          {children}
        </div>
      </div>
    )
  },

  renderSquare() {
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
  },
})

const NavBar = React.createClass({
  propTypes: {
    children: node,
    location: string.isRequired,
  },

  render() {
    const children = React.Children.map(this.props.children, child => {
      if (child && child.type === NavBlock) {
        return React.cloneElement(child, {
          location: this.props.location,
        })
      }

      return child
    })
    return (
      <nav className="sidebar">
        {children}
      </nav>
    )
  },
})

export {NavBar, NavBlock, NavHeader, NavListItem}
