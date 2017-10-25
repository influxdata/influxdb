import React, {Component, PropTypes} from 'react'

import {connect} from 'react-redux'

export const VIEWER_ROLE = 'viewer'
export const EDITOR_ROLE = 'editor'
export const ADMIN_ROLE = 'admin'
export const SUPERADMIN_ROLE = 'superadmin'

const getRoleName = ({roles: [{name}, ..._]}) => name

class Authorized extends Component {
  isAuthorized = (meRole, requiredRole) => {
    switch (requiredRole) {
      case VIEWER_ROLE:
        return (
          meRole === VIEWER_ROLE ||
          meRole === EDITOR_ROLE ||
          meRole === ADMIN_ROLE ||
          meRole === SUPERADMIN_ROLE
        )
      case EDITOR_ROLE:
        return (
          meRole === EDITOR_ROLE ||
          meRole === ADMIN_ROLE ||
          meRole === SUPERADMIN_ROLE
        )
      case ADMIN_ROLE:
        return meRole === ADMIN_ROLE || meRole === SUPERADMIN_ROLE
      case SUPERADMIN_ROLE:
        return meRole === SUPERADMIN_ROLE
      default:
        return false
    }
  }

  render() {
    const {children, me, isUsingAuth, requiredRole, replaceWith} = this.props

    if (!isUsingAuth) {
      return React.isValidElement(children) ? children : children[0]
    }

    const meRole = getRoleName(me)
    if (this.isAuthorized(meRole, requiredRole)) {
      return React.cloneElement(
        React.isValidElement(children) ? children : children[0]
      )
    }

    return replaceWith ? React.cloneElement(replaceWith) : null

    // if you want elements to be disabled instead of hidden:
    // return React.cloneElement(clonedElement, {disabled: !isAuthorized})
  }
}

const {arrayOf, bool, node, shape, string} = PropTypes

Authorized.propTypes = {
  isUsingAuth: bool.isRequired,
  replaceWith: node,
  children: node.isRequired,
  me: shape({
    roles: arrayOf(
      shape({
        name: string.isRequired,
      })
    ),
  }),
  requiredRole: string.isRequired,
}

const mapStateToProps = ({auth: {me, isUsingAuth}}) => ({
  me,
  isUsingAuth,
})

export default connect(mapStateToProps)(Authorized)
