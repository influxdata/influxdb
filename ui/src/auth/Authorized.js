import React, {PropTypes} from 'react'
import {connect} from 'react-redux'

export const MEMBER_ROLE = 'member'
export const VIEWER_ROLE = 'viewer'
export const EDITOR_ROLE = 'editor'
export const ADMIN_ROLE = 'admin'
export const SUPERADMIN_ROLE = 'superadmin'

export const isUserAuthorized = (meRole, requiredRole) => {
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
    // 'member' is the default role and has no authorization for anything currently
    case MEMBER_ROLE:
    default:
      return false
  }
}

const Authorized = ({
  children,
  meRole,
  isUsingAuth,
  requiredRole,
  replaceWith,
  propsOverride,
}) => {
  // if me response has not been received yet, render nothing
  if (typeof isUsingAuth !== 'boolean') {
    return null
  }

  // React.isValidElement guards against multiple children wrapped by Authorized
  const firstChild = React.isValidElement(children) ? children : children[0]

  if (!isUsingAuth || isUserAuthorized(meRole, requiredRole)) {
    return firstChild
  }

  if (propsOverride) {
    return React.cloneElement(firstChild, {...propsOverride})
  }

  return replaceWith || null
}

const {bool, node, shape, string} = PropTypes

Authorized.propTypes = {
  isUsingAuth: bool,
  replaceWith: node,
  children: node.isRequired,
  me: shape({
    role: string.isRequired,
  }),
  requiredRole: string.isRequired,
  propsOverride: shape(),
}

const mapStateToProps = ({auth: {me: {role}, isUsingAuth}}) => ({
  meRole: role,
  isUsingAuth,
})

export default connect(mapStateToProps)(Authorized)
