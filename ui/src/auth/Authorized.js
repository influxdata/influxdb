import React, {PropTypes} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

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
    default:
      return false
  }
}

export const getMeRole = me => {
  return _.get(_.first(_.get(me, 'roles', [])), 'name', 'none') // TODO: TBD if 'none' should be returned if none
}

const Authorized = ({
  children,
  me,
  isUsingAuth,
  requiredRole,
  replaceWith,
  propsOverride,
  ...additionalProps
}) => {
  // if me response has not been received yet, render nothing
  if (typeof isUsingAuth !== 'boolean') {
    return null
  }

  const meRole = getMeRole(me)

  if (!isUsingAuth || isUserAuthorized(meRole, requiredRole)) {
    return React.cloneElement(
      React.isValidElement(children) ? children : children[0],
      {...additionalProps, ...propsOverride}
    ) // guards against multiple children wrapped by Authorized
  } else if (
    isUsingAuth &&
    !isUserAuthorized(meRole, requiredRole) &&
    propsOverride
  ) {
    return React.cloneElement(
      React.isValidElement(children) ? children : children[0],
      {...additionalProps, ...propsOverride}
    )
  }

  return replaceWith || null

  // if you want elements to be disabled instead of hidden:
  // return React.cloneElement(clonedElement, {disabled: !isAuthorized})
}

const {arrayOf, bool, node, shape, string} = PropTypes

Authorized.propTypes = {
  isUsingAuth: bool,
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
  propsOverride: shape(),
}

const mapStateToProps = ({auth: {me, isUsingAuth}}) => ({
  me,
  isUsingAuth,
})

export default connect(mapStateToProps)(Authorized)
