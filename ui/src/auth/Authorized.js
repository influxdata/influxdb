import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {withRouter} from 'react-router'

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

class Authorized extends Component {
  componentWillUpdate() {
    const {router, me} = this.props

    if (me === null) {
      router.push('/login')
    }
  }

  render() {
    const {
      children,
      me,
      isUsingAuth,
      requiredRole,
      replaceWithIfNotAuthorized,
      replaceWithIfNotUsingAuth,
      replaceWithIfAuthorized,
      propsOverride,
    } = this.props

    if (me === null) {
      return null
    }

    // if me response has not been received yet, render nothing
    if (typeof isUsingAuth !== 'boolean') {
      return null
    }

    // React.isValidElement guards against multiple children wrapped by Authorized
    const firstChild = React.isValidElement(children) ? children : children[0]

    if (!isUsingAuth) {
      return replaceWithIfNotUsingAuth || firstChild
    }

    if (isUserAuthorized(me.role, requiredRole)) {
      return replaceWithIfAuthorized || firstChild
    }

    if (propsOverride) {
      return React.cloneElement(firstChild, {...propsOverride})
    }

    return replaceWithIfNotAuthorized || null
  }
}

const {bool, node, shape, string} = PropTypes

Authorized.propTypes = {
  isUsingAuth: bool,
  replaceWithIfNotUsingAuth: node,
  replaceWithIfAuthorized: node,
  replaceWithIfNotAuthorized: node,
  children: node.isRequired,
  router: shape().isRequired,
  me: shape({
    role: string,
  }),
  requiredRole: string.isRequired,
  propsOverride: shape(),
}

const mapStateToProps = ({auth: {me, isUsingAuth}}) => ({
  me,
  isUsingAuth,
})

export default connect(mapStateToProps)(withRouter(Authorized))
