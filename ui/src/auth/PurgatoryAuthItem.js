import React, {PropTypes} from 'react'

import {isUserAuthorized, VIEWER_ROLE} from 'src/auth/Authorized'

const PurgatoryAuthItem = ({roleAndOrg, onClickLogin, superAdmin}) =>
  <div
    className={
      roleAndOrg.currentOrganization
        ? 'auth--list-item current'
        : 'auth--list-item'
    }
  >
    <div className="auth--list-info">
      <div className="auth--list-org">
        {roleAndOrg.organization.name}
      </div>
      <div className="auth--list-role">
        {roleAndOrg.role}
      </div>
    </div>
    {superAdmin || isUserAuthorized(roleAndOrg.role, VIEWER_ROLE)
      ? <button
          className="btn btn-sm btn-primary"
          onClick={onClickLogin(roleAndOrg.organization)}
        >
          Log in
        </button>
      : <span className="auth--list-blocked">
          Contact your Admin<br />for access
        </span>}
  </div>

const {bool, func, shape, string} = PropTypes

PurgatoryAuthItem.propTypes = {
  roleAndOrg: shape({
    organization: shape({
      name: string,
      id: string,
    }),
    role: string,
    currentOrganization: bool,
  }).isRequired,
  superAdmin: bool,
  onClickLogin: func.isRequired,
}

export default PurgatoryAuthItem
