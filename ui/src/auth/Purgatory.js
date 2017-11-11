import React, {PropTypes} from 'react'
import {withRouter} from 'react-router'
import {connect} from 'react-redux'

import {MEMBER_ROLE} from 'src/auth/Authorized'

const getRoleNameByOrgID = (id, roles) => {
  const role = roles.find(r => r.organization === id)
  return (role && role.name) || 'ghost'
}

const Purgatory = ({
  router,
  name,
  provider,
  scheme,
  currentOrganization,
  roles,
  organizations,
  logoutLink,
}) => {
  const rolesAndOrgs = organizations.map(({id, name: orgName}) => ({
    id,
    organization: orgName,
    role: getRoleNameByOrgID(id, roles),
    currentOrganization: id === currentOrganization.id,
  }))

  const subHeading =
    rolesAndOrgs.length === 1
      ? 'Authenticated in 1 Organization'
      : `Authenticated in ${rolesAndOrgs.length} Organizations`

  const loginLink = () => {
    return router.push('')
  }

  return (
    <div>
      <div className="auth-page">
        <div className="auth-box">
          <div className="auth-logo" />
          <div className="auth--purgatory">
            <h3>
              {name}
            </h3>
            <h6>
              {subHeading}{' '}
              <code>
                {scheme}/{provider}
              </code>
            </h6>
            {rolesAndOrgs.length
              ? <div className="auth--list">
                  {rolesAndOrgs.map(rag =>
                    <div
                      key={rag.id}
                      className={
                        rag.currentOrganization
                          ? 'auth--list-item current'
                          : 'auth--list-item'
                      }
                    >
                      <div className="auth--list-info">
                        <div className="auth--list-org">
                          {rag.organization}
                        </div>
                        <div className="auth--list-role">
                          {rag.role}
                        </div>
                      </div>
                      {rag.role === MEMBER_ROLE
                        ? <span className="auth--list-blocked">
                            Contact your Admin<br />for access
                          </span>
                        : <button
                            className="btn btn-sm btn-primary"
                            onClick={loginLink}
                          >
                            Login
                          </button>}
                    </div>
                  )}
                </div>
              : <p>You are a Lost Soul</p>}
            <a href={logoutLink} className="btn btn-sm btn-link auth--logout">
              Logout
            </a>
          </div>
        </div>
        <p className="auth-credits">
          Made by <span className="icon cubo-uniform" />InfluxData
        </p>
        <div className="auth-image" />
      </div>
    </div>
  )
}

const {arrayOf, func, shape, string} = PropTypes

Purgatory.propTypes = {
  router: shape({
    push: func.isRequired,
  }).isRequired,
  name: string.isRequired,
  provider: string.isRequired,
  scheme: string.isRequired,
  currentOrganization: shape({
    id: string.isRequired,
    name: string.isRequired,
  }).isRequired,
  roles: arrayOf(
    shape({
      name: string,
      organization: string,
    })
  ).isRequired,
  organizations: arrayOf(
    shape({
      id: string,
      name: string,
    })
  ).isRequired,
  logoutLink: string.isRequired,
}

const mapStateToProps = ({
  auth: {
    me: {name, provider, scheme, currentOrganization, roles, organizations},
    logoutLink,
  },
}) => ({
  name,
  provider,
  scheme,
  currentOrganization,
  roles,
  organizations,
  logoutLink,
})

export default connect(mapStateToProps)(withRouter(Purgatory))
