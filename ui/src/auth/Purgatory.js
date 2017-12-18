import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {withRouter} from 'react-router'

import {meChangeOrganizationAsync} from 'shared/actions/auth'

import Notifications from 'shared/components/Notifications'
import SplashPage from 'shared/components/SplashPage'
import PurgatoryAuthItem from 'src/auth/PurgatoryAuthItem'

const getRoleNameByOrgID = (id, roles) => {
  const role = roles.find(r => r.organization === id)
  return (role && role.name) || 'ghost'
}

const handleClickLogin = props => organization => async e => {
  e.preventDefault()
  const {router, links, meChangeOrganization} = props

  await meChangeOrganization(links.me, {organization: organization.id})
  router.push('')
}

class Purgatory extends Component {
  componentWillUpdate() {
    const {router, me} = this.props

    if (me === null) {
      router.push('/login')
    }
  }

  render() {
    const {me, meChangeOrganization, logoutLink, router, links} = this.props

    if (me === null) {
      return null
    }

    const {
      name,
      provider,
      scheme,
      currentOrganization,
      roles,
      organizations,
      superAdmin,
    } = me

    const rolesAndOrgs = organizations.map(organization => ({
      organization,
      role: getRoleNameByOrgID(organization.id, roles),
      currentOrganization: organization.id === currentOrganization.id,
    }))

    const subHeading =
      rolesAndOrgs.length === 1
        ? 'Authenticated in 1 Organization'
        : `Authenticated in ${rolesAndOrgs.length} Organizations`

    return (
      <div>
        <Notifications />
        <SplashPage>
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
                  {rolesAndOrgs.map((rag, i) =>
                    <PurgatoryAuthItem
                      key={i}
                      roleAndOrg={rag}
                      superAdmin={superAdmin}
                      onClickLogin={handleClickLogin({
                        router,
                        links,
                        meChangeOrganization,
                      })}
                    />
                  )}
                </div>
              : <p>You are a Lost Soul</p>}
            <a href={logoutLink} className="btn btn-sm btn-link auth--logout">
              Log out
            </a>
          </div>
        </SplashPage>
      </div>
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

Purgatory.propTypes = {
  router: shape({
    push: func.isRequired,
  }).isRequired,
  links: shape({
    me: string,
  }),
  me: shape({
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
    superAdmin: bool,
  }),
  logoutLink: string,
  meChangeOrganization: func.isRequired,
}

const mapStateToProps = ({links, auth: {me, logoutLink}}) => ({
  links,
  logoutLink,
  me,
})

const mapDispatchToProps = dispatch => ({
  meChangeOrganization: bindActionCreators(meChangeOrganizationAsync, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(
  withRouter(Purgatory)
)
