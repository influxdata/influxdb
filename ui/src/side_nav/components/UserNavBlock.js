import React, {PropTypes, Component} from 'react'
import {Link} from 'react-router'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import Authorized, {SUPERADMIN_ROLE} from 'src/auth/Authorized'

import classnames from 'classnames'

import {meChangeOrganizationAsync} from 'shared/actions/auth'

class UserNavBlock extends Component {
  constructor(props) {
    super(props)
  }

  handleChangeCurrentOrganization = organizationID => () => {
    const {links, meChangeOrganization} = this.props
    meChangeOrganization(links.me, {organization: organizationID})
  }

  render() {
    const {
      logoutLink,
      links: {external: {custom: customLinks}},
      me,
      me: {currentOrganization, organizations, roles},
    } = this.props

    return (
      <div className="sidebar--item">
        <div className="sidebar--square">
          <div className="sidebar--icon icon user" />
        </div>
        <div className="sidebar-menu">
          <div className="sidebar-menu--heading">
            {currentOrganization.name} (Member)
          </div>
          <div className="sidebar-menu--section">
            {me.name}
          </div>
          <Authorized requiredRole={SUPERADMIN_ROLE}>
            <Link className="sidebar-menu--item" to="/admin/users">
              Manage Users
            </Link>
          </Authorized>
          <Authorized requiredRole={SUPERADMIN_ROLE}>
            <Link className="sidebar-menu--item" to="/admin/organizations">
              Manage Organizations
            </Link>
          </Authorized>
          <a className="sidebar-menu--item" href={logoutLink}>
            Logout
          </a>
          <div className="sidebar-menu--section">Switch Organizations</div>
          {roles.map((role, i) => {
            const isLinkCurrentOrg =
              currentOrganization.id === role.organization
            return (
              <span
                key={i}
                className={classnames({
                  'sidebar-menu--item': true,
                  active: isLinkCurrentOrg,
                })}
                onClick={this.handleChangeCurrentOrganization(
                  role.organization
                )}
              >
                {organizations.find(o => o.id === role.organization).name}{' '}
                <strong>({role.name})</strong>
              </span>
            )
          })}
          {customLinks
            ? <div className="sidebar-menu--section">Custom Links</div>
            : null}
          {customLinks
            ? customLinks.map((link, i) =>
                <a
                  key={i}
                  className="sidebar-menu--item"
                  href={link.url}
                  target="_blank"
                >
                  {link.name}
                </a>
              )
            : null}
          <div className="sidebar-menu--triangle" />
        </div>
      </div>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

UserNavBlock.propTypes = {
  links: shape({
    me: string,
    external: shape({
      custom: arrayOf(
        shape({
          name: string.isRequired,
          url: string.isRequired,
        })
      ),
    }),
  }),
  logoutLink: string.isRequired,
  me: shape({
    currentOrganization: shape({
      id: string.isRequired,
      name: string.isRequired,
    }),
    name: string,
    organizations: arrayOf(
      shape({
        id: string.isRequired,
        name: string.isRequired,
      })
    ),
    roles: arrayOf(
      shape({
        id: string,
        name: string,
      })
    ),
  }).isRequired,
  meChangeOrganization: func.isRequired,
}

const mapDispatchToProps = dispatch => ({
  meChangeOrganization: bindActionCreators(meChangeOrganizationAsync, dispatch),
})

export default connect(null, mapDispatchToProps)(UserNavBlock)
