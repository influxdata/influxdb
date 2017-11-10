import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import classnames from 'classnames'

import {meChangeOrganizationAsync} from 'shared/actions/auth'

import {SUPERADMIN_ROLE} from 'src/auth/Authorized'

class UserNavBlock extends Component {
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
      me: {role},
    } = this.props

    const isSuperAdmin = role === SUPERADMIN_ROLE

    return (
      <div className="sidebar--item">
        <div className="sidebar--square">
          <div className="sidebar--icon icon user" />
          {isSuperAdmin
            ? <div className="sidebar--icon sidebar--icon__superadmin icon crown2" />
            : null}
        </div>
        <div className="sidebar-menu">
          <div className="sidebar-menu--heading">
            {me.name}
          </div>
          <div className="sidebar-menu--section">Account</div>
          {isSuperAdmin
            ? <div className="sidebar-menu--superadmin">
                <div>
                  <span className="icon crown2" /> You are a SuperAdmin
                </div>
              </div>
            : null}
          <a className="sidebar-menu--item" href={logoutLink}>
            Logout
          </a>
          <div className="sidebar-menu--section">Switch Organizations</div>
          {roles.map((r, i) => {
            const isLinkCurrentOrg = currentOrganization.id === r.organization
            return (
              <span
                key={i}
                className={classnames({
                  'sidebar-menu--item': true,
                  active: isLinkCurrentOrg,
                })}
                onClick={this.handleChangeCurrentOrganization(r.organization)}
              >
                {organizations.find(o => o.id === r.organization).name}{' '}
                <strong>({r.name})</strong>
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
    role: string,
  }).isRequired,
  meChangeOrganization: func.isRequired,
}

const mapDispatchToProps = dispatch => ({
  meChangeOrganization: bindActionCreators(meChangeOrganizationAsync, dispatch),
})

export default connect(null, mapDispatchToProps)(UserNavBlock)
