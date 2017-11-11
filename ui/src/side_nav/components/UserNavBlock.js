import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {withRouter} from 'react-router'

import classnames from 'classnames'

import {meChangeOrganizationAsync} from 'shared/actions/auth'

class UserNavBlock extends Component {
  handleChangeCurrentOrganization = organizationID => async () => {
    const {router, links, meChangeOrganization} = this.props

    await meChangeOrganization(links.me, {organization: organizationID})
    router.push('')
  }

  render() {
    const {
      logoutLink,
      links: {external: {custom: customLinks}},
      me,
      me: {currentOrganization, organizations, roles},
    } = this.props

    // TODO: find a better way to glean this information.
    // Need this method for when a user is a superadmin,
    // which doesn't reflect their role in the current org
    const currentRole = roles.find(cr => {
      return cr.organization === currentOrganization.id
    }).name

    return (
      <div className="sidebar--item">
        <div className="sidebar--square">
          <div className="sidebar--icon icon user" />
        </div>
        <div className="sidebar-menu">
          <div className="sidebar-menu--heading">
            {currentOrganization.name} ({currentRole})
          </div>
          <div className="sidebar-menu--section">
            {me.name}
          </div>
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
  router: shape({
    push: func.isRequired,
  }).isRequired,
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

export default connect(null, mapDispatchToProps)(withRouter(UserNavBlock))
