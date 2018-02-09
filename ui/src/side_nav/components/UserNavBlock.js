import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import {withRouter} from 'react-router'

import classnames from 'classnames'

import FancyScrollbar from 'shared/components/FancyScrollbar'

import {meChangeOrganizationAsync} from 'shared/actions/auth'

import {SUPERADMIN_ROLE} from 'src/auth/Authorized'

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
      me: {role},
    } = this.props

    const isSuperAdmin = role === SUPERADMIN_ROLE

    return (
      <div className="sidebar--item">
        <div className="sidebar--square">
          <div className="sidebar--icon icon user" />
          {isSuperAdmin
            ? <span className="sidebar--icon sidebar--icon__superadmin icon crown2" />
            : null}
        </div>
        <div className="sidebar-menu sidebar-menu--user-nav">
          {customLinks
            ? <div className="sidebar-menu--section sidebar-menu--section__custom-links">
                Custom Links
              </div>
            : null}
          {customLinks
            ? customLinks.map((link, i) =>
                <a
                  key={i}
                  className="sidebar-menu--item sidebar-menu--item__link-name"
                  href={link.url}
                  target="_blank"
                >
                  {link.name}
                </a>
              )
            : null}
          <div className="sidebar-menu--section sidebar-menu--section__switch-orgs">
            Switch Organizations
          </div>
          <FancyScrollbar
            className="sidebar-menu--scrollbar"
            autoHeight={true}
            maxHeight={100}
            autoHide={false}
          >
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
          </FancyScrollbar>
          <div className="sidebar-menu--section sidebar-menu--section__account">
            Account
          </div>
          <div className="sidebar-menu--provider">
            <div>
              {me.scheme} / {me.provider}
            </div>
          </div>
          <a
            className="sidebar-menu--item sidebar-menu--item__logout"
            href={logoutLink}
          >
            Log out
          </a>
          <div className="sidebar-menu--heading sidebar--no-hover">
            {me.name}
          </div>
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
