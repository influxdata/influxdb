import React, {PropTypes, Component} from 'react'
import classnames from 'classnames'

class UserNavBlock extends Component {
  constructor(props) {
    super(props)
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
            {me.name}
          </div>
          <div className="sidebar-menu--section">Organizations</div>
          {roles.map((role, i) => {
            const isLinkCurrentOrg =
              currentOrganization.id === role.organization
            return (
              <a
                key={i}
                className={classnames({
                  'sidebar-menu--item': true,
                  active: isLinkCurrentOrg,
                })}
                href="#"
              >
                {organizations.find(o => o.id === role.organization).name}{' '}
                <strong>({role.name})</strong>
              </a>
            )
          })}
          <a className="sidebar-menu--item" href={logoutLink}>
            Logout
          </a>
          {customLinks ? <div className="sidebar-menu--divider" /> : null}
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

const {arrayOf, shape, string} = PropTypes

UserNavBlock.propTypes = {
  links: shape({
    me: string.isRequired,
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
    }).isRequired,
    name: string.isRequired,
    organizations: arrayOf(
      shape({
        id: string.isRequired,
        name: string.isRequired,
      })
    ).isRequired,
    roles: arrayOf(
      shape({
        id: string.isRequired,
        name: string.isRequired,
      })
    ).isRequired,
  }).isRequired,
}

export default UserNavBlock
