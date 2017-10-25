import React, {PropTypes} from 'react'
import {withRouter, Link} from 'react-router'
import {connect} from 'react-redux'

import {
  NavBar,
  NavBlock,
  NavHeader,
  NavListItem,
} from 'src/side_nav/components/NavItems'

import {DEFAULT_HOME_PAGE} from 'shared/constants'

const {arrayOf, bool, shape, string} = PropTypes

const SideNav = React.createClass({
  propTypes: {
    params: shape({
      sourceID: string.isRequired,
    }).isRequired,
    location: shape({
      pathname: string.isRequired,
    }).isRequired,
    isHidden: bool.isRequired,
    isUsingAuth: bool,
    logoutLink: string,
    customLinks: arrayOf(
      shape({
        name: string.isRequired,
        url: string.isRequired,
      })
    ),
  },

  renderUserMenuBlockWithCustomLinks(customLinks, logoutLink) {
    return [
      <NavHeader key={0} title="User" />,
      ...customLinks
        .sort((a, b) => a.name.toLowerCase() > b.name.toLowerCase())
        .map(({name, url}, i) =>
          <NavListItem
            key={i + 1}
            useAnchor={true}
            isExternal={true}
            link={url}
          >
            {name}
          </NavListItem>
        ),
      <NavListItem
        key={customLinks.length + 1}
        useAnchor={true}
        link={logoutLink}
      >
        Logout
      </NavListItem>,
    ]
  },

  render() {
    const {
      params: {sourceID},
      location: {pathname: location},
      isHidden,
      isUsingAuth,
      logoutLink,
      customLinks,
    } = this.props

    const sourcePrefix = `/sources/${sourceID}`
    const dataExplorerLink = `${sourcePrefix}/chronograf/data-explorer`

    const isDefaultPage = location.split('/').includes(DEFAULT_HOME_PAGE)

    return isHidden
      ? null
      : <NavBar location={location}>
          <div
            className={isDefaultPage ? 'sidebar--item active' : 'sidebar--item'}
          >
            <Link
              to={`${sourcePrefix}/${DEFAULT_HOME_PAGE}`}
              className="sidebar--square sidebar--logo"
            >
              <span className="sidebar--icon icon cubo-uniform" />
            </Link>
          </div>
          <NavBlock icon="cubo-node" link={`${sourcePrefix}/hosts`}>
            <NavHeader link={`${sourcePrefix}/hosts`} title="Host List" />
          </NavBlock>
          <NavBlock icon="graphline" link={dataExplorerLink}>
            <NavHeader link={dataExplorerLink} title="Data Explorer" />
          </NavBlock>
          <NavBlock icon="dash-h" link={`${sourcePrefix}/dashboards`}>
            <NavHeader
              link={`${sourcePrefix}/dashboards`}
              title={'Dashboards'}
            />
          </NavBlock>
          <NavBlock
            matcher="alerts"
            icon="alert-triangle"
            link={`${sourcePrefix}/alerts`}
          >
            <NavHeader link={`${sourcePrefix}/alerts`} title="Alerting" />
            <NavListItem link={`${sourcePrefix}/alerts`}>History</NavListItem>
            <NavListItem link={`${sourcePrefix}/alert-rules`}>
              Create
            </NavListItem>
          </NavBlock>
          <NavBlock icon="crown2" link={`${sourcePrefix}/admin`}>
            <NavHeader link={`${sourcePrefix}/admin`} title="Admin" />
          </NavBlock>
          <NavBlock icon="cog-thick" link={`${sourcePrefix}/manage-sources`}>
            <NavHeader
              link={`${sourcePrefix}/manage-sources`}
              title="Configuration"
            />
          </NavBlock>
          {isUsingAuth
            ? <NavBlock icon="user">
                {customLinks
                  ? this.renderUserMenuBlockWithCustomLinks(
                      customLinks,
                      logoutLink
                    )
                  : <NavHeader
                      useAnchor={true}
                      link={logoutLink}
                      title="Logout"
                    />}
              </NavBlock>
            : null}
        </NavBar>
  },
})

const mapStateToProps = ({
  auth: {isUsingAuth, logoutLink},
  app: {ephemeral: {inPresentationMode}},
  links: {external: {custom: customLinks}},
}) => ({
  isHidden: inPresentationMode,
  isUsingAuth,
  logoutLink,
  customLinks,
})

export default connect(mapStateToProps)(withRouter(SideNav))
