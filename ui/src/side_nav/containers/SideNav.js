import React, {PropTypes} from 'react'
import {withRouter, Link} from 'react-router'
import {connect} from 'react-redux'

import {
  NavBar,
  NavBlock,
  NavHeader,
  NavListItem,
} from 'src/side_nav/components/NavItems'

const {bool, shape, string} = PropTypes

const SideNav = React.createClass({
  propTypes: {
    params: shape({
      sourceID: string.isRequired,
    }).isRequired,
    location: shape({
      pathname: string.isRequired,
    }).isRequired,
    me: shape({
      email: string,
    }),
    isHidden: bool.isRequired,
    logoutLink: string,
  },

  render() {
    const {
      me,
      params: {sourceID},
      location: {pathname: location},
      isHidden,
      logoutLink,
    } = this.props

    const sourcePrefix = `/sources/${sourceID}`
    const dataExplorerLink = `${sourcePrefix}/chronograf/data-explorer`
    const showLogout = !!(me && me.name)

    return isHidden
      ? null
      : <NavBar location={location}>
          <div className="sidebar__logo">
            <Link to="/"><span className="icon cubo-uniform" /></Link>
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
            <NavListItem link={`${sourcePrefix}/alerts`}>
              Alert History
            </NavListItem>
            <NavListItem link={`${sourcePrefix}/alert-rules`}>
              Alert Rules
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
          {showLogout
            ? <NavBlock icon="user-outline" className="sidebar__square-last">
                <a className="sidebar__menu-item" href={logoutLink}>
                  Logout
                </a>
              </NavBlock>
            : null}
        </NavBar>
  },
})

const mapStateToProps = ({
  auth: {me, logoutLink},
  app: {ephemeral: {inPresentationMode}},
}) => ({
  me,
  isHidden: inPresentationMode,
  logoutLink,
})

export default connect(mapStateToProps)(withRouter(SideNav))
