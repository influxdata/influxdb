import React, {PropTypes} from 'react'
import {withRouter, Link} from 'react-router'
import {connect} from 'react-redux'

import Authorized, {ADMIN_ROLE} from 'src/auth/Authorized'

import UserNavBlock from 'src/side_nav/components/UserNavBlock'
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
  },

  render() {
    const {
      params: {sourceID},
      location: {pathname: location},
      isHidden,
      isUsingAuth,
      logoutLink,
      links,
      me,
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
          {isUsingAuth
            ? <UserNavBlock
                logoutLink={logoutLink}
                links={links}
                me={me}
                sourcePrefix={sourcePrefix}
              />
            : null}
          <NavBlock
            icon="cubo-node"
            link={`${sourcePrefix}/hosts`}
            location={location}
          >
            <NavHeader link={`${sourcePrefix}/hosts`} title="Host List" />
          </NavBlock>
          <NavBlock
            icon="graphline"
            link={dataExplorerLink}
            location={location}
          >
            <NavHeader link={dataExplorerLink} title="Data Explorer" />
          </NavBlock>
          <NavBlock
            icon="dash-h"
            link={`${sourcePrefix}/dashboards`}
            location={location}
          >
            <NavHeader
              link={`${sourcePrefix}/dashboards`}
              title={'Dashboards'}
            />
          </NavBlock>
          <NavBlock
            matcher="alerts"
            icon="alert-triangle"
            link={`${sourcePrefix}/alerts`}
            location={location}
          >
            <NavHeader link={`${sourcePrefix}/alerts`} title="Alerting" />
            <NavListItem link={`${sourcePrefix}/alerts`}>History</NavListItem>
            <NavListItem link={`${sourcePrefix}/alert-rules`}>
              Create
            </NavListItem>
          </NavBlock>

          <Authorized
            requiredRole={ADMIN_ROLE}
            replaceWithIfNotUsingAuth={
              <NavBlock
                icon="crown2"
                link={`${sourcePrefix}/admin-influxdb`}
                location={location}
              >
                <NavHeader
                  link={`${sourcePrefix}/admin-influxdb`}
                  title="Admin"
                />
              </NavBlock>
            }
          >
            <NavBlock
              icon="crown2"
              link={`${sourcePrefix}/admin-chronograf`}
              location={location}
            >
              <NavHeader
                link={`${sourcePrefix}/admin-chronograf`}
                title="Admin"
              />
              <NavListItem link={`${sourcePrefix}/admin-chronograf`}>
                Chronograf
              </NavListItem>
              <NavListItem link={`${sourcePrefix}/admin-influxdb`}>
                InfluxDB
              </NavListItem>
            </NavBlock>
          </Authorized>

          <NavBlock
            icon="cog-thick"
            link={`${sourcePrefix}/manage-sources`}
            location={location}
          >
            <NavHeader
              link={`${sourcePrefix}/manage-sources`}
              title="Configuration"
            />
          </NavBlock>
        </NavBar>
  },
})
const mapStateToProps = ({
  auth: {isUsingAuth, logoutLink, me},
  app: {ephemeral: {inPresentationMode}},
  links,
}) => ({
  isHidden: inPresentationMode,
  isUsingAuth,
  logoutLink,
  links,
  me,
})

export default connect(mapStateToProps)(withRouter(SideNav))
