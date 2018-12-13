// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'

// Components
import Support from 'src/me/components/Support'
import LogoutButton from 'src/me/components/LogoutButton'
import OrgsList from 'src/me/components/OrgsList'
import DashboardsList from 'src/me/components/DashboardsList'
import ResourceFetcher from 'src/shared/components/resource_fetcher'
import {Panel, Spinner} from 'src/clockface'

// APIs
import {getOrganizations, getDashboards} from 'src/organizations/apis'

// Types
import {Links, Organization, Dashboard, MeState} from 'src/types/v2'

const VERSION = process.env.npm_package_version

interface Props {
  me: MeState
  links: Links
}

class ResourceLists extends PureComponent<Props> {
  public render() {
    const {links} = this.props

    return (
      <>
        <Panel>
          <Panel.Header title="My Settings">
            <LogoutButton />
          </Panel.Header>
          <Panel.Body>
            <ul className="link-list">
              <li>
                <Link to={`/account/settings`}>Account Settings</Link>
              </li>
              <li>
                <Link to={`/account/tokens`}>Tokens</Link>
              </li>
            </ul>
          </Panel.Body>
        </Panel>
        <Panel>
          <Panel.Header title="Organizations" />
          <Panel.Body>
            <ResourceFetcher<Organization[]>
              link={links.orgs}
              fetcher={getOrganizations}
            >
              {(orgs, loading) => (
                <Spinner loading={loading}>
                  <OrgsList orgs={orgs} />
                </Spinner>
              )}
            </ResourceFetcher>
          </Panel.Body>
        </Panel>
        <Panel>
          <Panel.Header title="Dashboards" />
          <Panel.Body>
            <ResourceFetcher<Dashboard[]>
              link={links.dashboards}
              fetcher={getDashboards}
            >
              {(dashboards, loading) => (
                <Spinner loading={loading}>
                  <DashboardsList dashboards={dashboards} />
                </Spinner>
              )}
            </ResourceFetcher>
          </Panel.Body>
        </Panel>
        <Panel>
          <Panel.Header title="Useful Links" />
          <Panel.Body>
            <Support />
          </Panel.Body>
          <Panel.Footer>
            <p>Version {VERSION}</p>
          </Panel.Footer>
        </Panel>
      </>
    )
  }
}

export default ResourceLists
