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
import {Dashboard, MeState} from 'src/types/v2'
import {Organization} from 'src/api'

const VERSION = process.env.npm_package_version

interface Props {
  me: MeState
}

class ResourceLists extends PureComponent<Props> {
  public render() {
    return (
      <>
        <Panel>
          <Panel.Header title="Account">
            <LogoutButton />
          </Panel.Header>
          <Panel.Body>
            <ul className="link-list">
              <li>
                <Link to={`/configuration/settings_tab`}>My Settings</Link>
              </li>
              <li>
                <Link to={`/configuration/tokens_tab`}>My Tokens</Link>
              </li>
            </ul>
          </Panel.Body>
        </Panel>
        <Panel>
          <Panel.Header title="Organizations" />
          <Panel.Body>
            <ResourceFetcher<Organization[]> fetcher={getOrganizations}>
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
            <ResourceFetcher<Dashboard[]> fetcher={getDashboards}>
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
