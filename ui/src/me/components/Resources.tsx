// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'

// Components
import Support from 'src/me/components/Support'
import LogoutButton from 'src/me/components/LogoutButton'
import OrgsList from 'src/me/components/OrgsList'
import DashboardsList from 'src/me/components/DashboardsList'
import ResourceFetcher from 'src/shared/components/resource_fetcher'
import {Panel} from 'src/clockface'
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import VersionInfo from 'src/shared/components/VersionInfo'

// APIs
import {getDashboards} from 'src/organizations/apis'
import {client} from 'src/utils/api'

// Types
import {Dashboard, MeState} from 'src/types/v2'
import {Organization} from '@influxdata/influx'

interface Props {
  me: MeState
}

const getOrganizations = () => client.organizations.getAll()

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
                <Link to={`/configuration/settings_tab`}>Profile</Link>
              </li>
              <li>
                <Link to={`/configuration/tokens_tab`}>Tokens</Link>
              </li>
            </ul>
          </Panel.Body>
        </Panel>
        <Panel>
          <Panel.Header title="Organizations" />
          <Panel.Body>
            <ResourceFetcher<Organization[]> fetcher={getOrganizations}>
              {(orgs, loading) => (
                <SpinnerContainer
                  loading={loading}
                  spinnerComponent={<TechnoSpinner diameterPixels={50} />}
                >
                  <OrgsList orgs={orgs} />
                </SpinnerContainer>
              )}
            </ResourceFetcher>
          </Panel.Body>
        </Panel>
        <Panel>
          <Panel.Header title="Dashboards" />
          <Panel.Body>
            <ResourceFetcher<Dashboard[]> fetcher={getDashboards}>
              {(dashboards, loading) => (
                <SpinnerContainer
                  loading={loading}
                  spinnerComponent={<TechnoSpinner diameterPixels={50} />}
                >
                  <DashboardsList dashboards={dashboards} />
                </SpinnerContainer>
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
            <VersionInfo />
          </Panel.Footer>
        </Panel>
      </>
    )
  }
}

export default ResourceLists
