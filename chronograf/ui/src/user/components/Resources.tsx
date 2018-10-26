// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import Support from 'src/user/components/Support'
import Settings from 'src/user/components/Settings'
import OrgsList from 'src/user/components/OrgsList'
import DashboardsList from 'src/user/components/DashboardsList'
import ResourceFetcher from 'src/shared/components/resource_fetcher'
import {Panel, Spinner} from 'src/clockface'

// APIs
import {getOrganizations, getDashboards} from 'src/organizations/apis'

// Types
import {Links, User, Organization, Dashboard, AppState} from 'src/types/v2'

const VERSION = process.env.npm_package_version

interface OwnProps {
  user: User
}

interface StateProps {
  links: Links
}

type Props = OwnProps & StateProps

class ResourceLists extends PureComponent<Props> {
  public render() {
    const {links} = this.props

    return (
      <>
        <Panel>
          <Panel.Header title="User Settings">
            <Settings signOutLink={links.signout} />
          </Panel.Header>
          <Panel.Body>
            <span>Your Settings Here</span>
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

const mstp = ({links}: AppState): StateProps => ({
  links,
})

export default connect<StateProps>(mstp)(ResourceLists)
