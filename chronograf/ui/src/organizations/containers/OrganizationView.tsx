// Libraries
import React, {PureComponent} from 'react'
import {WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// APIs
import {
  getBuckets,
  getDashboards,
  getMembers,
  getTasks,
} from 'src/organizations/apis'

// Actions
import {updateOrg} from 'src/organizations/actions'

// Components
import {Page} from 'src/pageLayout'
import {Spinner} from 'src/clockface'
import ProfilePage from 'src/shared/components/profile_page/ProfilePage'
import Members from 'src/organizations/components/Members'
import Buckets from 'src/organizations/components/Buckets'
import Dashboards from 'src/organizations/components/Dashboards'
import Tasks from 'src/organizations/components/Tasks'
import OrgOptions from 'src/organizations/components/OrgOptions'
import GetOrgResources from 'src/organizations/components/GetOrgResources'

// Types
import {
  Organization,
  AppState,
  Bucket,
  Dashboard,
  Member,
  Task,
} from 'src/types/v2'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface StateProps {
  org: Organization
}

interface DispatchProps {
  onUpdateOrg: typeof updateOrg
}

type Props = StateProps & WithRouterProps & DispatchProps

@ErrorHandling
class OrganizationView extends PureComponent<Props> {
  public render() {
    const {org, params, onUpdateOrg} = this.props

    return (
      <Page>
        <Page.Header fullWidth={false}>
          <Page.Header.Left>
            <Page.Title title="Organization" />
          </Page.Header.Left>
          <Page.Header.Right />
        </Page.Header>
        <Page.Contents fullWidth={false} scrollable={true}>
          <div className="col-xs-12">
            <ProfilePage
              name={org.name}
              parentUrl={`/organizations/${org.id}`}
              activeTabUrl={params.tab}
            >
              <ProfilePage.Section
                id="org-view-tab--members"
                url="members_tab"
                title="Members"
              >
                <GetOrgResources<Member[]>
                  link={org.links.members}
                  fetcher={getMembers}
                >
                  {(members, loading) => (
                    <Spinner loading={loading}>
                      <Members members={members} />
                    </Spinner>
                  )}
                </GetOrgResources>
              </ProfilePage.Section>
              <ProfilePage.Section
                id="org-view-tab--buckets"
                url="buckets_tab"
                title="Buckets"
              >
                <GetOrgResources<Bucket[]>
                  link={org.links.buckets}
                  fetcher={getBuckets}
                >
                  {(buckets, loading) => (
                    <Spinner loading={loading}>
                      <Buckets buckets={buckets} org={org} />
                    </Spinner>
                  )}
                </GetOrgResources>
              </ProfilePage.Section>
              <ProfilePage.Section
                id="org-view-tab--dashboards"
                url="dashboards_tab"
                title="Dashboards"
              >
                <GetOrgResources<Dashboard[]>
                  link={org.links.dashboards}
                  fetcher={getDashboards}
                >
                  {(dashboards, loading) => (
                    <Spinner loading={loading}>
                      <Dashboards dashboards={dashboards} />
                    </Spinner>
                  )}
                </GetOrgResources>
              </ProfilePage.Section>
              <ProfilePage.Section
                id="org-view-tab--tasks"
                url="tasks_tab"
                title="Tasks"
              >
                <GetOrgResources<Task[]>
                  link={org.links.tasks}
                  fetcher={getTasks}
                >
                  {(tasks, loading) => (
                    <Spinner loading={loading}>
                      <Tasks tasks={tasks} />
                    </Spinner>
                  )}
                </GetOrgResources>
              </ProfilePage.Section>
              <ProfilePage.Section
                id="org-view-tab--options"
                url="options_tab"
                title="Options"
              >
                <OrgOptions org={org} onUpdateOrg={onUpdateOrg} />
              </ProfilePage.Section>
            </ProfilePage>
          </div>
        </Page.Contents>
      </Page>
    )
  }
}

const mstp = (state: AppState, props: WithRouterProps) => {
  const {orgs} = state
  const org = orgs.find(o => o.id === props.params.orgID)
  return {
    org,
  }
}

const mdtp: DispatchProps = {
  onUpdateOrg: updateOrg,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(OrganizationView)
