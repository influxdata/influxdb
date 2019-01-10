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
import TabbedPage from 'src/shared/components/tabbed_page/TabbedPage'
import TabbedPageSection from 'src/shared/components/tabbed_page/TabbedPageSection'
import Members from 'src/organizations/components/Members'
import Buckets from 'src/organizations/components/Buckets'
import Dashboards from 'src/organizations/components/Dashboards'
import Tasks from 'src/organizations/components/Tasks'
import OrgOptions from 'src/organizations/components/OrgOptions'
import GetOrgResources from 'src/organizations/components/GetOrgResources'

// Types
import {AppState, Dashboard} from 'src/types/v2'
import {ResourceOwner, Bucket, Organization, Task} from 'src/api'

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
      <Page titleTag={org.name}>
        <Page.Header fullWidth={false}>
          <Page.Header.Left>
            <Page.Title title={org.name ? org.name : 'Organization'} />
          </Page.Header.Left>
          <Page.Header.Right />
        </Page.Header>
        <Page.Contents fullWidth={false} scrollable={true}>
          <div className="col-xs-12">
            <TabbedPage
              name={org.name}
              parentUrl={`/organizations/${org.id}`}
              activeTabUrl={params.tab}
            >
              <TabbedPageSection
                id="org-view-tab--members"
                url="members_tab"
                title="Members"
              >
                <GetOrgResources<ResourceOwner[]>
                  organization={org}
                  fetcher={getMembers}
                >
                  {(members, loading) => (
                    <Spinner loading={loading}>
                      <Members members={members} orgName={org.name} />
                    </Spinner>
                  )}
                </GetOrgResources>
              </TabbedPageSection>
              <TabbedPageSection
                id="org-view-tab--buckets"
                url="buckets_tab"
                title="Buckets"
              >
                <GetOrgResources<Bucket[]>
                  organization={org}
                  fetcher={getBuckets}
                >
                  {(buckets, loading, fetch) => (
                    <Spinner loading={loading}>
                      <Buckets buckets={buckets} org={org} onChange={fetch} />
                    </Spinner>
                  )}
                </GetOrgResources>
              </TabbedPageSection>
              <TabbedPageSection
                id="org-view-tab--dashboards"
                url="dashboards_tab"
                title="Dashboards"
              >
                <GetOrgResources<Dashboard[]>
                  organization={org}
                  fetcher={getDashboards}
                >
                  {(dashboards, loading) => (
                    <Spinner loading={loading}>
                      <Dashboards dashboards={dashboards} orgName={org.name} />
                    </Spinner>
                  )}
                </GetOrgResources>
              </TabbedPageSection>
              <TabbedPageSection
                id="org-view-tab--tasks"
                url="tasks_tab"
                title="Tasks"
              >
                <GetOrgResources<Task[]> organization={org} fetcher={getTasks}>
                  {(tasks, loading) => (
                    <Spinner loading={loading}>
                      <Tasks tasks={tasks} orgName={org.name} />
                    </Spinner>
                  )}
                </GetOrgResources>
              </TabbedPageSection>
              <TabbedPageSection
                id="org-view-tab--options"
                url="options_tab"
                title="Options"
              >
                <OrgOptions org={org} onUpdateOrg={onUpdateOrg} />
              </TabbedPageSection>
            </TabbedPage>
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
