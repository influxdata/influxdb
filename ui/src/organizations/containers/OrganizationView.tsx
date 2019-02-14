// Libraries
import React, {PureComponent} from 'react'
import {WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// APIs
import {client} from 'src/utils/api'

const getCollectors = async (org: Organization) => {
  return client.telegrafConfigs.getAllByOrg(org)
}

const getScrapers = async (): Promise<ScraperTargetResponse[]> => {
  return await client.scrapers.getAll()
}

const getBuckets = async (org: Organization): Promise<Bucket[]> => {
  return client.buckets.getAllByOrg(org.name)
}

const getVariables = async (org: Organization): Promise<Variable[]> => {
  return await client.variables.getAllByOrg(org.name)
}

// Actions
import {updateOrg} from 'src/organizations/actions'
import * as notifyActions from 'src/shared/actions/notifications'

// Components
import {Page} from 'src/pageLayout'
import {SpinnerContainer, TechnoSpinner} from 'src/clockface'
import TabbedPageSection from 'src/shared/components/tabbed_page/TabbedPageSection'
import Variables from 'src/organizations/components/Variables'
import OrgTasksPage from 'src/organizations/components/OrgTasksPage'
import Collectors from 'src/organizations/components/Collectors'
import Scrapers from 'src/organizations/components/Scrapers'
import GetOrgResources from 'src/organizations/components/GetOrgResources'
import OrganizationTabs from 'src/organizations/components/OrganizationTabs'
import OrgHeader from 'src/organizations/containers/OrgHeader'

// Types
import {AppState} from 'src/types/v2'
import {
  Bucket,
  Organization,
  Telegraf,
  ScraperTargetResponse,
} from '@influxdata/influx'
import * as NotificationsActions from 'src/types/actions/notifications'
import {Macro as Variable} from '@influxdata/influx'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Task} from 'src/tasks/containers/TasksPage'

interface StateProps {
  org: Organization
}

const getTasks = async (org: Organization): Promise<Task[]> => {
  const tasks = await client.tasks.getAllByOrg(org.name)
  const mappedTasks = tasks.map(task => {
    return {
      ...task,
      organization: org,
    }
  })
  return mappedTasks
}

interface DispatchProps {
  onUpdateOrg: typeof updateOrg
  notify: NotificationsActions.PublishNotificationActionCreator
}

type Props = StateProps & WithRouterProps & DispatchProps

@ErrorHandling
class OrganizationView extends PureComponent<Props> {
  public render() {
    const {org, params, notify, router} = this.props

    return (
      <Page titleTag={org.name}>
        <OrgHeader orgID={org.id} />
        <Page.Contents fullWidth={false} scrollable={true}>
          <div className="col-xs-12">
            <OrganizationTabs
              name={org.name}
              activeTabUrl={params.tab}
              orgID={org.id}
            >
              <TabbedPageSection
                id="org-view-tab--tasks"
                url="tasks_tab"
                title="Tasks"
              >
                <GetOrgResources<Task[]> organization={org} fetcher={getTasks}>
                  {(tasks, loading, fetch) => (
                    <SpinnerContainer
                      loading={loading}
                      spinnerComponent={<TechnoSpinner />}
                    >
                      <OrgTasksPage
                        tasks={tasks}
                        orgName={org.name}
                        orgID={org.id}
                        onChange={fetch}
                        router={router}
                      />
                    </SpinnerContainer>
                  )}
                </GetOrgResources>
              </TabbedPageSection>
              <TabbedPageSection
                id="org-view-tab--telegrafs"
                url="telegrafs_tab"
                title="Telegraf"
              >
                <GetOrgResources<Telegraf[]>
                  organization={org}
                  fetcher={getCollectors}
                >
                  {(collectors, loading, fetch) => (
                    <SpinnerContainer
                      loading={loading}
                      spinnerComponent={<TechnoSpinner />}
                    >
                      <GetOrgResources<Bucket[]>
                        organization={org}
                        fetcher={getBuckets}
                      >
                        {(buckets, loading) => (
                          <SpinnerContainer
                            loading={loading}
                            spinnerComponent={<TechnoSpinner />}
                          >
                            <Collectors
                              collectors={collectors}
                              onChange={fetch}
                              notify={notify}
                              buckets={buckets}
                              orgName={org.name}
                            />
                          </SpinnerContainer>
                        )}
                      </GetOrgResources>
                    </SpinnerContainer>
                  )}
                </GetOrgResources>
              </TabbedPageSection>
              <TabbedPageSection
                id="org-view-tab--scrapers"
                url="scrapers_tab"
                title="Scrapers"
              >
                <GetOrgResources<ScraperTargetResponse[]>
                  organization={org}
                  fetcher={getScrapers}
                >
                  {(scrapers, loading, fetch) => {
                    return (
                      <SpinnerContainer
                        loading={loading}
                        spinnerComponent={<TechnoSpinner />}
                      >
                        <GetOrgResources<Bucket[]>
                          organization={org}
                          fetcher={getBuckets}
                        >
                          {(buckets, loading) => (
                            <SpinnerContainer
                              loading={loading}
                              spinnerComponent={<TechnoSpinner />}
                            >
                              <Scrapers
                                scrapers={scrapers}
                                onChange={fetch}
                                orgName={org.name}
                                buckets={buckets}
                              />
                            </SpinnerContainer>
                          )}
                        </GetOrgResources>
                      </SpinnerContainer>
                    )
                  }}
                </GetOrgResources>
              </TabbedPageSection>
              <TabbedPageSection
                id="org-view-tab--variables"
                url="variables_tab"
                title="Variables"
              >
                <GetOrgResources<Variable[]>
                  organization={org}
                  fetcher={getVariables}
                >
                  {(variables, loading, fetch) => {
                    return (
                      <SpinnerContainer
                        loading={loading}
                        spinnerComponent={<TechnoSpinner />}
                      >
                        <Variables
                          onChange={fetch}
                          variables={variables}
                          orgName={org.name}
                          orgID={org.id}
                          notify={notify}
                        />
                      </SpinnerContainer>
                    )
                  }}
                </GetOrgResources>
              </TabbedPageSection>
            </OrganizationTabs>
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
  notify: notifyActions.notify,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(OrganizationView)
