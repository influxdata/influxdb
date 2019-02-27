import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import OrganizationNavigation from 'src/organizations/components/OrganizationNavigation'
import OrgHeader from 'src/organizations/containers/OrgHeader'
import {Tabs} from 'src/clockface'
import {Page} from 'src/pageLayout'
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import TabbedPageSection from 'src/shared/components/tabbed_page/TabbedPageSection'
import GetOrgResources from 'src/organizations/components/GetOrgResources'
import OrgTasksPage from 'src/organizations/components/OrgTasksPage'

//Actions
import * as NotificationsActions from 'src/types/actions/notifications'
import * as notifyActions from 'src/shared/actions/notifications'

// APIs
import {client} from 'src/utils/api'

// Types
import {Organization} from '@influxdata/influx'
import {AppState} from 'src/types/v2'
import {Task} from 'src/tasks/containers/TasksPage'

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

interface RouterProps {
  params: {
    orgID: string
  }
}

interface DispatchProps {
  notify: NotificationsActions.PublishNotificationActionCreator
}

interface StateProps {
  org: Organization
}

type Props = WithRouterProps & RouterProps & DispatchProps & StateProps

@ErrorHandling
class OrgTasksIndex extends Component<Props> {
  public render() {
    const {org, router} = this.props

    return (
      <>
        <Page titleTag={org.name}>
          <OrgHeader orgID={org.id} />
          <Page.Contents fullWidth={false} scrollable={true}>
            <div className="col-xs-12">
              <Tabs>
                <OrganizationNavigation tab={'tasks'} orgID={org.id} />
                <Tabs.TabContents>
                  <TabbedPageSection
                    id="org-view-tab--tasks"
                    url="tasks"
                    title="Tasks"
                  >
                    <GetOrgResources<Task>
                      organization={org}
                      fetcher={getTasks}
                    >
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
                </Tabs.TabContents>
              </Tabs>
            </div>
          </Page.Contents>
        </Page>
        {this.props.children}
      </>
    )
  }
}

const mstp = (state: AppState, props: Props) => {
  const {orgs} = state
  const org = orgs.find(o => o.id === props.params.orgID)
  return {
    org,
  }
}

const mdtp: DispatchProps = {
  notify: notifyActions.notify,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter<{}>(OrgTasksIndex))
