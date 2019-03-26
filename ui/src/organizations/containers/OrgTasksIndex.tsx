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
import OrgTasksPage from 'src/organizations/components/OrgTasksPage'

//Actions
import {getTasks as getTasksAction} from 'src/organizations/actions/orgView'

// Types
import {Organization} from '@influxdata/influx'
import {AppState, Task} from 'src/types'
import {RemoteDataState} from 'src/types'

interface RouterProps {
  params: {
    orgID: string
  }
}

interface DispatchProps {
  getTasks: typeof getTasksAction
}

interface StateProps {
  org: Organization
  tasks: Task[]
}

type Props = WithRouterProps & RouterProps & DispatchProps & StateProps

interface State {
  loadingState: RemoteDataState
}

@ErrorHandling
class OrgTasksIndex extends Component<Props, State> {
  public state = {
    loadingState: RemoteDataState.NotStarted,
  }

  public componentDidMount = async () => {
    this.setState({loadingState: RemoteDataState.Loading})

    const {getTasks, org} = this.props

    await getTasks(org.id)

    this.setState({loadingState: RemoteDataState.Done})
  }

  public render() {
    const {org} = this.props

    return (
      <>
        <Page titleTag={org.name}>
          <OrgHeader orgID={org.id} />
          <Page.Contents fullWidth={false} scrollable={true}>
            <div className="col-xs-12">
              <Tabs>
                <OrganizationNavigation tab="tasks" orgID={org.id} />
                <Tabs.TabContents>
                  <TabbedPageSection
                    id="org-view-tab--tasks"
                    url="tasks"
                    title="Tasks"
                  >
                    {this.tasksPage}
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

  private get tasksPage() {
    const {org, tasks, router} = this.props
    const {loadingState} = this.state
    return (
      <SpinnerContainer
        loading={loadingState}
        spinnerComponent={<TechnoSpinner />}
      >
        <OrgTasksPage
          tasks={tasks}
          orgName={org.name}
          orgID={org.id}
          onChange={this.updateTasks}
          router={router}
        />
      </SpinnerContainer>
    )
  }

  private updateTasks = () => {
    const {getTasks, org} = this.props

    getTasks(org.id)
  }
}

const mstp = (state: AppState, props: Props): StateProps => {
  const {
    orgs,
    orgView: {tasks},
  } = state

  const org = orgs.find(o => o.id === props.params.orgID)

  return {
    org,
    tasks,
  }
}

const mdtp: DispatchProps = {
  getTasks: getTasksAction,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter<{}>(OrgTasksIndex))
