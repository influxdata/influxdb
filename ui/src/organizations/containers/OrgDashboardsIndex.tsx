// Libraries
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
import Dashboards from 'src/organizations/components/Dashboards'

//Actions
import * as NotificationsActions from 'src/types/actions/notifications'
import * as notifyActions from 'src/shared/actions/notifications'
import {
  getDashboards as getDashboardsAction,
  populateDashboards as populateDashboardsAction,
} from 'src/organizations/actions/orgView'

// Types
import {Organization} from '@influxdata/influx'
import {AppState, Dashboard} from 'src/types'
import {RemoteDataState} from 'src/types'

interface RouterProps {
  params: {
    orgID: string
  }
}

interface DispatchProps {
  notify: NotificationsActions.PublishNotificationActionCreator
  getDashboards: typeof getDashboardsAction
  populateDashboards: typeof populateDashboardsAction
}

interface StateProps {
  org: Organization
  dashboards: Dashboard[]
}

type Props = WithRouterProps & RouterProps & DispatchProps & StateProps

interface State {
  loadingState: RemoteDataState
}

@ErrorHandling
class OrgDashboardsIndex extends Component<Props, State> {
  public state = {
    loadingState: RemoteDataState.NotStarted,
  }

  public componentDidMount = async () => {
    this.setState({loadingState: RemoteDataState.Loading})

    const {getDashboards, org} = this.props

    await getDashboards(org.id)

    this.setState({loadingState: RemoteDataState.Done})
  }

  public componentWillUnmount = async () => {
    const {populateDashboards} = this.props
    populateDashboards([])
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
                <OrganizationNavigation tab="dashboards" orgID={org.id} />
                <Tabs.TabContents>
                  <TabbedPageSection
                    id="org-view-tab--dashboards"
                    url="dashboards"
                    title="Dashboards"
                  >
                    {this.orgsDashboardsPage}
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

  private get orgsDashboardsPage() {
    const {org, dashboards} = this.props
    const {loadingState} = this.state
    return (
      <SpinnerContainer
        loading={loadingState}
        spinnerComponent={<TechnoSpinner />}
      >
        <Dashboards
          dashboards={dashboards}
          onChange={this.getDashboards}
          orgID={org.id}
        />
      </SpinnerContainer>
    )
  }

  private getDashboards = async () => {
    const {getDashboards, org} = this.props

    await getDashboards(org.id)
  }
}

const mstp = (state: AppState, props: Props): StateProps => {
  const {
    orgs,
    orgView: {dashboards},
  } = state

  const org = orgs.find(o => o.id === props.params.orgID)

  return {
    org,
    dashboards,
  }
}

const mdtp: DispatchProps = {
  notify: notifyActions.notify,
  getDashboards: getDashboardsAction,
  populateDashboards: populateDashboardsAction,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter<{}>(OrgDashboardsIndex))
