// Libraries
import React, {PureComponent} from 'react'
import {InjectedRouter} from 'react-router'
import {connect} from 'react-redux'

// Components
import DashboardsIndexContents from 'src/dashboards/components/dashboard_index/DashboardsIndexContents'
import {Page} from 'src/pageLayout'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'

// APIs
import {createDashboard, cloneDashboard} from 'src/dashboards/apis/'

// Actions
import {
  getDashboardsAsync,
  deleteDashboardAsync,
  updateDashboardAsync,
  addDashboardLabelsAsync,
  removeDashboardLabelsAsync,
} from 'src/dashboards/actions'
import {retainRangesDashTimeV1 as retainRangesDashTimeV1Action} from 'src/dashboards/actions/ranges'
import {notify as notifyAction} from 'src/shared/actions/notifications'
import GetResources, {
  ResourceTypes,
} from 'src/configuration/components/GetResources'

// Constants
import {DEFAULT_DASHBOARD_NAME} from 'src/dashboards/constants/index'
import {dashboardCreateFailed} from 'src/shared/copy/notifications'

// Types
import {Notification} from 'src/types/notifications'
import {Links, Dashboard, AppState, Organization} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface DispatchProps {
  handleGetDashboards: typeof getDashboardsAsync
  handleDeleteDashboard: typeof deleteDashboardAsync
  handleUpdateDashboard: typeof updateDashboardAsync
  notify: (message: Notification) => void
  retainRangesDashTimeV1: (dashboardIDs: string[]) => void
  onAddDashboardLabels: typeof addDashboardLabelsAsync
  onRemoveDashboardLabels: typeof removeDashboardLabelsAsync
}

interface StateProps {
  links: Links
  dashboards: Dashboard[]
  orgs: Organization[]
}

interface OwnProps {
  router: InjectedRouter
}

type Props = DispatchProps & StateProps & OwnProps

interface State {
  searchTerm: string
}

@ErrorHandling
class DashboardIndex extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      searchTerm: '',
    }
  }

  public async componentDidMount() {
    const {dashboards} = this.props

    const dashboardIDs = dashboards.map(d => d.id)
    this.props.retainRangesDashTimeV1(dashboardIDs)
  }

  public render() {
    const {dashboards, notify, handleUpdateDashboard} = this.props
    const {searchTerm} = this.state

    return (
      <>
        <Page titleTag="Dashboards">
          <Page.Header fullWidth={false}>
            <Page.Header.Left>
              <Page.Title title="Dashboards" />
            </Page.Header.Left>
            <Page.Header.Right>
              <AddResourceDropdown
                onSelectNew={this.handleCreateDashboard}
                onSelectImport={this.summonImportOverlay}
                resourceName="Dashboard"
              />
            </Page.Header.Right>
          </Page.Header>
          <Page.Contents fullWidth={false} scrollable={true}>
            <div className="col-md-12">
              <GetResources resource={ResourceTypes.Dashboards}>
                <DashboardsIndexContents
                  filterComponent={() => (
                    <SearchWidget
                      placeholderText="Filter dashboards..."
                      onSearch={this.handleFilterDashboards}
                      searchTerm={searchTerm}
                    />
                  )}
                  dashboards={dashboards}
                  onDeleteDashboard={this.handleDeleteDashboard}
                  onCreateDashboard={this.handleCreateDashboard}
                  onCloneDashboard={this.handleCloneDashboard}
                  onUpdateDashboard={handleUpdateDashboard}
                  notify={notify}
                  searchTerm={searchTerm}
                  showOwnerColumn={true}
                  onFilterChange={this.handleFilterDashboards}
                  onImportDashboard={this.summonImportOverlay}
                />
              </GetResources>
            </div>
          </Page.Contents>
        </Page>
        {this.props.children}
      </>
    )
  }

  private handleCreateDashboard = async (): Promise<void> => {
    const {router, notify, orgs} = this.props
    try {
      const newDashboard = {
        name: DEFAULT_DASHBOARD_NAME,
        cells: [],
        orgID: orgs[0].id,
      }
      const data = await createDashboard(newDashboard)
      router.push(`/dashboards/${data.id}`)
    } catch (error) {
      notify(dashboardCreateFailed())
    }
  }

  private handleCloneDashboard = async (
    dashboard: Dashboard
  ): Promise<void> => {
    const {router, notify, orgs, dashboards} = this.props
    try {
      const data = await cloneDashboard(
        {
          ...dashboard,
          orgID: orgs[0].id,
        },
        dashboards
      )
      router.push(`/dashboards/${data.id}`)
    } catch (error) {
      console.error(error)
      notify(dashboardCreateFailed())
    }
  }

  private handleDeleteDashboard = (dashboard: Dashboard) => {
    this.props.handleDeleteDashboard(dashboard)
  }

  private handleFilterDashboards = (searchTerm: string): void => {
    this.setState({searchTerm})
  }

  private summonImportOverlay = (): void => {
    const {router} = this.props
    router.push(`/dashboards/import`)
  }
}

const mstp = (state: AppState): StateProps => {
  const {
    dashboards: {list: dashboards},
    links,
    orgs,
  } = state

  return {
    orgs,
    dashboards,
    links,
  }
}

const mdtp: DispatchProps = {
  notify: notifyAction,
  handleGetDashboards: getDashboardsAsync,
  handleDeleteDashboard: deleteDashboardAsync,
  handleUpdateDashboard: updateDashboardAsync,
  retainRangesDashTimeV1: retainRangesDashTimeV1Action,
  onAddDashboardLabels: addDashboardLabelsAsync,
  onRemoveDashboardLabels: removeDashboardLabelsAsync,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(DashboardIndex)
