// Libraries
import React, {PureComponent} from 'react'
import {InjectedRouter} from 'react-router'
import {connect} from 'react-redux'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Components
import DashboardsIndexContents from 'src/dashboards/components/dashboard_index/DashboardsIndexContents'
import {Page} from 'src/pageLayout'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import AddResourceDropdown from 'src/shared/components/AddResourceDropdown'
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'
import GetAssetLimits from 'src/cloud/components/GetAssetLimits'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'

// APIs
import {createDashboard, cloneDashboard} from 'src/dashboards/apis/'

// Actions
import {
  deleteDashboardAsync,
  updateDashboardAsync,
} from 'src/dashboards/actions'
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {checkDashboardLimits as checkDashboardLimitsAction} from 'src/cloud/actions/limits'

// Constants
import {DEFAULT_DASHBOARD_NAME} from 'src/dashboards/constants/index'
import {dashboardCreateFailed} from 'src/shared/copy/notifications'

// Types
import {Dashboard, AppState} from 'src/types'
import {LimitStatus} from 'src/cloud/actions/limits'
import {ComponentStatus} from 'src/clockface'

interface DispatchProps {
  handleDeleteDashboard: typeof deleteDashboardAsync
  handleUpdateDashboard: typeof updateDashboardAsync
  checkDashboardLimits: typeof checkDashboardLimitsAction
  notify: typeof notifyAction
}

interface StateProps {
  dashboards: Dashboard[]
  limitStatus: LimitStatus
}

interface OwnProps {
  router: InjectedRouter
  params: {orgID: string}
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

  public render() {
    const {handleUpdateDashboard, handleDeleteDashboard} = this.props
    const {searchTerm} = this.state

    return (
      <>
        <Page titleTag="Dashboards">
          <Page.Header fullWidth={false}>
            <Page.Header.Left>
              <PageTitleWithOrg title="Dashboards" />
            </Page.Header.Left>
            <Page.Header.Right>
              <AddResourceDropdown
                onSelectNew={this.handleCreateDashboard}
                onSelectImport={this.summonImportOverlay}
                onSelectTemplate={this.summonImportFromTemplateOverlay}
                resourceName="Dashboard"
                canImportFromTemplate={true}
                status={this.addResourceStatus}
              />
            </Page.Header.Right>
          </Page.Header>
          <Page.Contents fullWidth={false} scrollable={true}>
            <div className="col-md-12">
              <GetResources resource={ResourceTypes.Dashboards}>
                <GetResources resource={ResourceTypes.Labels}>
                  <GetAssetLimits>
                    <DashboardsIndexContents
                      filterComponent={() => (
                        <SearchWidget
                          placeholderText="Filter dashboards..."
                          onSearch={this.handleFilterDashboards}
                          searchTerm={searchTerm}
                        />
                      )}
                      onDeleteDashboard={handleDeleteDashboard}
                      onCreateDashboard={this.handleCreateDashboard}
                      onCloneDashboard={this.handleCloneDashboard}
                      onUpdateDashboard={handleUpdateDashboard}
                      searchTerm={searchTerm}
                      onFilterChange={this.handleFilterDashboards}
                      onImportDashboard={this.summonImportOverlay}
                    />
                  </GetAssetLimits>
                </GetResources>
              </GetResources>
            </div>
          </Page.Contents>
        </Page>
        {this.props.children}
      </>
    )
  }

  private handleCreateDashboard = async (): Promise<void> => {
    const {
      router,
      notify,
      params: {orgID},
      checkDashboardLimits,
    } = this.props
    try {
      const newDashboard = {
        name: DEFAULT_DASHBOARD_NAME,
        cells: [],
        orgID,
      }
      const data = await createDashboard(newDashboard)
      checkDashboardLimits()
      router.push(`/orgs/${orgID}/dashboards/${data.id}`)
    } catch (error) {
      notify(dashboardCreateFailed())
    }
  }

  private handleCloneDashboard = async (
    dashboard: Dashboard
  ): Promise<void> => {
    const {
      router,
      notify,
      dashboards,
      params: {orgID},
      checkDashboardLimits,
    } = this.props
    try {
      const data = await cloneDashboard(
        {
          ...dashboard,
          orgID,
        },
        dashboards,
        orgID
      )
      router.push(`/orgs/${orgID}/dashboards/${data.id}`)
      checkDashboardLimits()
    } catch (error) {
      console.error(error)
      notify(dashboardCreateFailed())
    }
  }

  private handleFilterDashboards = (searchTerm: string): void => {
    this.setState({searchTerm})
  }

  private summonImportOverlay = (): void => {
    const {
      router,
      params: {orgID},
    } = this.props
    router.push(`/orgs/${orgID}/dashboards/import`)
  }

  private summonImportFromTemplateOverlay = (): void => {
    const {
      router,
      params: {orgID},
    } = this.props
    router.push(`/orgs/${orgID}/dashboards/import/template`)
  }

  private get addResourceStatus(): ComponentStatus {
    const {limitStatus} = this.props
    if (limitStatus === LimitStatus.EXCEEDED) {
      return ComponentStatus.Disabled
    } else {
      return ComponentStatus.Default
    }
  }
}

const mstp = (state: AppState): StateProps => {
  const {
    dashboards: {list: dashboards},
    cloud: {
      limits: {
        dashboards: {limitStatus},
      },
    },
  } = state

  return {
    dashboards,
    limitStatus,
  }
}

const mdtp: DispatchProps = {
  notify: notifyAction,
  handleDeleteDashboard: deleteDashboardAsync,
  handleUpdateDashboard: updateDashboardAsync,
  checkDashboardLimits: checkDashboardLimitsAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(DashboardIndex)
