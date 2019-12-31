// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'
import {withRouter} from 'react-router'
import {get, isEqual} from 'lodash'

// Components
import {Page} from '@influxdata/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'
import DashboardHeader from 'src/dashboards/components/DashboardHeader'
import DashboardComponent from 'src/dashboards/components/Dashboard'
import ManualRefresh from 'src/shared/components/ManualRefresh'
import {HoverTimeProvider} from 'src/dashboards/utils/hoverTime'
import VariablesControlBar from 'src/dashboards/components/variablesControlBar/VariablesControlBar'
import LimitChecker from 'src/cloud/components/LimitChecker'
import RateLimitAlert from 'src/cloud/components/RateLimitAlert'

// Actions
import * as dashboardActions from 'src/dashboards/actions'
import * as rangesActions from 'src/dashboards/actions/ranges'
import * as appActions from 'src/shared/actions/app'
import {
  setAutoRefreshInterval,
  setAutoRefreshStatus,
} from 'src/shared/actions/autoRefresh'
import {toggleShowVariablesControls} from 'src/userSettings/actions'

// Utils
import {GlobalAutoRefresher} from 'src/utils/AutoRefresher'
import {
  extractRateLimitResources,
  extractRateLimitStatus,
} from 'src/cloud/utils/limits'
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

// Constants
import {AUTOREFRESH_DEFAULT} from 'src/shared/constants'

// Selectors
import {getTimeRangeByDashboardID} from 'src/dashboards/selectors'
import {getOrg} from 'src/organizations/selectors'

// Types
import {
  Links,
  Dashboard,
  Cell,
  View,
  TimeRange,
  AppState,
  AutoRefresh,
  AutoRefreshStatus,
  Organization,
  RemoteDataState,
} from 'src/types'
import {WithRouterProps} from 'react-router'
import {ManualRefreshProps} from 'src/shared/components/ManualRefresh'
import {Location} from 'history'
import * as AppActions from 'src/types/actions/app'
import * as ColorsModels from 'src/types/colors'
import {LimitStatus} from 'src/cloud/actions/limits'

interface StateProps {
  limitedResources: string[]
  limitStatus: LimitStatus
  org: Organization
  links: Links
  timeRange: TimeRange
  dashboard: Dashboard
  autoRefresh: AutoRefresh
  showVariablesControls: boolean
  views: {[cellID: string]: {view: View; status: RemoteDataState}}
}

interface DispatchProps {
  deleteCell: typeof dashboardActions.deleteCellAsync
  copyCell: typeof dashboardActions.copyDashboardCellAsync
  getDashboard: typeof dashboardActions.getDashboardAsync
  updateDashboard: typeof dashboardActions.updateDashboardAsync
  updateCells: typeof dashboardActions.updateCellsAsync
  updateQueryParams: typeof rangesActions.updateQueryParams
  setDashboardTimeRange: typeof rangesActions.setDashboardTimeRange
  handleChooseAutoRefresh: typeof setAutoRefreshInterval
  onSetAutoRefreshStatus: typeof setAutoRefreshStatus
  handleClickPresentationButton: AppActions.DelayEnablePresentationModeDispatcher
  onCreateCellWithView: typeof dashboardActions.createCellWithView
  onUpdateView: typeof dashboardActions.updateView
  onToggleShowVariablesControls: typeof toggleShowVariablesControls
}

interface PassedProps {
  params: {
    dashboardID: string
  }
  location: Location
  cellQueryStatus: {
    queryID: string
    status: object
  }
  thresholdsListType: string
  thresholdsListColors: ColorsModels.Color[]
  gaugeColors: ColorsModels.Color[]
  lineColors: ColorsModels.Color[]
}

type Props = PassedProps &
  StateProps &
  DispatchProps &
  ManualRefreshProps &
  WithRouterProps

@ErrorHandling
class DashboardPage extends Component<Props> {
  public componentDidMount() {
    const {autoRefresh} = this.props

    if (autoRefresh.status === AutoRefreshStatus.Active) {
      GlobalAutoRefresher.poll(autoRefresh.interval)
    }

    this.getDashboard()
  }

  public componentDidUpdate(prevProps: Props) {
    const {autoRefresh} = this.props

    if (!isEqual(autoRefresh, prevProps.autoRefresh)) {
      if (autoRefresh.status === AutoRefreshStatus.Active) {
        GlobalAutoRefresher.poll(autoRefresh.interval)
        return
      }

      GlobalAutoRefresher.stopPolling()
    }
  }

  public componentWillUnmount() {
    GlobalAutoRefresher.stopPolling()
  }

  public render() {
    const {
      org,
      timeRange,
      dashboard,
      autoRefresh,
      limitStatus,
      limitedResources,
      manualRefresh,
      onManualRefresh,
      showVariablesControls,
      handleClickPresentationButton,
      onToggleShowVariablesControls,
      children,
    } = this.props

    return (
      <Page titleTag={this.pageTitle}>
        <LimitChecker>
          <HoverTimeProvider>
            <DashboardHeader
              org={org}
              dashboard={dashboard}
              timeRange={timeRange}
              autoRefresh={autoRefresh}
              onAddCell={this.handleAddCell}
              onAddNote={this.showNoteOverlay}
              onManualRefresh={onManualRefresh}
              onRenameDashboard={this.handleRenameDashboard}
              activeDashboard={dashboard ? dashboard.name : ''}
              handleChooseAutoRefresh={this.handleChooseAutoRefresh}
              onSetAutoRefreshStatus={this.handleSetAutoRefreshStatus}
              handleChooseTimeRange={this.handleChooseTimeRange}
              handleClickPresentationButton={handleClickPresentationButton}
              toggleVariablesControlBar={onToggleShowVariablesControls}
              isShowingVariablesControlBar={showVariablesControls}
            />
            <RateLimitAlert
              resources={limitedResources}
              limitStatus={limitStatus}
              className="dashboard--rate-alert"
            />
            {showVariablesControls && !!dashboard && (
              <VariablesControlBar dashboardID={dashboard.id} />
            )}
            {!!dashboard && (
              <DashboardComponent
                dashboard={dashboard}
                timeRange={timeRange}
                manualRefresh={manualRefresh}
                onCloneCell={this.handleCloneCell}
                onPositionChange={this.handlePositionChange}
                onDeleteCell={this.handleDeleteDashboardCell}
                onEditView={this.handleEditView}
                onAddCell={this.handleAddCell}
                onEditNote={this.showNoteOverlay}
              />
            )}
            {children}
          </HoverTimeProvider>
        </LimitChecker>
      </Page>
    )
  }

  private getDashboard = () => {
    const {params, getDashboard} = this.props

    getDashboard(params.dashboardID)
  }

  private handleChooseTimeRange = (timeRange: TimeRange): void => {
    const {dashboard, setDashboardTimeRange, updateQueryParams} = this.props
    setDashboardTimeRange(dashboard.id, timeRange)
    updateQueryParams({
      lower: timeRange.lower,
      upper: timeRange.upper,
    })
  }

  private handleSetAutoRefreshStatus = (
    autoRefreshStatus: AutoRefreshStatus
  ) => {
    const {
      onSetAutoRefreshStatus,
      params: {dashboardID},
    } = this.props

    onSetAutoRefreshStatus(dashboardID, autoRefreshStatus)
  }

  private handleChooseAutoRefresh = (autoRefreshInterval: number) => {
    const {
      handleChooseAutoRefresh,
      params: {dashboardID},
    } = this.props

    handleChooseAutoRefresh(dashboardID, autoRefreshInterval)

    if (autoRefreshInterval === 0) {
      this.handleSetAutoRefreshStatus(AutoRefreshStatus.Paused)
      return
    }

    this.handleSetAutoRefreshStatus(AutoRefreshStatus.Active)
  }

  private handlePositionChange = (cells: Cell[]) => {
    const {dashboard, updateCells} = this.props
    updateCells(dashboard, cells)
  }

  private handleAddCell = () => {
    const {router, location} = this.props
    router.push(`${location.pathname}/cells/new`)
  }

  private showNoteOverlay = (id?: string) => {
    if (id) {
      this.props.router.push(`${this.props.location.pathname}/notes/${id}/edit`)
    } else {
      this.props.router.push(`${this.props.location.pathname}/notes/new`)
    }
  }

  private handleEditView = (cellID: string): void => {
    const {router, location} = this.props
    router.push(`${location.pathname}/cells/${cellID}/edit`)
  }

  private handleCloneCell = (cell: Cell) => {
    const {dashboard, onCreateCellWithView, views} = this.props
    const viewEntry = views[cell.id]
    if (viewEntry && viewEntry.view) {
      onCreateCellWithView(dashboard.id, viewEntry.view, cell)
    }
  }

  private handleRenameDashboard = (name: string) => {
    const {dashboard, updateDashboard} = this.props
    const renamedDashboard = {...dashboard, name}

    updateDashboard(renamedDashboard)
  }

  private handleDeleteDashboardCell = (cell: Cell) => {
    const {dashboard, deleteCell} = this.props
    deleteCell(dashboard, cell)
  }

  private get pageTitle(): string {
    const {dashboard} = this.props
    const dashboardName = get(dashboard, 'name', 'Loading...')

    return pageTitleSuffixer([dashboardName])
  }
}

const mstp = (state: AppState, {params: {dashboardID}}): StateProps => {
  const {
    links,
    dashboards,
    views: {views},
    userSettings: {showVariablesControls},
    cloud: {limits},
  } = state

  const org = getOrg(state)

  const timeRange = getTimeRangeByDashboardID(state, dashboardID)

  const autoRefresh = state.autoRefresh[dashboardID] || AUTOREFRESH_DEFAULT

  const dashboard = dashboards.list.find(d => d.id === dashboardID)

  const limitedResources = extractRateLimitResources(limits)
  const limitStatus = extractRateLimitStatus(limits)

  return {
    org,
    links,
    views,
    timeRange,
    dashboard,
    autoRefresh,
    limitStatus,
    limitedResources,
    showVariablesControls,
  }
}

const mdtp: DispatchProps = {
  getDashboard: dashboardActions.getDashboardAsync,
  updateDashboard: dashboardActions.updateDashboardAsync,
  copyCell: dashboardActions.copyDashboardCellAsync,
  deleteCell: dashboardActions.deleteCellAsync,
  updateCells: dashboardActions.updateCellsAsync,
  handleChooseAutoRefresh: setAutoRefreshInterval,
  onSetAutoRefreshStatus: setAutoRefreshStatus,
  handleClickPresentationButton: appActions.delayEnablePresentationMode,
  setDashboardTimeRange: rangesActions.setDashboardTimeRange,
  updateQueryParams: rangesActions.updateQueryParams,
  onCreateCellWithView: dashboardActions.createCellWithView,
  onUpdateView: dashboardActions.updateView,
  onToggleShowVariablesControls: toggleShowVariablesControls,
}

export default connect(
  mstp,
  mdtp
)(ManualRefresh<Props>(withRouter<Props>(DashboardPage)))
