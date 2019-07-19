// Libraries
import React, {Component, MouseEvent} from 'react'
import {connect} from 'react-redux'
import {withRouter} from 'react-router'
import _ from 'lodash'

// Components
import {Page} from 'src/pageLayout'
import {ErrorHandling} from 'src/shared/decorators/errors'
import DashboardHeader from 'src/dashboards/components/DashboardHeader'
import DashboardComponent from 'src/dashboards/components/Dashboard'
import ManualRefresh from 'src/shared/components/ManualRefresh'
import {HoverTimeProvider} from 'src/dashboards/utils/hoverTime'
import VariablesControlBar from 'src/dashboards/components/variablesControlBar/VariablesControlBar'
import LimitChecker from 'src/cloud/components/LimitChecker'
import AssetLimitAlert from 'src/cloud/components/AssetLimitAlert'

// Actions
import * as dashboardActions from 'src/dashboards/actions'
import * as rangesActions from 'src/dashboards/actions/ranges'
import * as appActions from 'src/shared/actions/app'
import {setActiveTimeMachine} from 'src/timeMachine/actions'
import {
  setAutoRefreshInterval,
  setAutoRefreshStatus,
} from 'src/shared/actions/autoRefresh'

// Utils
import {GlobalAutoRefresher} from 'src/utils/AutoRefresher'
import {createView} from 'src/shared/utils/view'
import {
  extractRateLimitResourceName,
  extractRateLimitStatus,
} from 'src/cloud/utils/limits'

// Constants
import {
  DASHBOARD_LAYOUT_ROW_HEIGHT,
  AUTOREFRESH_DEFAULT,
} from 'src/shared/constants'
import {VEO_TIME_MACHINE_ID} from 'src/timeMachine/constants'
import {DEFAULT_TIME_RANGE} from 'src/shared/constants/timeRanges'

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
  XYView,
  ViewType,
  QueryView,
} from 'src/types'
import {RemoteDataState} from 'src/types'
import {WithRouterProps} from 'react-router'
import {ManualRefreshProps} from 'src/shared/components/ManualRefresh'
import {Location} from 'history'
import * as AppActions from 'src/types/actions/app'
import * as ColorsModels from 'src/types/colors'
import {toggleShowVariablesControls} from 'src/userSettings/actions'
import {Organization} from '@influxdata/influx'
import {LimitStatus} from 'src/cloud/actions/limits'

interface StateProps {
  resourceName: string
  limitStatus: LimitStatus
  org: Organization
  links: Links
  zoomedTimeRange: TimeRange
  timeRange: TimeRange
  dashboard: Dashboard
  autoRefresh: AutoRefresh
  inPresentationMode: boolean
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
  setDashTimeV1: typeof rangesActions.setDashTimeV1
  setZoomedTimeRange: typeof rangesActions.setZoomedTimeRange
  handleChooseAutoRefresh: typeof setAutoRefreshInterval
  onSetAutoRefreshStatus: typeof setAutoRefreshStatus
  handleClickPresentationButton: AppActions.DelayEnablePresentationModeDispatcher
  onCreateCellWithView: typeof dashboardActions.createCellWithView
  onUpdateView: typeof dashboardActions.updateView
  onSetActiveTimeMachine: typeof setActiveTimeMachine
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

interface State {
  scrollTop: number
  windowHeight: number
}

@ErrorHandling
class DashboardPage extends Component<Props, State> {
  public constructor(props: Props) {
    super(props)

    this.state = {
      scrollTop: 0,
      windowHeight: window.innerHeight,
    }
  }

  public async componentDidMount() {
    const {autoRefresh} = this.props

    if (autoRefresh.status === AutoRefreshStatus.Active) {
      GlobalAutoRefresher.poll(autoRefresh.interval)
    }

    window.addEventListener('resize', this.handleWindowResize, true)

    await this.getDashboard()
  }

  public componentDidUpdate(prevProps: Props) {
    const {autoRefresh} = this.props

    if (!_.isEqual(autoRefresh, prevProps.autoRefresh)) {
      if (autoRefresh.status === AutoRefreshStatus.Active) {
        GlobalAutoRefresher.poll(autoRefresh.interval)
        return
      }

      GlobalAutoRefresher.stopPolling()
    }
  }

  public componentWillUnmount() {
    GlobalAutoRefresher.stopPolling()

    window.removeEventListener('resize', this.handleWindowResize, true)
  }

  public render() {
    const {
      org,
      timeRange,
      zoomedTimeRange,
      dashboard,
      autoRefresh,
      limitStatus,
      resourceName,
      manualRefresh,
      onManualRefresh,
      inPresentationMode,
      showVariablesControls,
      handleClickPresentationButton,
      onToggleShowVariablesControls,
      children,
    } = this.props

    return (
      <Page titleTag={this.pageTitle}>
        <LimitChecker>
          <AssetLimitAlert
            resourceName={resourceName}
            limitStatus={limitStatus}
          />
          <HoverTimeProvider>
            <DashboardHeader
              org={org}
              dashboard={dashboard}
              timeRange={timeRange}
              autoRefresh={autoRefresh}
              isHidden={inPresentationMode}
              onAddCell={this.handleAddCell}
              onAddNote={this.showNoteOverlay}
              onManualRefresh={onManualRefresh}
              zoomedTimeRange={zoomedTimeRange}
              onRenameDashboard={this.handleRenameDashboard}
              activeDashboard={dashboard ? dashboard.name : ''}
              handleChooseAutoRefresh={this.handleChooseAutoRefresh}
              onSetAutoRefreshStatus={this.handleSetAutoRefreshStatus}
              handleChooseTimeRange={this.handleChooseTimeRange}
              handleClickPresentationButton={handleClickPresentationButton}
              toggleVariablesControlBar={onToggleShowVariablesControls}
              isShowingVariablesControlBar={showVariablesControls}
            />
            {showVariablesControls && !!dashboard && (
              <VariablesControlBar dashboardID={dashboard.id} />
            )}
            {!!dashboard && (
              <DashboardComponent
                inView={this.inView}
                dashboard={dashboard}
                timeRange={timeRange}
                manualRefresh={manualRefresh}
                setScrollTop={this.setScrollTop}
                onCloneCell={this.handleCloneCell}
                inPresentationMode={inPresentationMode}
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

  private getDashboard = async () => {
    const {params, getDashboard} = this.props

    await getDashboard(params.dashboardID)
  }

  private inView = (cell: Cell): boolean => {
    const {scrollTop, windowHeight} = this.state
    const bufferValue = 600
    const cellTop = cell.y * DASHBOARD_LAYOUT_ROW_HEIGHT
    const cellBottom = (cell.y + cell.h) * DASHBOARD_LAYOUT_ROW_HEIGHT
    const bufferedWindowBottom = windowHeight + scrollTop + bufferValue
    const bufferedWindowTop = scrollTop - bufferValue
    const topInView = cellTop < bufferedWindowBottom
    const bottomInView = cellBottom > bufferedWindowTop

    return topInView && bottomInView
  }

  private handleChooseTimeRange = (timeRange: TimeRange): void => {
    const {dashboard, setDashTimeV1, updateQueryParams} = this.props
    setDashTimeV1(dashboard.id, {...timeRange})

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

  private handlePositionChange = async (cells: Cell[]): Promise<void> => {
    const {dashboard, updateCells} = this.props
    await updateCells(dashboard, cells)
  }

  private handleAddCell = async (): Promise<void> => {
    this.showVEO()
  }

  private showNoteOverlay = async (id?: string): Promise<void> => {
    if (id) {
      this.props.router.push(`${this.props.location.pathname}/notes/${id}/edit`)
    } else {
      this.props.router.push(`${this.props.location.pathname}/notes/new`)
    }
  }

  private handleEditView = (cellID: string): void => {
    this.showVEO(cellID)
  }

  private showVEO = (id?: string): void => {
    const {router, location, views, onSetActiveTimeMachine} = this.props
    if (id) {
      const view = _.get(views, `${id}.view`) as QueryView
      onSetActiveTimeMachine(VEO_TIME_MACHINE_ID, {view})
      router.push(`${location.pathname}/cells/${id}/edit`)
    } else {
      const view = createView<XYView>(ViewType.XY)
      onSetActiveTimeMachine(VEO_TIME_MACHINE_ID, {view})
      router.push(`${location.pathname}/cells/new`)
    }
  }

  private handleCloneCell = async (cell: Cell): Promise<void> => {
    const {dashboard, onCreateCellWithView, views} = this.props
    const viewEntry = views[cell.id]
    if (viewEntry && viewEntry.view) {
      await onCreateCellWithView(dashboard, viewEntry.view, cell)
    }
  }

  private handleRenameDashboard = async (name: string): Promise<void> => {
    const {dashboard, updateDashboard} = this.props
    const renamedDashboard = {...dashboard, name}

    await updateDashboard(renamedDashboard)
  }

  private handleDeleteDashboardCell = async (cell: Cell): Promise<void> => {
    const {dashboard, deleteCell} = this.props
    await deleteCell(dashboard, cell)
  }

  private setScrollTop = (e: MouseEvent<HTMLElement>): void => {
    const target = e.target as HTMLElement

    this.setState({scrollTop: target.scrollTop})
  }

  private handleWindowResize = (): void => {
    this.setState({windowHeight: window.innerHeight})
  }

  private get pageTitle(): string {
    const {dashboard} = this.props

    return _.get(dashboard, 'name', 'Loading...')
  }
}

const mstp = (state: AppState, {params: {dashboardID}}): StateProps => {
  const {
    links,
    app: {
      ephemeral: {inPresentationMode},
    },
    ranges,
    dashboards,
    views: {views},
    userSettings: {showVariablesControls},
    orgs: {org},
    cloud: {limits},
  } = state

  const timeRange =
    ranges.find(r => r.dashboardID === dashboardID) || DEFAULT_TIME_RANGE

  const autoRefresh = state.autoRefresh[dashboardID] || AUTOREFRESH_DEFAULT

  const dashboard = dashboards.list.find(d => d.id === dashboardID)

  const resourceName = extractRateLimitResourceName(limits)
  const limitStatus = extractRateLimitStatus(limits)

  return {
    org,
    links,
    views,
    zoomedTimeRange: {lower: null, upper: null},
    timeRange,
    dashboard,
    autoRefresh,
    limitStatus,
    resourceName,
    inPresentationMode,
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
  setDashTimeV1: rangesActions.setDashTimeV1,
  updateQueryParams: rangesActions.updateQueryParams,
  setZoomedTimeRange: rangesActions.setZoomedTimeRange,
  onCreateCellWithView: dashboardActions.createCellWithView,
  onUpdateView: dashboardActions.updateView,
  onSetActiveTimeMachine: setActiveTimeMachine,
  onToggleShowVariablesControls: toggleShowVariablesControls,
}

export default connect(
  mstp,
  mdtp
)(ManualRefresh<Props>(withRouter<Props>(DashboardPage)))
