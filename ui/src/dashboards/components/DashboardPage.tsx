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

// Actions
import * as dashboardActions from 'src/dashboards/actions'
import * as rangesActions from 'src/dashboards/actions/ranges'
import * as appActions from 'src/shared/actions/app'
import * as notifyActions from 'src/shared/actions/notifications'
import {setActiveTimeMachine} from 'src/timeMachine/actions'

// Utils
import {getDeep} from 'src/utils/wrappers'
import {GlobalAutoRefresher} from 'src/utils/AutoRefresher'

// Constants
import {DASHBOARD_LAYOUT_ROW_HEIGHT} from 'src/shared/constants'
import {DEFAULT_TIME_RANGE} from 'src/shared/constants/timeRanges'

// Types
import {Links, Dashboard, Cell, View, TimeRange, AppState} from 'src/types'
import {RemoteDataState} from 'src/types'
import {WithRouterProps} from 'react-router'
import {ManualRefreshProps} from 'src/shared/components/ManualRefresh'
import {Location} from 'history'
import * as AppActions from 'src/types/actions/app'
import * as ColorsModels from 'src/types/colors'
import * as NotificationsActions from 'src/types/actions/notifications'

interface StateProps {
  links: Links
  zoomedTimeRange: TimeRange
  timeRange: TimeRange
  dashboard: Dashboard
  autoRefresh: number
  inPresentationMode: boolean
  showTemplateControlBar: boolean
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
  handleChooseAutoRefresh: AppActions.SetAutoRefreshActionCreator
  handleClickPresentationButton: AppActions.DelayEnablePresentationModeDispatcher
  notify: NotificationsActions.PublishNotificationActionCreator
  onCreateCellWithView: typeof dashboardActions.createCellWithView
  onUpdateView: typeof dashboardActions.updateView
  onSetActiveTimeMachine: typeof setActiveTimeMachine
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
  isShowingVEO: boolean
  isShowingVariablesControlBar: boolean
}

@ErrorHandling
class DashboardPage extends Component<Props, State> {
  public constructor(props: Props) {
    super(props)

    this.state = {
      scrollTop: 0,
      windowHeight: window.innerHeight,
      isShowingVEO: false,
      isShowingVariablesControlBar: false,
    }
  }

  public async componentDidMount() {
    const {autoRefresh} = this.props

    GlobalAutoRefresher.poll(autoRefresh)

    window.addEventListener('resize', this.handleWindowResize, true)

    await this.getDashboard()
  }

  public componentDidUpdate(prevProps: Props) {
    const {autoRefresh} = this.props

    const prevPath = getDeep(prevProps.location, 'pathname', null)
    const thisPath = getDeep(this.props.location, 'pathname', null)

    if (prevPath && thisPath && prevPath !== thisPath) {
      this.getDashboard()
    }

    if (autoRefresh !== prevProps.autoRefresh) {
      GlobalAutoRefresher.poll(autoRefresh)
    }
  }

  public componentWillUnmount() {
    GlobalAutoRefresher.stopPolling()

    window.removeEventListener('resize', this.handleWindowResize, true)
  }

  public render() {
    const {
      timeRange,
      zoomedTimeRange,
      showTemplateControlBar,
      dashboard,
      autoRefresh,
      manualRefresh,
      onManualRefresh,
      inPresentationMode,
      handleChooseAutoRefresh,
      handleClickPresentationButton,
      children,
    } = this.props
    const {isShowingVariablesControlBar} = this.state

    return (
      <Page titleTag={this.pageTitle}>
        <HoverTimeProvider>
          <DashboardHeader
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
            showTemplateControlBar={showTemplateControlBar}
            handleChooseAutoRefresh={handleChooseAutoRefresh}
            handleChooseTimeRange={this.handleChooseTimeRange}
            handleClickPresentationButton={handleClickPresentationButton}
            toggleVariablesControlBar={this.toggleVariablesControlBar}
            isShowingVariablesControlBar={isShowingVariablesControlBar}
          />
          {isShowingVariablesControlBar && (
            <VariablesControlBar dashboardID={dashboard.id} />
          )}
          {!!dashboard && (
            <DashboardComponent
              inView={this.inView}
              dashboard={dashboard}
              timeRange={timeRange}
              autoRefresh={autoRefresh}
              manualRefresh={manualRefresh}
              setScrollTop={this.setScrollTop}
              onCloneCell={this.handleCloneCell}
              onZoom={this.handleZoomedTimeRange}
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
    if (id) {
      this.props.router.push(`${this.props.location.pathname}/cells/${id}/edit`)
    } else {
      this.props.router.push(`${this.props.location.pathname}/cells/new`)
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

  private handleZoomedTimeRange = (__: TimeRange): void => {}

  private setScrollTop = (e: MouseEvent<HTMLElement>): void => {
    const target = e.target as HTMLElement

    this.setState({scrollTop: target.scrollTop})
  }

  private handleWindowResize = (): void => {
    this.setState({windowHeight: window.innerHeight})
  }

  private toggleVariablesControlBar = (): void => {
    this.setState({
      isShowingVariablesControlBar: !this.state.isShowingVariablesControlBar,
    })
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
      persisted: {autoRefresh, showTemplateControlBar},
    },
    ranges,
    dashboards,
    views: {views},
  } = state

  const timeRange =
    ranges.find(r => r.dashboardID === dashboardID) || DEFAULT_TIME_RANGE

  const dashboard = dashboards.find(d => d.id === dashboardID)

  return {
    links,
    views,
    zoomedTimeRange: {lower: null, upper: null},
    timeRange,
    dashboard,
    autoRefresh,
    inPresentationMode,
    showTemplateControlBar,
  }
}

const mdtp: DispatchProps = {
  getDashboard: dashboardActions.getDashboardAsync,
  updateDashboard: dashboardActions.updateDashboardAsync,
  copyCell: dashboardActions.copyDashboardCellAsync,
  deleteCell: dashboardActions.deleteCellAsync,
  updateCells: dashboardActions.updateCellsAsync,
  handleChooseAutoRefresh: appActions.setAutoRefresh,
  handleClickPresentationButton: appActions.delayEnablePresentationMode,
  notify: notifyActions.notify,
  setDashTimeV1: rangesActions.setDashTimeV1,
  updateQueryParams: rangesActions.updateQueryParams,
  setZoomedTimeRange: rangesActions.setZoomedTimeRange,
  onCreateCellWithView: dashboardActions.createCellWithView,
  onUpdateView: dashboardActions.updateView,
  onSetActiveTimeMachine: setActiveTimeMachine,
}

export default connect(
  mstp,
  mdtp
)(ManualRefresh<Props>(withRouter<Props>(DashboardPage)))
