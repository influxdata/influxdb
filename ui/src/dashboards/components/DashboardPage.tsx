// Libraries
import React, {Component, MouseEvent} from 'react'
import {connect} from 'react-redux'
import {withRouter} from 'react-router'
import {get} from 'lodash'

// Components
import {Page} from 'src/pageLayout'
import {ErrorHandling} from 'src/shared/decorators/errors'
import DashboardHeader from 'src/dashboards/components/DashboardHeader'
import DashboardComponent from 'src/dashboards/components/Dashboard'
import ManualRefresh from 'src/shared/components/ManualRefresh'
import VEO from 'src/dashboards/components/VEO'
import {OverlayTechnology} from 'src/clockface'
import {HoverTimeProvider} from 'src/dashboards/utils/hoverTime'
import NoteEditorContainer from 'src/dashboards/components/NoteEditorContainer'

// Actions
import * as dashboardActions from 'src/dashboards/actions/v2'
import * as rangesActions from 'src/dashboards/actions/v2/ranges'
import * as appActions from 'src/shared/actions/app'
import * as notifyActions from 'src/shared/actions/notifications'
import * as viewActions from 'src/dashboards/actions/v2/views'
import {setActiveTimeMachine} from 'src/shared/actions/v2/timeMachines'

// Utils
import {getDeep} from 'src/utils/wrappers'
import {updateDashboardLinks} from 'src/dashboards/utils/dashboardSwitcherLinks'
import {GlobalAutoRefresher} from 'src/utils/AutoRefresher'
import {createView} from 'src/shared/utils/view'
import {cellAddFailed} from 'src/shared/copy/notifications'

// APIs
import {loadDashboardLinks} from 'src/dashboards/apis/v2'

// Constants
import {DASHBOARD_LAYOUT_ROW_HEIGHT} from 'src/shared/constants'
import {DEFAULT_TIME_RANGE} from 'src/shared/constants/timeRanges'
import {EMPTY_LINKS} from 'src/dashboards/constants/dashboardHeader'
import {VEO_TIME_MACHINE_ID} from 'src/shared/constants/timeMachine'

// Types
import {
  Links,
  Source,
  Dashboard,
  Cell,
  View,
  ViewType,
  TimeRange,
  DashboardSwitcherLinks,
} from 'src/types/v2'
import {NewView, XYView, QueryViewProperties} from 'src/types/v2/dashboards'
import {RemoteDataState} from 'src/types'
import {WithRouterProps} from 'react-router'
import {ManualRefreshProps} from 'src/shared/components/ManualRefresh'
import {Location} from 'history'
import * as AppActions from 'src/types/actions/app'
import * as ColorsModels from 'src/types/colors'
import * as NotificationsActions from 'src/types/actions/notifications'

interface StateProps {
  links: Links
  sources: Source[]
  zoomedTimeRange: TimeRange
  timeRange: TimeRange
  dashboard: Dashboard
  autoRefresh: number
  inPresentationMode: boolean
  showTemplateControlBar: boolean
  views: {[viewID: string]: {view: View; status: RemoteDataState}}
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
  onAddCell: typeof dashboardActions.addCellAsync
  onCreateCellWithView: typeof dashboardActions.createCellWithView
  onUpdateView: typeof viewActions.updateView
  onSetActiveTimeMachine: typeof setActiveTimeMachine
}

interface PassedProps {
  source: Source
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
  dashboardLinks: DashboardSwitcherLinks
  isShowingVEO: boolean
}

@ErrorHandling
class DashboardPage extends Component<Props, State> {
  public constructor(props: Props) {
    super(props)

    this.state = {
      scrollTop: 0,
      windowHeight: window.innerHeight,
      dashboardLinks: EMPTY_LINKS,
      isShowingVEO: false,
    }
  }

  public async componentDidMount() {
    const {autoRefresh} = this.props

    GlobalAutoRefresher.poll(autoRefresh)

    window.addEventListener('resize', this.handleWindowResize, true)

    await this.getDashboard()

    this.getDashboardLinks()
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
      source,
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
    } = this.props
    const {dashboardLinks, isShowingVEO} = this.state

    return (
      <Page>
        <HoverTimeProvider>
          <DashboardHeader
            dashboard={dashboard}
            timeRange={timeRange}
            autoRefresh={autoRefresh}
            isHidden={inPresentationMode}
            onAddCell={this.handleAddCell}
            onManualRefresh={onManualRefresh}
            zoomedTimeRange={zoomedTimeRange}
            onRenameDashboard={this.handleRenameDashboard}
            dashboardLinks={dashboardLinks}
            activeDashboard={dashboard ? dashboard.name : ''}
            showTemplateControlBar={showTemplateControlBar}
            handleChooseAutoRefresh={handleChooseAutoRefresh}
            handleChooseTimeRange={this.handleChooseTimeRange}
            handleClickPresentationButton={handleClickPresentationButton}
          />
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
            />
          )}
          <OverlayTechnology visible={isShowingVEO}>
            <VEO
              source={source}
              onHide={this.handleHideVEO}
              onSave={this.handleSaveVEO}
            />
          </OverlayTechnology>
        </HoverTimeProvider>
        <NoteEditorContainer />
      </Page>
    )
  }

  private getDashboard = async () => {
    const {params, getDashboard} = this.props

    await getDashboard(params.dashboardID)
    this.updateActiveDashboard()
  }

  private updateActiveDashboard(): void {
    this.setState((prevState, props) => ({
      dashboardLinks: updateDashboardLinks(
        prevState.dashboardLinks,
        props.dashboard
      ),
    }))
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
    const newView = createView<XYView>(ViewType.XY)

    this.showVEO(newView)
  }

  private handleHideVEO = (): void => {
    this.setState({isShowingVEO: false})
  }

  private handleSaveVEO = async (view): Promise<void> => {
    this.setState({isShowingVEO: false})

    const {dashboard, onCreateCellWithView, onUpdateView, notify} = this.props

    try {
      if (view.id) {
        await onUpdateView(view.links.self, view)
      } else {
        await onCreateCellWithView(dashboard, view)
      }
    } catch (error) {
      console.error(error)
      notify(cellAddFailed())
    }
  }

  private handleEditView = (viewID: string): void => {
    const entry = this.props.views[viewID]

    if (!entry || !entry.view) {
      throw new Error(`Can't edit non-existant view with ID "${viewID}"`)
    }

    this.showVEO(entry.view as View<QueryViewProperties>)
  }

  private showVEO = (
    view: View<QueryViewProperties> | NewView<QueryViewProperties>
  ): void => {
    const {onSetActiveTimeMachine} = this.props
    const draftScript: string = get(view, 'properties.queries.0.text', '')

    onSetActiveTimeMachine(VEO_TIME_MACHINE_ID, {view, draftScript})

    this.setState({isShowingVEO: true})
  }

  private handleCloneCell = async (cell: Cell): Promise<void> => {
    const {dashboard, copyCell} = this.props
    await copyCell(dashboard, cell)
  }

  private handleRenameDashboard = async (name: string): Promise<void> => {
    const {dashboard, updateDashboard} = this.props
    const renamedDashboard = {...dashboard, name}

    await updateDashboard(renamedDashboard)
    this.updateActiveDashboard()
  }

  private handleDeleteDashboardCell = async (cell: Cell): Promise<void> => {
    const {dashboard, deleteCell} = this.props
    await deleteCell(dashboard, cell)
  }

  private handleZoomedTimeRange = (__: TimeRange): void => {
    // const {setZoomedTimeRange, updateQueryParams} = this.props
    // setZoomedTimeRange(zoomedTimeRange)
    // updateQueryParams({
    //   zoomedLower: zoomedTimeRange.lower,
    //   zoomedUpper: zoomedTimeRange.upper,
    // })
  }

  private setScrollTop = (e: MouseEvent<HTMLElement>): void => {
    const target = e.target as HTMLElement

    this.setState({scrollTop: target.scrollTop})
  }

  private getDashboardLinks = async (): Promise<void> => {
    const {dashboard: activeDashboard} = this.props

    try {
      const dashboardLinks = await loadDashboardLinks(activeDashboard)

      this.setState({
        dashboardLinks,
      })
    } catch (error) {
      console.error(error)
    }
  }

  private handleWindowResize = (): void => {
    this.setState({windowHeight: window.innerHeight})
  }
}

const mstp = (state, {params: {dashboardID}}): StateProps => {
  const {
    links,
    app: {
      ephemeral: {inPresentationMode},
      persisted: {autoRefresh, showTemplateControlBar},
    },
    sources,
    ranges,
    dashboards,
    views,
  } = state

  const timeRange =
    ranges.find(r => r.dashboardID === dashboardID) || DEFAULT_TIME_RANGE

  const dashboard = dashboards.find(d => d.id === dashboardID)

  return {
    links,
    views,
    sources,
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
  onAddCell: dashboardActions.addCellAsync,
  onCreateCellWithView: dashboardActions.createCellWithView,
  onUpdateView: viewActions.updateView,
  onSetActiveTimeMachine: setActiveTimeMachine,
}

export default connect(
  mstp,
  mdtp
)(ManualRefresh<Props>(withRouter<Props>(DashboardPage)))
