// Libraries
import React, {Component, MouseEvent} from 'react'
import {connect} from 'react-redux'
import {withRouter} from 'react-router'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import CellEditorOverlay from 'src/dashboards/components/CellEditorOverlay'
import DashboardHeader from 'src/dashboards/components/DashboardHeader'
import Dashboard from 'src/dashboards/components/Dashboard'
import ManualRefresh from 'src/shared/components/ManualRefresh'
import TemplateControlBar from 'src/tempVars/components/TemplateControlBar'

// Actions
import * as dashboardActions from 'src/dashboards/actions'
import * as annotationActions from 'src/shared/actions/annotations'
import * as cellEditorOverlayActions from 'src/dashboards/actions/cellEditorOverlay'
import * as appActions from 'src/shared/actions/app'
import * as errorActions from 'src/shared/actions/errors'
import * as notifyActions from 'src/shared/actions/notifications'

// Utils
import idNormalizer, {TYPE_ID} from 'src/normalizers/id'
import {millisecondTimeRange} from 'src/dashboards/utils/time'
import {getDeep} from 'src/utils/wrappers'
import {updateDashboardLinks} from 'src/dashboards/utils/dashboardSwitcherLinks'
import AutoRefresh from 'src/utils/AutoRefresh'

// APIs
import {loadDashboardLinks} from 'src/dashboards/apis'

// Constants
import {
  interval,
  DASHBOARD_LAYOUT_ROW_HEIGHT,
  TEMP_VAR_DASHBOARD_TIME,
  TEMP_VAR_UPPER_DASHBOARD_TIME,
} from 'src/shared/constants'
import {FORMAT_INFLUXQL, defaultTimeRange} from 'src/shared/data/timeRanges'
import {EMPTY_LINKS} from 'src/dashboards/constants/dashboardHeader'

// Types
import {WithRouterProps} from 'react-router'
import {ManualRefreshProps} from 'src/shared/components/ManualRefresh'
import {Location} from 'history'
import {InjectedRouter} from 'react-router'
import * as AnnotationsActions from 'src/types/actions/annotations'
import * as AppActions from 'src/types/actions/app'
import * as ColorsModels from 'src/types/colors'
import * as DashboardsModels from 'src/types/dashboards'
import * as ErrorsActions from 'src/types/actions/errors'
import * as QueriesModels from 'src/types/queries'
import * as SourcesModels from 'src/types/sources'
import * as TempVarsModels from 'src/types/tempVars'
import * as NotificationsActions from 'src/types/actions/notifications'

interface Props extends ManualRefreshProps, WithRouterProps {
  source: SourcesModels.Source
  sources: SourcesModels.Source[]
  params: {
    sourceID: string
    dashboardID: string
  }
  location: Location
  dashboardID: number
  dashboard: DashboardsModels.Dashboard
  dashboards: DashboardsModels.Dashboard[]
  handleChooseAutoRefresh: AppActions.SetAutoRefreshActionCreator
  autoRefresh: number
  templateControlBarVisibilityToggled: () => AppActions.TemplateControlBarVisibilityToggledActionCreator
  timeRange: QueriesModels.TimeRange
  zoomedTimeRange: QueriesModels.TimeRange
  showTemplateControlBar: boolean
  inPresentationMode: boolean
  handleClickPresentationButton: AppActions.DelayEnablePresentationModeDispatcher
  cellQueryStatus: {
    queryID: string
    status: object
  }
  errorThrown: ErrorsActions.ErrorThrownActionCreator
  router: InjectedRouter
  notify: NotificationsActions.PublishNotificationActionCreator
  getAnnotationsAsync: AnnotationsActions.GetAnnotationsDispatcher
  handleShowCellEditorOverlay: typeof cellEditorOverlayActions.showCellEditorOverlay
  handleHideCellEditorOverlay: typeof cellEditorOverlayActions.hideCellEditorOverlay
  handleDismissEditingAnnotation: AnnotationsActions.DismissEditingAnnotationActionCreator
  selectedCell: DashboardsModels.Cell
  thresholdsListType: string
  thresholdsListColors: ColorsModels.ColorNumber[]
  gaugeColors: ColorsModels.ColorNumber[]
  lineColors: ColorsModels.ColorString[]
  setDashTimeV1: typeof dashboardActions.setDashTimeV1
  setZoomedTimeRange: typeof dashboardActions.setZoomedTimeRange
  updateDashboard: typeof dashboardActions.updateDashboard
  putDashboard: typeof dashboardActions.putDashboard
  putDashboardByID: typeof dashboardActions.putDashboardByID
  getDashboardsAsync: typeof dashboardActions.getDashboardsAsync
  addDashboardCellAsync: typeof dashboardActions.addDashboardCellAsync
  editCellQueryStatus: typeof dashboardActions.editCellQueryStatus
  updateDashboardCell: typeof dashboardActions.updateDashboardCell
  cloneDashboardCellAsync: typeof dashboardActions.cloneDashboardCellAsync
  deleteDashboardCellAsync: typeof dashboardActions.deleteDashboardCellAsync
  templateVariableLocalSelected: typeof dashboardActions.templateVariableLocalSelected
  getDashboardWithTemplatesAsync: typeof dashboardActions.getDashboardWithTemplatesAsync
  rehydrateTemplatesAsync: typeof dashboardActions.rehydrateTemplatesAsync
  updateTemplateQueryParams: typeof dashboardActions.updateTemplateQueryParams
  updateQueryParams: typeof dashboardActions.updateQueryParams
}

interface State {
  scrollTop: number
  windowHeight: number
  selectedCell: DashboardsModels.Cell | null
  dashboardLinks: DashboardsModels.DashboardSwitcherLinks
}

@ErrorHandling
class DashboardPage extends Component<Props, State> {
  public constructor(props: Props) {
    super(props)

    this.state = {
      scrollTop: 0,
      selectedCell: null,
      windowHeight: window.innerHeight,
      dashboardLinks: EMPTY_LINKS,
    }
  }

  public async componentDidMount() {
    const {autoRefresh} = this.props

    AutoRefresh.poll(autoRefresh)
    AutoRefresh.subscribe(this.fetchAnnotations)

    window.addEventListener('resize', this.handleWindowResize, true)

    await this.getDashboard()

    this.fetchAnnotations()
    this.getDashboardLinks()
  }

  public fetchAnnotations = () => {
    const {source, timeRange, getAnnotationsAsync} = this.props
    const rangeMs = millisecondTimeRange(timeRange)
    getAnnotationsAsync(source.links.annotations, rangeMs)
  }

  public componentDidUpdate(prevProps: Props) {
    const {dashboard, autoRefresh} = this.props

    const prevPath = getDeep(prevProps.location, 'pathname', null)
    const thisPath = getDeep(this.props.location, 'pathname', null)

    const templates = this.parseTempVar(dashboard)
    const prevTemplates = this.parseTempVar(prevProps.dashboard)

    const intersection = _.intersection(templates, prevTemplates)
    const isTemplateDeleted = intersection.length !== prevTemplates.length

    if ((prevPath && thisPath && prevPath !== thisPath) || isTemplateDeleted) {
      this.getDashboard()
    }

    if (autoRefresh !== prevProps.autoRefresh) {
      AutoRefresh.poll(autoRefresh)
    }
  }

  public componentWillUnmount() {
    AutoRefresh.stopPolling()
    AutoRefresh.unsubscribe(this.fetchAnnotations)

    window.removeEventListener('resize', this.handleWindowResize, true)
    this.props.handleDismissEditingAnnotation()
  }

  public render() {
    const {
      source,
      sources,
      timeRange,
      timeRange: {lower, upper},
      zoomedTimeRange,
      zoomedTimeRange: {lower: zoomedLower, upper: zoomedUpper},
      showTemplateControlBar,
      dashboard,
      dashboardID,
      lineColors,
      gaugeColors,
      autoRefresh,
      selectedCell,
      manualRefresh,
      onManualRefresh,
      cellQueryStatus,
      thresholdsListType,
      thresholdsListColors,
      inPresentationMode,
      handleChooseAutoRefresh,
      handleShowCellEditorOverlay,
      handleHideCellEditorOverlay,
      handleClickPresentationButton,
    } = this.props
    const low = zoomedLower || lower
    const up = zoomedUpper || upper

    const lowerType = low && low.includes(':') ? 'timeStamp' : 'constant'
    const upperType = up && up.includes(':') ? 'timeStamp' : 'constant'
    const dashboardTime = {
      id: 'dashtime',
      tempVar: TEMP_VAR_DASHBOARD_TIME,
      type: lowerType,
      values: [
        {
          value: low,
          type: lowerType,
          selected: true,
          localSelected: true,
        },
      ],
    }

    const upperDashboardTime = {
      id: 'upperdashtime',
      tempVar: TEMP_VAR_UPPER_DASHBOARD_TIME,
      type: upperType,
      values: [
        {
          value: up || 'now()',
          type: upperType,
          selected: true,
          localSelected: true,
        },
      ],
    }

    let templatesIncludingDashTime
    if (dashboard) {
      templatesIncludingDashTime = [
        ...dashboard.templates,
        dashboardTime,
        upperDashboardTime,
        interval,
      ]
    } else {
      templatesIncludingDashTime = []
    }

    const {dashboardLinks} = this.state

    return (
      <div className="page dashboard-page">
        {selectedCell ? (
          <CellEditorOverlay
            source={source}
            sources={sources}
            cell={selectedCell}
            timeRange={timeRange}
            autoRefresh={autoRefresh}
            dashboardID={dashboardID}
            queryStatus={cellQueryStatus}
            onSave={this.handleSaveEditedCell}
            onCancel={handleHideCellEditorOverlay}
            templates={templatesIncludingDashTime}
            editQueryStatus={this.props.editCellQueryStatus}
            thresholdsListType={thresholdsListType}
            thresholdsListColors={thresholdsListColors}
            gaugeColors={gaugeColors}
            lineColors={lineColors}
          />
        ) : null}
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
          onToggleTempVarControls={this.handleToggleTempVarControls}
          handleClickPresentationButton={handleClickPresentationButton}
        />
        {inPresentationMode || (
          <TemplateControlBar
            templates={dashboard && dashboard.templates}
            onSaveTemplates={this.handleSaveTemplateVariables}
            onPickTemplate={this.handlePickTemplate}
            isOpen={showTemplateControlBar}
            source={source}
          />
        )}
        {dashboard ? (
          <Dashboard
            source={source}
            sources={sources}
            setScrollTop={this.setScrollTop}
            inView={this.inView}
            dashboard={dashboard}
            timeRange={timeRange}
            autoRefresh={autoRefresh}
            manualRefresh={manualRefresh}
            onZoom={this.handleZoomedTimeRange}
            inPresentationMode={inPresentationMode}
            onPositionChange={this.handleUpdatePosition}
            onDeleteCell={this.handleDeleteDashboardCell}
            onCloneCell={this.handleCloneCell}
            templatesIncludingDashTime={templatesIncludingDashTime}
            onSummonOverlayTechnologies={handleShowCellEditorOverlay}
          />
        ) : null}
      </div>
    )
  }

  public parseTempVar(
    dashboard: DashboardsModels.Dashboard
  ): TempVarsModels.Template[] {
    return getDeep(dashboard, 'templates', []).map(t => t.tempVar)
  }

  private handleWindowResize = (): void => {
    this.setState({windowHeight: window.innerHeight})
  }

  private getDashboard = async () => {
    const {dashboardID, source, getDashboardWithTemplatesAsync} = this.props

    await getDashboardWithTemplatesAsync(dashboardID, source)
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

  private inView = (cell: DashboardsModels.Cell): boolean => {
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

  private handleSaveEditedCell = async (
    newCell: DashboardsModels.Cell
  ): Promise<void> => {
    const {dashboard, handleHideCellEditorOverlay} = this.props
    await this.props.updateDashboardCell(dashboard, newCell)
    handleHideCellEditorOverlay()
  }

  private handleChooseTimeRange = (
    timeRange: QueriesModels.TimeRange
  ): void => {
    const {
      dashboard,
      getAnnotationsAsync,
      source,
      setDashTimeV1,
      updateQueryParams,
    } = this.props

    setDashTimeV1(dashboard.id, {
      ...timeRange,
      format: FORMAT_INFLUXQL,
    })

    updateQueryParams({
      lower: timeRange.lower,
      upper: timeRange.upper,
    })

    const annotationRange = millisecondTimeRange(timeRange)
    getAnnotationsAsync(source.links.annotations, annotationRange)
  }

  private handleUpdatePosition = (cells: DashboardsModels.Cell[]): void => {
    const {dashboard} = this.props
    const newDashboard = {...dashboard, cells}

    this.props.updateDashboard(newDashboard)
    this.props.putDashboard(newDashboard)
  }

  private handleAddCell = (): void => {
    const {dashboard} = this.props
    this.props.addDashboardCellAsync(dashboard)
  }

  private handleCloneCell = (cell: DashboardsModels.Cell): void => {
    const {dashboard} = this.props
    this.props.cloneDashboardCellAsync(dashboard, cell)
  }

  private handleRenameDashboard = async (name: string): Promise<void> => {
    const {dashboard} = this.props
    const renamedDashboard = {...dashboard, name}

    this.props.updateDashboard(renamedDashboard)
    await this.props.putDashboard(renamedDashboard)
    this.updateActiveDashboard()
  }

  private handleDeleteDashboardCell = (cell: DashboardsModels.Cell): void => {
    const {dashboard} = this.props
    this.props.deleteDashboardCellAsync(dashboard, cell)
  }

  private handlePickTemplate = (
    template: TempVarsModels.Template,
    value: TempVarsModels.TemplateValue
  ): void => {
    const {
      dashboard,
      source,
      templateVariableLocalSelected,
      rehydrateTemplatesAsync,
    } = this.props

    templateVariableLocalSelected(dashboard.id, template.id, value)
    rehydrateTemplatesAsync(dashboard.id, source)
  }

  private handleSaveTemplateVariables = async (
    templates: TempVarsModels.Template[]
  ): Promise<void> => {
    const {dashboard, updateTemplateQueryParams} = this.props

    try {
      await this.props.putDashboard({
        ...dashboard,
        templates,
      })

      updateTemplateQueryParams(dashboard.id)
    } catch (error) {
      console.error(error)
    }
  }

  private handleToggleTempVarControls = (): void => {
    this.props.templateControlBarVisibilityToggled()
  }

  private handleZoomedTimeRange = (
    zoomedTimeRange: QueriesModels.TimeRange
  ): void => {
    const {setZoomedTimeRange, updateQueryParams} = this.props

    setZoomedTimeRange(zoomedTimeRange)

    updateQueryParams({
      zoomedLower: zoomedTimeRange.lower,
      zoomedUpper: zoomedTimeRange.upper,
    })
  }

  private setScrollTop = (e: MouseEvent<JSX.Element>): void => {
    const target = e.target as HTMLElement

    this.setState({scrollTop: target.scrollTop})
  }

  private getDashboardLinks = async (): Promise<void> => {
    const {source, dashboard: activeDashboard} = this.props

    try {
      const dashboardLinks = await loadDashboardLinks(source, {activeDashboard})

      this.setState({
        dashboardLinks,
      })
    } catch (error) {
      console.error(error)
    }
  }
}

const mstp = (state, {params: {dashboardID}}) => {
  const {
    app: {
      ephemeral: {inPresentationMode},
      persisted: {autoRefresh, showTemplateControlBar},
    },
    dashboardUI: {dashboards, cellQueryStatus, zoomedTimeRange},
    sources,
    dashTimeV1,
    cellEditorOverlay: {
      cell,
      thresholdsListType,
      thresholdsListColors,
      gaugeColors,
      lineColors,
    },
  } = state

  const timeRange =
    dashTimeV1.ranges.find(
      r => r.dashboardID === idNormalizer(TYPE_ID, dashboardID)
    ) || defaultTimeRange

  const dashboard = dashboards.find(
    d => d.id === idNormalizer(TYPE_ID, dashboardID)
  )

  const selectedCell = cell

  return {
    sources,
    dashboard,
    dashboardID: Number(dashboardID),
    timeRange,
    zoomedTimeRange,
    autoRefresh,
    cellQueryStatus,
    inPresentationMode,
    showTemplateControlBar,
    selectedCell,
    thresholdsListType,
    thresholdsListColors,
    gaugeColors,
    lineColors,
  }
}

const mdtp = {
  setDashTimeV1: dashboardActions.setDashTimeV1,
  setZoomedTimeRange: dashboardActions.setZoomedTimeRange,
  updateDashboard: dashboardActions.updateDashboard,
  putDashboard: dashboardActions.putDashboard,
  putDashboardByID: dashboardActions.putDashboardByID,
  getDashboardsAsync: dashboardActions.getDashboardsAsync,
  addDashboardCellAsync: dashboardActions.addDashboardCellAsync,
  editCellQueryStatus: dashboardActions.editCellQueryStatus,
  updateDashboardCell: dashboardActions.updateDashboardCell,
  cloneDashboardCellAsync: dashboardActions.cloneDashboardCellAsync,
  deleteDashboardCellAsync: dashboardActions.deleteDashboardCellAsync,
  templateVariableLocalSelected: dashboardActions.templateVariableLocalSelected,
  getDashboardWithTemplatesAsync:
    dashboardActions.getDashboardWithTemplatesAsync,
  rehydrateTemplatesAsync: dashboardActions.rehydrateTemplatesAsync,
  updateTemplateQueryParams: dashboardActions.updateTemplateQueryParams,
  updateQueryParams: dashboardActions.updateQueryParams,
  handleChooseAutoRefresh: appActions.setAutoRefresh,
  templateControlBarVisibilityToggled:
    appActions.templateControlBarVisibilityToggled,
  handleClickPresentationButton: appActions.delayEnablePresentationMode,
  errorThrown: errorActions.errorThrown,
  notify: notifyActions.notify,
  handleShowCellEditorOverlay: cellEditorOverlayActions.showCellEditorOverlay,
  handleHideCellEditorOverlay: cellEditorOverlayActions.hideCellEditorOverlay,
  getAnnotationsAsync: annotationActions.getAnnotationsAsync,
  handleDismissEditingAnnotation: annotationActions.dismissEditingAnnotation,
}

export default connect(mstp, mdtp)(
  ManualRefresh<Props>(withRouter<Props>(DashboardPage))
)
