// Libraries
import React, {Component, MouseEvent} from 'react'
import {connect} from 'react-redux'
import {withRouter} from 'react-router'
import _ from 'lodash'

// Components
import {isUserAuthorized, EDITOR_ROLE} from 'src/auth/Authorized'
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
import {stripTempVar} from 'src/dashboards/utils/tempVars'
import {getDeep} from 'src/utils/wrappers'

// Constants
import {
  interval,
  DASHBOARD_LAYOUT_ROW_HEIGHT,
  TEMP_VAR_DASHBOARD_TIME,
  TEMP_VAR_UPPER_DASHBOARD_TIME,
} from 'src/shared/constants'
import {FORMAT_INFLUXQL, defaultTimeRange} from 'src/shared/data/timeRanges'

// Types
import {WithRouterProps} from 'react-router'
import {ManualRefreshProps} from 'src/shared/components/ManualRefresh'
import {Location} from 'history'
import {InjectedRouter} from 'react-router'
import * as SourceData from 'src/types/sources'
import * as TempVarData from 'src/types/tempVars'
import * as DashboardData from 'src/types/dashboards'
import * as QueryData from 'src/types/query'
import * as ColorData from 'src/types/colors'
import * as AnnotationActions from 'src/types/actions/annotations'
import * as AppActions from 'src/types/actions/app'
import * as CellEditorOverlayActions from 'src/types/actions/cellEditorOverlay'
import * as DashboardActions from 'src/types/actions/dashboards'
import * as ErrorActions from 'src/types/actions/errors'
import * as NotificationActions from 'src/types/actions/notifications'

interface DashboardActions {
  setDashTimeV1: DashboardActions.SetDashTimeV1ActionCreator
  updateDashboard: DashboardActions.UpdateDashboardActionCreator
  syncURLQueryParamsFromQueryParamsObject: DashboardActions.SyncURLQueryFromQueryParamsObjectDispatcher
  putDashboard: DashboardActions.PutDashboardDispatcher
  putDashboardByID: DashboardActions.PutDashboardByIDDispatcher
  getDashboardsNamesAsync: DashboardActions.GetDashboardsNamesDispatcher
  getDashboardWithHydratedAndSyncedTempVarsAsync: DashboardActions.GetDashboardWithHydratedAndSyncedTempVarsAsyncDispatcher
  setTimeRange: DashboardActions.SetTimeRangeActionCreator
  addDashboardCellAsync: DashboardActions.AddDashboardCellDispatcher
  editCellQueryStatus: DashboardActions.EditCellQueryStatusActionCreator
  updateDashboardCell: DashboardActions.UpdateDashboardCellDispatcher
  cloneDashboardCellAsync: DashboardActions.CloneDashboardCellDispatcher
  deleteDashboardCellAsync: DashboardActions.DeleteDashboardCellDispatcher
  templateVariableSelected: DashboardActions.TemplateVariableSelectedActionCreator
  syncURLQueryFromTempVars: DashboardActions.SyncURLQueryFromTempVarsDispatcher
  setZoomedTimeRangeAsync: DashboardActions.SetZoomedTimeRangeDispatcher
}

interface Props extends DashboardActions, ManualRefreshProps, WithRouterProps {
  source: SourceData.Source
  sources: SourceData.Source[]
  params: {
    sourceID: string
    dashboardID: string
  }
  location: Location
  dashboardID: number
  dashboard: DashboardData.Dashboard
  dashboards: DashboardData.Dashboard[]
  handleChooseAutoRefresh: AppActions.SetAutoRefreshActionCreator
  autoRefresh: number
  templateControlBarVisibilityToggled: () => AppActions.TemplateControlBarVisibilityToggledActionCreator
  timeRange: QueryData.TimeRange
  zoomedTimeRange: QueryData.TimeRange
  showTemplateControlBar: boolean
  inPresentationMode: boolean
  handleClickPresentationButton: AppActions.DelayEnablePresentationModeDispatcher
  cellQueryStatus: {
    queryID: string
    status: object
  }
  errorThrown: ErrorActions.ErrorThrownActionCreator
  meRole: string
  isUsingAuth: boolean
  router: InjectedRouter
  notify: NotificationActions.PublishNotificationActionCreator
  getAnnotationsAsync: AnnotationActions.GetAnnotationsDispatcher
  handleShowCellEditorOverlay: CellEditorOverlayActions.ShowCellEditorOverlayActionCreator
  handleHideCellEditorOverlay: CellEditorOverlayActions.HideCellEditorOverlayActionCreator
  handleDismissEditingAnnotation: AnnotationActions.DismissEditingAnnotationActionCreator
  selectedCell: DashboardData.Cell
  thresholdsListType: string
  thresholdsListColors: ColorData.ColorNumber[]
  gaugeColors: ColorData.ColorNumber[]
  lineColors: ColorData.ColorString[]
}

interface State {
  isEditMode: boolean
  selectedCell: DashboardData.Cell | null
  scrollTop: number
  windowHeight: number
  dashboardsNames: DashboardData.DashboardName[]
}

@ErrorHandling
class DashboardPage extends Component<Props, State> {
  private intervalID: number

  public constructor(props: Props) {
    super(props)

    this.state = {
      isEditMode: false,
      selectedCell: null,
      scrollTop: 0,
      windowHeight: window.innerHeight,
      dashboardsNames: [],
    }
  }

  public async componentDidMount() {
    const {
      dashboardID,
      source,
      meRole,
      isUsingAuth,
      getAnnotationsAsync,
      timeRange,
      autoRefresh,
    } = this.props

    const annotationRange = millisecondTimeRange(timeRange)
    getAnnotationsAsync(source.links.annotations, annotationRange)

    if (autoRefresh) {
      this.intervalID = window.setInterval(() => {
        getAnnotationsAsync(source.links.annotations, annotationRange)
      }, autoRefresh)
    }

    window.addEventListener('resize', this.handleWindowResize, true)

    await this.getDashboard()

    // If using auth and role is Viewer, temp vars will be stale until dashboard
    // is refactored so as not to require a write operation (a PUT in this case)
    if (!isUsingAuth || isUserAuthorized(meRole, EDITOR_ROLE)) {
      // putDashboardByID refreshes & persists influxql generated template variable values.
      await this.props.putDashboardByID(dashboardID)
    }

    this.getDashboardsNames()
  }

  public componentWillReceiveProps(nextProps: Props) {
    const {source, getAnnotationsAsync, timeRange} = this.props
    if (this.props.autoRefresh !== nextProps.autoRefresh) {
      clearInterval(this.intervalID)
      const annotationRange = millisecondTimeRange(timeRange)
      if (nextProps.autoRefresh) {
        this.intervalID = window.setInterval(() => {
          getAnnotationsAsync(source.links.annotations, annotationRange)
        }, nextProps.autoRefresh)
      }
    }
  }

  public componentDidUpdate(prevProps: Props) {
    const prevPath = getDeep(prevProps.location, 'pathname', null)
    const thisPath = getDeep(this.props.location, 'pathname', null)

    if (prevPath && thisPath && prevPath !== thisPath) {
      this.getDashboard()
    }
  }

  public componentWillUnmount() {
    clearInterval(this.intervalID)
    window.removeEventListener('resize', this.handleWindowResize, true)
    this.props.handleDismissEditingAnnotation()
  }

  public render() {
    const {
      isUsingAuth,
      meRole,
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
    const {dashboardsNames} = this.state

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

    const {isEditMode} = this.state

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
          names={dashboardsNames}
          dashboard={dashboard}
          timeRange={timeRange}
          isEditMode={isEditMode}
          autoRefresh={autoRefresh}
          isHidden={inPresentationMode}
          onAddCell={this.handleAddCell}
          onManualRefresh={onManualRefresh}
          zoomedTimeRange={zoomedTimeRange}
          onSave={this.handleRenameDashboard}
          onCancel={this.handleCancelEditDashboard}
          onEditDashboard={this.handleEditDashboard}
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
            meRole={meRole}
            isUsingAuth={isUsingAuth}
            onSaveTemplates={this.handleSaveTemplateVariables}
            onSelectTemplate={this.handleSelectTemplate}
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

  private handleWindowResize = (): void => {
    this.setState({windowHeight: window.innerHeight})
  }

  private getDashboard = async (): Promise<
    DashboardActions.GetDashboardWithHydratedAndSyncedTempVarsAsyncThunk
  > => {
    const {dashboardID, source, router, location} = this.props

    return await this.props.getDashboardWithHydratedAndSyncedTempVarsAsync(
      dashboardID,
      source,
      router,
      location
    )
  }

  private getDashboardsNames = async (): Promise<void> => {
    const {
      params: {sourceID},
    } = this.props

    // TODO: remove any once react-redux connect is properly typed
    const dashboardsNames = (await this.props.getDashboardsNamesAsync(
      sourceID
    )) as any

    this.setState({dashboardsNames})
  }

  private inView = (cell: DashboardData.Cell): boolean => {
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
    newCell: DashboardData.Cell
  ): Promise<void> => {
    const {dashboard, handleHideCellEditorOverlay} = this.props
    await this.props.updateDashboardCell(dashboard, newCell)
    handleHideCellEditorOverlay()
  }

  private handleChooseTimeRange = (timeRange: QueryData.TimeRange): void => {
    const {
      dashboard,

      getAnnotationsAsync,
      source,
      location,
    } = this.props

    this.props.setDashTimeV1(dashboard.id, {
      ...timeRange,
      format: FORMAT_INFLUXQL,
    })

    this.props.syncURLQueryParamsFromQueryParamsObject(location, {
      lower: timeRange.lower,
      upper: timeRange.upper,
    })

    const annotationRange = millisecondTimeRange(timeRange)
    getAnnotationsAsync(source.links.annotations, annotationRange)
  }

  private handleUpdatePosition = (cells: DashboardData.Cell[]): void => {
    const {dashboard, meRole, isUsingAuth} = this.props
    const newDashboard = {...dashboard, cells}

    // GridLayout invokes onLayoutChange on first load, which bubbles up to
    // invoke handleUpdatePosition. If using auth, Viewer is not authorized to
    // PUT, so until the need for PUT is removed, this is prevented.
    if (!isUsingAuth || isUserAuthorized(meRole, EDITOR_ROLE)) {
      this.props.updateDashboard(newDashboard)
      this.props.putDashboard(newDashboard)
    }
  }

  private handleAddCell = (): void => {
    const {dashboard} = this.props
    this.props.addDashboardCellAsync(dashboard)
  }

  private handleCloneCell = (cell: DashboardData.Cell): void => {
    const {dashboard} = this.props
    this.props.cloneDashboardCellAsync(dashboard, cell)
  }

  private handleEditDashboard = (): void => {
    this.setState({isEditMode: true})
  }

  private handleCancelEditDashboard = (): void => {
    this.setState({isEditMode: false})
  }

  private handleRenameDashboard = async (name: string): Promise<void> => {
    const {dashboard} = this.props
    this.setState({isEditMode: false})
    const newDashboard = {...dashboard, name}

    this.props.updateDashboard(newDashboard)
    await this.props.putDashboard(newDashboard)
    this.getDashboardsNames()
  }

  private handleDeleteDashboardCell = (cell: DashboardData.Cell): void => {
    const {dashboard} = this.props
    this.props.deleteDashboardCellAsync(dashboard, cell)
  }

  private handleSelectTemplate = (
    templateID: string
  ): ((value: TempVarData.TemplateValue) => void) => (
    value: TempVarData.TemplateValue
  ): void => {
    const {dashboard, dashboardID, location} = this.props

    const currentTempVar = dashboard.templates.find(
      tempVar => tempVar.id === templateID
    )
    const strippedTempVar = stripTempVar(currentTempVar.tempVar)
    const isTempVarInURLQuery = !!location.query[strippedTempVar]

    if (isTempVarInURLQuery) {
      const updatedQueryParam = {
        [strippedTempVar]: value.value,
      }
      this.props.syncURLQueryParamsFromQueryParamsObject(
        location,
        updatedQueryParam
      )
    }
    this.props.templateVariableSelected(dashboard.id, templateID, [value])
    this.props.putDashboardByID(dashboardID)
  }

  private handleSaveTemplateVariables = async (
    templates: TempVarData.Template[]
  ): Promise<void> => {
    const {location, dashboard} = this.props

    try {
      await this.props.putDashboard({
        ...dashboard,
        templates,
      })
      const deletedTempVars = dashboard.templates.filter(
        ({tempVar: oldTempVar}) =>
          !templates.find(({tempVar: newTempVar}) => oldTempVar === newTempVar)
      )
      this.props.syncURLQueryFromTempVars(location, templates, deletedTempVars)
    } catch (error) {
      console.error(error)
    }
  }

  private handleToggleTempVarControls = (): void => {
    this.props.templateControlBarVisibilityToggled()
  }

  private handleZoomedTimeRange = (
    zoomedTimeRange: QueryData.TimeRange
  ): void => {
    const {location} = this.props
    this.props.setZoomedTimeRangeAsync(zoomedTimeRange, location)
  }

  private setScrollTop = (e: MouseEvent<JSX.Element>): void => {
    const target = e.target as HTMLElement

    this.setState({scrollTop: target.scrollTop})
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
    auth: {me, isUsingAuth},
    cellEditorOverlay: {
      cell,
      thresholdsListType,
      thresholdsListColors,
      gaugeColors,
      lineColors,
    },
  } = state

  const meRole = _.get(me, 'role', null)

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
    meRole,
    dashboard,
    dashboardID: Number(dashboardID),
    timeRange,
    zoomedTimeRange,
    dashboards,
    autoRefresh,
    isUsingAuth,
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
  ...dashboardActions,
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
