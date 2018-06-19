import React, {Component} from 'react'
import {connect} from 'react-redux'
import {withRouter} from 'react-router'
import {bindActionCreators} from 'redux'

import _ from 'lodash'

import {isUserAuthorized, EDITOR_ROLE} from 'src/auth/Authorized'

import CellEditorOverlay from 'src/dashboards/components/CellEditorOverlay'
import DashboardHeader from 'src/dashboards/components/DashboardHeader'
import Dashboard from 'src/dashboards/components/Dashboard'
import ManualRefresh from 'src/shared/components/ManualRefresh'
import TemplateControlBar from 'src/tempVars/components/TemplateControlBar'

import {errorThrown as errorThrownAction} from 'src/shared/actions/errors'
import {notify as notifyAction} from 'src/shared/actions/notifications'
import idNormalizer, {TYPE_ID} from 'src/normalizers/id'
import {millisecondTimeRange} from 'src/dashboards/utils/time'

import * as dashboardActionCreators from 'src/dashboards/actions'
import * as annotationActions from 'src/shared/actions/annotations'

import {
  showCellEditorOverlay,
  hideCellEditorOverlay,
} from 'src/dashboards/actions/cellEditorOverlay'
import {showOverlay} from 'src/shared/actions/overlayTechnology'

import {stripTempVar} from 'src/dashboards/utils/tempVars'

import {dismissEditingAnnotation} from 'src/shared/actions/annotations'

import {
  setAutoRefresh,
  templateControlBarVisibilityToggled as templateControlBarVisibilityToggledAction,
  delayEnablePresentationMode,
} from 'src/shared/actions/app'

import {
  interval,
  DASHBOARD_LAYOUT_ROW_HEIGHT,
  TEMP_VAR_DASHBOARD_TIME,
  TEMP_VAR_UPPER_DASHBOARD_TIME,
} from 'src/shared/constants'
import {FORMAT_INFLUXQL, defaultTimeRange} from 'src/shared/data/timeRanges'

import {ErrorHandling} from 'src/shared/decorators/errors'

import {getDeep} from 'src/utils/wrappers'

import {Location} from 'history'
import {InjectedRouter} from 'react-router'
import {Dispatch} from 'redux'
import {
  Cell,
  CellType,
  Dashboard as IDashboard,
  Source,
  TimeRange,
} from 'src/types'
import {DashboardName} from 'src/types/dashboard'
import {
  SetTimeRangeAction,
  PutDashboardDispatcher,
  PutDashboardByIDDispatcher,
  GetDashboardsNamesDispatcher,
  GetDashboardWithHydratedAndSyncedTempVarsAsyncDispatcher,
} from 'src/dashboards/actions'
import {
  SetAutoRefreshAction,
  TemplateControlBarVisibilityToggledAction,
  EnablePresentationModeAction,
} from 'src/shared/actions/app'
import {ColorNumber, ColorString} from 'src/types/colors'

interface DashboardActions {
  putDashboard: PutDashboardDispatcher
  putDashboardByID: PutDashboardByIDDispatcher
  getDashboardsNamesAsync: GetDashboardsNamesDispatcher
  getDashboardWithHydratedAndSyncedTempVarsAsync: GetDashboardWithHydratedAndSyncedTempVarsAsyncDispatcher
  setTimeRange: (timeRange: TimeRange) => SetTimeRangeAction
  addDashboardCellAsync: (
    dashboard: IDashboard,
    cellType: CellType
  ) => Promise<void>
}
interface Router {
  push: (path: string) => void
}

interface Props {
  source: Source
  sources: Source[]
  params: {
    sourceID: string
    dashboardID: string
  }
  location: Location
  dashboard: IDashboard
  dashboardActions: DashboardActions
  dashboards: IDashboard[]
  handleChooseAutoRefresh: (milliseconds: number) => SetAutoRefreshAction
  autoRefresh: number
  templateControlBarVisibilityToggled: () => TemplateControlBarVisibilityToggledAction
  timeRange: TimeRange
  zoomedTimeRange: TimeRange
  showTemplateControlBar: boolean
  inPresentationMode: boolean
  handleClickPresentationButton: (
    dispatch: Dispatch<EnablePresentationModeAction>
  ) => Promise<void>
  cellQueryStatus: {
    queryID: string
    status: object
  }
  errorThrown: () => void // TODO: double-check type
  manualRefresh: number
  onManualRefresh: () => void // TODO: double-check type
  meRole: string
  isUsingAuth: boolean
  router: Router
  notify: () => void // TODO: double-check type
  getAnnotationsAsync: () => void // TODO: double-check type
  handleShowCellEditorOverlay: () => void // TODO: double-check type
  handleHideCellEditorOverlay: () => void // TODO: double-check type
  handleDismissEditingAnnotation: () => void // TODO: double-check type
  selectedCell: Cell
  thresholdsListType: string
  thresholdsListColors: ColorNumber[]
  gaugeColors: ColorNumber[]
  lineColors: ColorString[]
  handleShowOverlay: () => void // TODO: double-check type
}

interface State {
  isEditMode: boolean
  selectedCell: Cell | null
  scrollTop: number
  windowHeight: number
  dashboardsNames: DashboardName[]
}

@ErrorHandling
class DashboardPage extends Component<Props, State> {
  constructor(props) {
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
      params: {dashboardID},
      dashboardActions: {putDashboardByID},
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
      this.intervalID = setInterval(() => {
        getAnnotationsAsync(source.links.annotations, annotationRange)
      }, autoRefresh)
    }

    window.addEventListener('resize', this.handleWindowResize, true)

    await this.getDashboard()

    // If using auth and role is Viewer, temp vars will be stale until dashboard
    // is refactored so as not to require a write operation (a PUT in this case)
    if (!isUsingAuth || isUserAuthorized(meRole, EDITOR_ROLE)) {
      // putDashboardByID refreshes & persists influxql generated template variable values.
      await putDashboardByID(dashboardID)
    }

    this.getDashboardsNames()
  }

  public componentWillReceiveProps(nextProps) {
    const {source, getAnnotationsAsync, timeRange} = this.props
    if (this.props.autoRefresh !== nextProps.autoRefresh) {
      clearInterval(this.intervalID)
      const annotationRange = millisecondTimeRange(timeRange)
      if (nextProps.autoRefresh) {
        this.intervalID = setInterval(() => {
          getAnnotationsAsync(source.links.annotations, annotationRange)
        }, nextProps.autoRefresh)
      }
    }
  }

  public componentDidUpdate(prevProps) {
    const prevPath = getDeep(prevProps.location, 'pathname', null)
    const thisPath = getDeep(this.props.location, 'pathname', null)

    if (prevPath && thisPath && prevPath !== thisPath) {
      this.getDashboard()
    }
  }

  public componentWillUnmount() {
    clearInterval(this.intervalID)
    this.intervalID = false
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
      dashboards,
      lineColors,
      gaugeColors,
      autoRefresh,
      selectedCell,
      manualRefresh,
      onManualRefresh,
      cellQueryStatus,
      thresholdsListType,
      thresholdsListColors,
      dashboardActions,
      inPresentationMode,
      handleChooseAutoRefresh,
      handleShowCellEditorOverlay,
      handleHideCellEditorOverlay,
      handleClickPresentationButton,
      params: {sourceID, dashboardID},
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
            editQueryStatus={dashboardActions.editCellQueryStatus}
            thresholdsListType={thresholdsListType}
            thresholdsListColors={thresholdsListColors}
            gaugeColors={gaugeColors}
            lineColors={lineColors}
          />
        ) : null}
        <DashboardHeader
          names={dashboardsNames}
          sourceID={sourceID}
          dashboard={dashboard}
          dashboards={dashboards}
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
            onAddCell={this.handleAddCell}
            inPresentationMode={inPresentationMode}
            onPositionChange={this.handleUpdatePosition}
            onSelectTemplate={this.handleSelectTemplate}
            onDeleteCell={this.handleDeleteDashboardCell}
            onCloneCell={this.handleCloneCell}
            showTemplateControlBar={showTemplateControlBar}
            templatesIncludingDashTime={templatesIncludingDashTime}
            onSummonOverlayTechnologies={handleShowCellEditorOverlay}
          />
        ) : null}
      </div>
    )
  }

  private handleWindowResize = () => {
    this.setState({windowHeight: window.innerHeight})
  }

  private async getDashboard() {
    const {
      params: {dashboardID},
      dashboardActions: {getDashboardWithHydratedAndSyncedTempVarsAsync},
      source,
      router,
      location,
    } = this.props

    return await getDashboardWithHydratedAndSyncedTempVarsAsync(
      dashboardID,
      source,
      router,
      location
    )
  }

  private async getDashboardsNames() {
    const {
      params: {sourceID},
      dashboardActions: {getDashboardsNamesAsync},
    } = this.props

    const dashboardsNames = await getDashboardsNamesAsync(sourceID)
    this.setState({dashboardsNames})
  }

  private inView = cell => {
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

  private handleSaveEditedCell = newCell => {
    const {
      dashboardActions,
      dashboard,
      handleHideCellEditorOverlay,
    } = this.props
    dashboardActions
      .updateDashboardCell(dashboard, newCell)
      .then(handleHideCellEditorOverlay)
  }

  private handleChooseTimeRange = timeRange => {
    const {
      dashboard,
      dashboardActions,
      getAnnotationsAsync,
      source,
      location,
    } = this.props

    dashboardActions.setDashTimeV1(dashboard.id, {
      ...timeRange,
      format: FORMAT_INFLUXQL,
    })

    dashboardActions.syncURLQueryParamsFromQueryParamsObject(location, {
      lower: timeRange.lower,
      upper: timeRange.upper,
    })

    const annotationRange = millisecondTimeRange(timeRange)
    getAnnotationsAsync(source.links.annotations, annotationRange)
  }

  private handleUpdatePosition = cells => {
    const {dashboardActions, dashboard, meRole, isUsingAuth} = this.props
    const newDashboard = {...dashboard, cells}

    // GridLayout invokes onLayoutChange on first load, which bubbles up to
    // invoke handleUpdatePosition. If using auth, Viewer is not authorized to
    // PUT, so until the need for PUT is removed, this is prevented.
    if (!isUsingAuth || isUserAuthorized(meRole, EDITOR_ROLE)) {
      dashboardActions.updateDashboard(newDashboard)
      dashboardActions.putDashboard(newDashboard)
    }
  }

  private handleAddCell = () => {
    const {dashboardActions, dashboard} = this.props
    dashboardActions.addDashboardCellAsync(dashboard)
  }

  private handleCloneCell = cell => {
    const {dashboardActions, dashboard} = this.props
    dashboardActions.cloneDashboardCellAsync(dashboard, cell)
  }

  private handleEditDashboard = () => {
    this.setState({isEditMode: true})
  }

  private handleCancelEditDashboard = () => {
    this.setState({isEditMode: false})
  }

  private handleRenameDashboard = async name => {
    const {dashboardActions, dashboard} = this.props
    this.setState({isEditMode: false})
    const newDashboard = {...dashboard, name}

    dashboardActions.updateDashboard(newDashboard)
    await dashboardActions.putDashboard(newDashboard)
    this.getDashboardsNames()
  }

  private handleUpdateDashboardCell = newCell => () => {
    const {dashboardActions, dashboard} = this.props
    dashboardActions.updateDashboardCell(dashboard, newCell)
  }

  private handleDeleteDashboardCell = cell => {
    const {dashboardActions, dashboard} = this.props
    dashboardActions.deleteDashboardCellAsync(dashboard, cell)
  }

  private handleSelectTemplate = templateID => value => {
    const {
      dashboardActions,
      dashboard,
      params: {dashboardID},
      location,
    } = this.props

    const currentTempVar = dashboard.templates.find(
      tempVar => tempVar.id === templateID
    )
    const strippedTempVar = stripTempVar(currentTempVar.tempVar)
    const isTempVarInURLQuery = !!location.query[strippedTempVar]

    if (isTempVarInURLQuery) {
      const updatedQueryParam = {
        [strippedTempVar]: value.value,
      }
      dashboardActions.syncURLQueryParamsFromQueryParamsObject(
        location,
        updatedQueryParam
      )
    }
    dashboardActions.templateVariableSelected(dashboard.id, templateID, [value])
    dashboardActions.putDashboardByID(dashboardID)
  }

  private handleSaveTemplateVariables = async templates => {
    const {location, dashboardActions, dashboard} = this.props

    try {
      await dashboardActions.putDashboard({
        ...dashboard,
        templates,
      })
      const deletedTempVars = dashboard.templates.filter(
        ({tempVar: oldTempVar}) =>
          !templates.find(({tempVar: newTempVar}) => oldTempVar === newTempVar)
      )
      dashboardActions.syncURLQueryFromTempVars(
        location,
        templates,
        deletedTempVars
      )
    } catch (error) {
      console.error(error)
    }
  }

  private handleRunQueryFailure = error => {
    console.error(error)
    this.props.errorThrown(error)
  }

  private handleToggleTempVarControls = () => {
    this.props.templateControlBarVisibilityToggled()
  }

  private handleZoomedTimeRange = (zoomedLower, zoomedUpper) => {
    const {dashboardActions, location} = this.props
    const zoomedTimeRange = {lower: zoomedLower, upper: zoomedUpper}
    dashboardActions.setZoomedTimeRangeAsync(zoomedTimeRange, location)
  }

  private setScrollTop = event => {
    this.setState({scrollTop: event.target.scrollTop})
  }
}

const mapStateToProps = (state, {params: {dashboardID}}) => {
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

const mapDispatchToProps = dispatch => ({
  handleChooseAutoRefresh: bindActionCreators(setAutoRefresh, dispatch),
  templateControlBarVisibilityToggled: bindActionCreators(
    templateControlBarVisibilityToggledAction,
    dispatch
  ),
  handleClickPresentationButton: bindActionCreators(
    delayEnablePresentationMode,
    dispatch
  ),
  dashboardActions: bindActionCreators(dashboardActionCreators, dispatch),
  errorThrown: bindActionCreators(errorThrownAction, dispatch),
  notify: bindActionCreators(notifyAction, dispatch),
  getAnnotationsAsync: bindActionCreators(
    annotationActions.getAnnotationsAsync,
    dispatch
  ),
  handleShowCellEditorOverlay: bindActionCreators(
    showCellEditorOverlay,
    dispatch
  ),
  handleHideCellEditorOverlay: bindActionCreators(
    hideCellEditorOverlay,
    dispatch
  ),
  handleDismissEditingAnnotation: bindActionCreators(
    dismissEditingAnnotation,
    dispatch
  ),
  handleShowOverlay: bindActionCreators(showOverlay, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(
  ManualRefresh(withRouter(DashboardPage))
)
