import React, {Component} from 'react'
import PropTypes from 'prop-types'
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

import {errorThrown as errorThrownAction} from 'shared/actions/errors'
import {notify as notifyAction} from 'shared/actions/notifications'
import idNormalizer, {TYPE_ID} from 'src/normalizers/id'
import {millisecondTimeRange} from 'src/dashboards/utils/time'

import * as dashboardActionCreators from 'src/dashboards/actions'
import * as annotationActions from 'shared/actions/annotations'

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
} from 'shared/actions/app'
import {presentationButtonDispatcher} from 'shared/dispatchers'
import {
  interval,
  DASHBOARD_LAYOUT_ROW_HEIGHT,
  TEMP_VAR_DASHBOARD_TIME,
  TEMP_VAR_UPPER_DASHBOARD_TIME,
} from 'shared/constants'
import {FORMAT_INFLUXQL, defaultTimeRange} from 'src/shared/data/timeRanges'

import {colorsStringSchema, colorsNumberSchema} from 'shared/schemas'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {getDeep} from 'src/utils/wrappers'

@ErrorHandling
class DashboardPage extends Component {
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

  async componentDidMount() {
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

  componentWillReceiveProps(nextProps) {
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

  componentDidUpdate(prevProps) {
    const prevPath = getDeep(prevProps.location, 'pathname', null)
    const thisPath = getDeep(this.props.location, 'pathname', null)

    if (prevPath && thisPath && prevPath !== thisPath) {
      this.getDashboard()
    }
  }

  handleWindowResize = () => {
    this.setState({windowHeight: window.innerHeight})
  }

  componentWillUnmount() {
    clearInterval(this.intervalID)
    this.intervalID = false
    window.removeEventListener('resize', this.handleWindowResize, true)
    this.props.handleDismissEditingAnnotation()
  }

  async getDashboard() {
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

  async getDashboardsNames() {
    const {
      params: {sourceID},
      dashboardActions: {getDashboardsNamesAsync},
    } = this.props

    const dashboardsNames = await getDashboardsNamesAsync(sourceID)
    this.setState({dashboardsNames})
  }

  inView = cell => {
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

  handleSaveEditedCell = newCell => {
    const {
      dashboardActions,
      dashboard,
      handleHideCellEditorOverlay,
    } = this.props
    dashboardActions
      .updateDashboardCell(dashboard, newCell)
      .then(handleHideCellEditorOverlay)
  }

  handleChooseTimeRange = timeRange => {
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

  handleUpdatePosition = cells => {
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

  handleAddCell = () => {
    const {dashboardActions, dashboard} = this.props
    dashboardActions.addDashboardCellAsync(dashboard)
  }

  handleCloneCell = cell => {
    const {dashboardActions, dashboard} = this.props
    dashboardActions.cloneDashboardCellAsync(dashboard, cell)
  }

  handleEditDashboard = () => {
    this.setState({isEditMode: true})
  }

  handleCancelEditDashboard = () => {
    this.setState({isEditMode: false})
  }

  handleRenameDashboard = async name => {
    const {dashboardActions, dashboard} = this.props
    this.setState({isEditMode: false})
    const newDashboard = {...dashboard, name}

    dashboardActions.updateDashboard(newDashboard)
    await dashboardActions.putDashboard(newDashboard)
    this.getDashboardsNames()
  }

  handleUpdateDashboardCell = newCell => () => {
    const {dashboardActions, dashboard} = this.props
    dashboardActions.updateDashboardCell(dashboard, newCell)
  }

  handleDeleteDashboardCell = cell => {
    const {dashboardActions, dashboard} = this.props
    dashboardActions.deleteDashboardCellAsync(dashboard, cell)
  }

  handleSelectTemplate = templateID => value => {
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

  handleSaveTemplateVariables = async templates => {
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

  handleRunQueryFailure = error => {
    console.error(error)
    this.props.errorThrown(error)
  }

  handleToggleTempVarControls = () => {
    this.props.templateControlBarVisibilityToggled()
  }

  handleZoomedTimeRange = (zoomedLower, zoomedUpper) => {
    const {dashboardActions, location} = this.props
    const zoomedTimeRange = {lower: zoomedLower, upper: zoomedUpper}
    dashboardActions.setZoomedTimeRangeAsync(zoomedTimeRange, location)
  }

  setScrollTop = event => {
    this.setState({scrollTop: event.target.scrollTop})
  }

  render() {
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
}

const {arrayOf, bool, func, number, shape, string} = PropTypes

DashboardPage.propTypes = {
  source: shape({
    links: shape({
      proxy: string,
      self: string,
    }),
  }).isRequired,
  sources: arrayOf(shape({})).isRequired,
  params: shape({
    sourceID: string.isRequired,
    dashboardID: string.isRequired,
  }).isRequired,
  location: shape({
    pathname: string.isRequired,
    query: shape({}),
  }).isRequired,
  dashboard: shape({}),
  dashboardActions: shape({
    putDashboard: func.isRequired,
    getDashboardsNamesAsync: func.isRequired,
    getDashboardWithHydratedAndSyncedTempVarsAsync: func.isRequired,
    setTimeRange: func.isRequired,
    addDashboardCellAsync: func.isRequired,
  }).isRequired,
  dashboards: arrayOf(
    shape({
      id: number.isRequired,
      cells: arrayOf(shape({})).isRequired,
      templates: arrayOf(
        shape({
          type: string.isRequired,
          tempVar: string.isRequired,
          query: shape({
            db: string,
            rp: string,
            influxql: string,
          }),
          values: arrayOf(
            shape({
              value: string.isRequired,
              selected: bool.isRequired,
              type: string.isRequired,
            })
          ),
        })
      ),
    })
  ),
  handleChooseAutoRefresh: func.isRequired,
  autoRefresh: number.isRequired,
  templateControlBarVisibilityToggled: func.isRequired,
  timeRange: shape({
    upper: string,
    lower: string,
  }),
  zoomedTimeRange: shape({
    upper: string,
    lower: string,
  }),
  showTemplateControlBar: bool.isRequired,
  inPresentationMode: bool.isRequired,
  handleClickPresentationButton: func,
  cellQueryStatus: shape({
    queryID: string,
    status: shape(),
  }).isRequired,
  errorThrown: func,
  manualRefresh: number.isRequired,
  onManualRefresh: func.isRequired,
  meRole: string,
  isUsingAuth: bool.isRequired,
  router: shape().isRequired,
  notify: func.isRequired,
  getAnnotationsAsync: func.isRequired,
  handleShowCellEditorOverlay: func.isRequired,
  handleHideCellEditorOverlay: func.isRequired,
  handleDismissEditingAnnotation: func.isRequired,
  selectedCell: shape({}),
  thresholdsListType: string.isRequired,
  thresholdsListColors: colorsNumberSchema.isRequired,
  gaugeColors: colorsNumberSchema.isRequired,
  lineColors: colorsStringSchema.isRequired,
  handleShowOverlay: func.isRequired,
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
  handleClickPresentationButton: presentationButtonDispatcher(dispatch),
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
