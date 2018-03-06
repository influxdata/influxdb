import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {withRouter} from 'react-router'
import {bindActionCreators} from 'redux'

import _ from 'lodash'
import Dygraph from 'src/external/dygraph'

import {isUserAuthorized, EDITOR_ROLE} from 'src/auth/Authorized'

import OverlayTechnologies from 'shared/components/OverlayTechnologies'
import CellEditorOverlay from 'src/dashboards/components/CellEditorOverlay'
import DashboardHeader from 'src/dashboards/components/DashboardHeader'
import Dashboard from 'src/dashboards/components/Dashboard'
import TemplateVariableManager from 'src/dashboards/components/template_variables/Manager'
import ManualRefresh from 'src/shared/components/ManualRefresh'

import {errorThrown as errorThrownAction} from 'shared/actions/errors'
import {publishNotification} from 'shared/actions/notifications'
import idNormalizer, {TYPE_ID} from 'src/normalizers/id'

import * as dashboardActionCreators from 'src/dashboards/actions'
import * as annotationActions from 'shared/actions/annotations'

import {
  showCellEditorOverlay,
  hideCellEditorOverlay,
} from 'src/dashboards/actions/cellEditorOverlay'

import {
  setAutoRefresh,
  templateControlBarVisibilityToggled as templateControlBarVisibilityToggledAction,
} from 'shared/actions/app'
import {presentationButtonDispatcher} from 'shared/dispatchers'
import {DASHBOARD_LAYOUT_ROW_HEIGHT} from 'shared/constants'

const FORMAT_INFLUXQL = 'influxql'
const defaultTimeRange = {
  upper: null,
  lower: 'now() - 15m',
  seconds: 900,
  format: FORMAT_INFLUXQL,
}

class DashboardPage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      isEditMode: false,
      selectedCell: null,
      isTemplating: false,
      zoomedTimeRange: {zoomedLower: null, zoomedUpper: null},
      scrollTop: 0,
      windowHeight: window.innerHeight,
      hoverTime: 0,
    }
  }

  dygraphs = []

  async componentDidMount() {
    const {
      params: {dashboardID},
      dashboardActions: {
        getDashboardsAsync,
        updateTempVarValues,
        putDashboardByID,
      },
      source,
      meRole,
      isUsingAuth,
      router,
      notify,
      getAnnotationsAsync,
      timeRange,
    } = this.props

    getAnnotationsAsync(
      source.links.annotations,
      Date.now() - timeRange.seconds * 1000
    )
    window.addEventListener('resize', this.handleWindowResize, true)

    const dashboards = await getDashboardsAsync()
    const dashboard = dashboards.find(
      d => d.id === idNormalizer(TYPE_ID, dashboardID)
    )

    if (!dashboard) {
      router.push(`/sources/${source.id}/dashboards`)
      return notify('error', `Dashboard ${dashboardID} could not be found`)
    }

    // Refresh and persists influxql generated template variable values.
    // If using auth and role is Viewer, temp vars will be stale until dashboard
    // is refactored so as not to require a write operation (a PUT in this case)
    if (!isUsingAuth || isUserAuthorized(meRole, EDITOR_ROLE)) {
      await updateTempVarValues(source, dashboard)
      await putDashboardByID(dashboardID)
    }
  }

  handleWindowResize = () => {
    this.setState({windowHeight: window.innerHeight})
  }

  componentWillUnMount() {
    window.removeEventListener('resize', this.handleWindowResize, true)
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

  handleOpenTemplateManager = () => {
    this.setState({isTemplating: true})
  }

  handleCloseTemplateManager = isEdited => () => {
    if (
      !isEdited ||
      (isEdited && confirm('Do you want to close without saving?')) // eslint-disable-line no-alert
    ) {
      this.setState({isTemplating: false})
    }
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
    } = this.props
    dashboardActions.setDashTimeV1(dashboard.id, {
      ...timeRange,
      format: FORMAT_INFLUXQL,
    })
    getAnnotationsAsync(
      source.links.annotations,
      Date.now() - timeRange.seconds * 1000
    )
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

  handleEditDashboard = () => {
    this.setState({isEditMode: true})
  }

  handleCancelEditDashboard = () => {
    this.setState({isEditMode: false})
  }

  handleRenameDashboard = name => {
    const {dashboardActions, dashboard} = this.props
    this.setState({isEditMode: false})
    const newDashboard = {...dashboard, name}

    dashboardActions.updateDashboard(newDashboard)
    dashboardActions.putDashboard(newDashboard)
  }

  handleUpdateDashboardCell = newCell => () => {
    const {dashboardActions, dashboard} = this.props
    dashboardActions.updateDashboardCell(dashboard, newCell)
  }

  handleDeleteDashboardCell = cell => {
    const {dashboardActions, dashboard} = this.props
    dashboardActions.deleteDashboardCellAsync(dashboard, cell)
  }

  handleSelectTemplate = templateID => values => {
    const {dashboardActions, dashboard, params: {dashboardID}} = this.props
    dashboardActions.templateVariableSelected(dashboard.id, templateID, [
      values,
    ])
    dashboardActions.putDashboardByID(dashboardID)
  }

  handleEditTemplateVariables = (
    templates,
    onSaveTemplatesSuccess
  ) => async () => {
    const {dashboardActions, dashboard} = this.props

    try {
      await dashboardActions.putDashboard({
        ...dashboard,
        templates,
      })
      onSaveTemplatesSuccess()
    } catch (error) {
      console.error(error)
    }
  }

  handleRunQueryFailure = error => {
    console.error(error)
    this.props.errorThrown(error)
  }

  synchronizer = dygraph => {
    const dygraphs = [...this.dygraphs, dygraph].filter(d => d.graphDiv)
    const {dashboards, params: {dashboardID}} = this.props

    const dashboard = dashboards.find(
      d => d.id === idNormalizer(TYPE_ID, dashboardID)
    )

    // Get only the graphs that can sync the hover line
    const graphsToSync = dashboard.cells.filter(c => c.type !== 'single-stat')

    if (
      dashboard &&
      dygraphs.length === graphsToSync.length &&
      dygraphs.length > 1
    ) {
      Dygraph.synchronize(dygraphs, {
        selection: true,
        zoom: false,
        range: false,
      })
    }

    this.dygraphs = dygraphs
  }

  handleSetHoverTime = hoverTime => {
    this.setState({hoverTime})
  }

  handleToggleTempVarControls = () => {
    this.props.templateControlBarVisibilityToggled()
  }

  handleZoomedTimeRange = (zoomedLower, zoomedUpper) => {
    this.setState({zoomedTimeRange: {zoomedLower, zoomedUpper}})
  }

  setScrollTop = event => {
    this.setState({scrollTop: event.target.scrollTop})
  }

  render() {
    const {zoomedTimeRange, hoverTime} = this.state
    const {zoomedLower, zoomedUpper} = zoomedTimeRange
    const {
      source,
      sources,
      timeRange,
      timeRange: {lower, upper},
      showTemplateControlBar,
      dashboard,
      dashboards,
      gaugeColors,
      autoRefresh,
      selectedCell,
      manualRefresh,
      onManualRefresh,
      cellQueryStatus,
      singleStatType,
      singleStatColors,
      dashboardActions,
      inPresentationMode,
      handleChooseAutoRefresh,
      handleShowCellEditorOverlay,
      handleHideCellEditorOverlay,
      handleClickPresentationButton,
      params: {sourceID, dashboardID},
    } = this.props

    const low = zoomedLower ? zoomedLower : lower
    const up = zoomedUpper ? zoomedUpper : upper

    const lowerType = low && low.includes(':') ? 'timeStamp' : 'constant'
    const upperType = up && up.includes(':') ? 'timeStamp' : 'constant'

    const dashboardTime = {
      id: 'dashtime',
      tempVar: ':dashboardTime:',
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
      tempVar: ':upperDashboardTime:',
      type: upperType,
      values: [
        {
          value: up || 'now()',
          type: upperType,
          selected: true,
        },
      ],
    }

    const interval = {
      id: 'interval',
      type: 'autoGroupBy',
      tempVar: ':interval:',
      label: 'automatically determine the best group by time',
      values: [
        {
          value: '1000', // pixels
          type: 'resolution',
          selected: true,
        },
        {
          value: '3',
          type: 'pointsPerPixel',
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

    const {isEditMode, isTemplating} = this.state

    const names = dashboards.map(d => ({
      name: d.name,
      link: `/sources/${sourceID}/dashboards/${d.id}`,
    }))

    return (
      <div className="page">
        {isTemplating
          ? <OverlayTechnologies>
              <TemplateVariableManager
                source={source}
                templates={dashboard.templates}
                onClose={this.handleCloseTemplateManager}
                onRunQueryFailure={this.handleRunQueryFailure}
                onEditTemplateVariables={this.handleEditTemplateVariables}
              />
            </OverlayTechnologies>
          : null}
        {selectedCell
          ? <CellEditorOverlay
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
              singleStatType={singleStatType}
              singleStatColors={singleStatColors}
              gaugeColors={gaugeColors}
            />
          : null}
        <DashboardHeader
          names={names}
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
        {dashboard
          ? <Dashboard
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
              synchronizer={this.synchronizer}
              hoverTime={hoverTime}
              onSetHoverTime={this.handleSetHoverTime}
              inPresentationMode={inPresentationMode}
              onPositionChange={this.handleUpdatePosition}
              onSelectTemplate={this.handleSelectTemplate}
              onDeleteCell={this.handleDeleteDashboardCell}
              showTemplateControlBar={showTemplateControlBar}
              onOpenTemplateManager={this.handleOpenTemplateManager}
              templatesIncludingDashTime={templatesIncludingDashTime}
              onSummonOverlayTechnologies={handleShowCellEditorOverlay}
            />
          : null}
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
    getDashboardsAsync: func.isRequired,
    setTimeRange: func.isRequired,
    addDashboardCellAsync: func.isRequired,
    editDashboardCell: func.isRequired,
    cancelEditCell: func.isRequired,
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
  selectedCell: shape({}),
  singleStatType: string.isRequired,
  singleStatColors: arrayOf(shape({}).isRequired).isRequired,
  gaugeColors: arrayOf(shape({}).isRequired).isRequired,
}

const mapStateToProps = (state, {params: {dashboardID}}) => {
  const {
    app: {
      ephemeral: {inPresentationMode},
      persisted: {autoRefresh, showTemplateControlBar},
    },
    dashboardUI: {dashboards, cellQueryStatus},
    sources,
    dashTimeV1,
    auth: {me, isUsingAuth},
    cellEditorOverlay: {cell, singleStatType, singleStatColors, gaugeColors},
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
    dashboards,
    autoRefresh,
    isUsingAuth,
    cellQueryStatus,
    inPresentationMode,
    showTemplateControlBar,
    selectedCell,
    singleStatType,
    singleStatColors,
    gaugeColors,
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
  notify: bindActionCreators(publishNotification, dispatch),
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
})

export default connect(mapStateToProps, mapDispatchToProps)(
  ManualRefresh(withRouter(DashboardPage))
)
