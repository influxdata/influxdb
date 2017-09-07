import React, {PropTypes, Component} from 'react'
import {Link} from 'react-router'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import Dygraph from 'src/external/dygraph'

import OverlayTechnologies from 'shared/components/OverlayTechnologies'
import CellEditorOverlay from 'src/dashboards/components/CellEditorOverlay'
import DashboardHeader from 'src/dashboards/components/DashboardHeader'
import DashboardHeaderEdit from 'src/dashboards/components/DashboardHeaderEdit'
import Dashboard from 'src/dashboards/components/Dashboard'
import TemplateVariableManager from 'src/dashboards/components/template_variables/Manager'

import {errorThrown as errorThrownAction} from 'shared/actions/errors'

import * as dashboardActionCreators from 'src/dashboards/actions'

import {
  setAutoRefresh,
  templateControlBarVisibilityToggled as templateControlBarVisibilityToggledAction,
} from 'shared/actions/app'
import {presentationButtonDispatcher} from 'shared/dispatchers'

class DashboardPage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      dygraphs: [],
      isEditMode: false,
      selectedCell: null,
      isTemplating: false,
      zoomedTimeRange: {zoomedLower: null, zoomedUpper: null},
    }
  }

  async componentDidMount() {
    const {
      params: {dashboardID},
      dashboardActions: {
        getDashboardsAsync,
        updateTempVarValues,
        putDashboardByID,
      },
      source,
    } = this.props

    const dashboards = await getDashboardsAsync()
    const dashboard = dashboards.find(d => d.id === +dashboardID)

    // Refresh and persists influxql generated template variable values
    await updateTempVarValues(source, dashboard)
    await putDashboardByID(dashboardID)
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

  handleDismissOverlay = () => {
    this.setState({selectedCell: null})
  }

  handleSaveEditedCell = newCell => {
    this.props.dashboardActions
      .updateDashboardCell(this.getActiveDashboard(), newCell)
      .then(this.handleDismissOverlay)
  }

  handleSummonOverlayTechnologies = cell => {
    this.setState({selectedCell: cell})
  }

  handleChooseTimeRange = timeRange => {
    this.props.dashboardActions.setTimeRange(timeRange)
  }

  handleUpdatePosition = cells => {
    const newDashboard = {...this.getActiveDashboard(), cells}
    this.props.dashboardActions.updateDashboard(newDashboard)
    this.props.dashboardActions.putDashboard(newDashboard)
  }

  handleAddCell = () => {
    this.props.dashboardActions.addDashboardCellAsync(this.getActiveDashboard())
  }

  handleEditDashboard = () => {
    this.setState({isEditMode: true})
  }

  handleCancelEditDashboard = () => {
    this.setState({isEditMode: false})
  }

  handleRenameDashboard = name => {
    this.setState({isEditMode: false})
    const newDashboard = {...this.getActiveDashboard(), name}
    this.props.dashboardActions.updateDashboard(newDashboard)
    this.props.dashboardActions.putDashboard(newDashboard)
  }

  // Places cell into editing mode.
  handleEditDashboardCell = (x, y, isEditing) => {
    return () => {
      this.props.dashboardActions.editDashboardCell(
        this.getActiveDashboard(),
        x,
        y,
        !isEditing
      ) /* eslint-disable no-negated-condition */
    }
  }

  handleUpdateDashboardCell = newCell => {
    return () => {
      this.props.dashboardActions.updateDashboardCell(
        this.getActiveDashboard(),
        newCell
      )
    }
  }

  handleDeleteDashboardCell = cell => {
    const dashboard = this.getActiveDashboard()
    this.props.dashboardActions.deleteDashboardCellAsync(dashboard, cell)
  }

  handleSelectTemplate = templateID => values => {
    const {params: {dashboardID}} = this.props
    this.props.dashboardActions.templateVariableSelected(
      +dashboardID,
      templateID,
      [values]
    )
  }

  handleEditTemplateVariables = (
    templates,
    onSaveTemplatesSuccess
  ) => async () => {
    try {
      await this.props.dashboardActions.putDashboard({
        ...this.getActiveDashboard(),
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
    const dygraphs = [...this.state.dygraphs, dygraph]
    const {dashboards, params} = this.props
    const dashboard = dashboards.find(d => d.id === +params.dashboardID)
    if (
      dashboard &&
      dygraphs.length === dashboard.cells.length &&
      dashboard.cells.length > 1
    ) {
      Dygraph.synchronize(dygraphs, {
        selection: true,
        zoom: false,
        range: false,
      })
    }
    this.setState({dygraphs})
  }

  handleToggleTempVarControls = () => {
    this.props.templateControlBarVisibilityToggled()
  }

  handleCancelEditCell = cellID => {
    this.props.dashboardActions.cancelEditCell(
      this.getActiveDashboard().id,
      cellID
    )
  }

  handleZoomedTimeRange = (zoomedLower, zoomedUpper) => {
    this.setState({zoomedTimeRange: {zoomedLower, zoomedUpper}})
  }

  getActiveDashboard() {
    const {params: {dashboardID}, dashboards} = this.props
    return dashboards.find(d => d.id === +dashboardID)
  }

  render() {
    const {zoomedTimeRange} = this.state
    const {zoomedLower, zoomedUpper} = zoomedTimeRange

    const {
      source,
      timeRange,
      timeRange: {lower, upper},
      showTemplateControlBar,
      dashboards,
      autoRefresh,
      cellQueryStatus,
      dashboardActions,
      inPresentationMode,
      handleChooseAutoRefresh,
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

    // this controls the auto group by behavior
    const interval = {
      id: 'interval',
      type: 'constant',
      tempVar: ':interval:',
      resolution: 1000,
      reportingInterval: 10000000000,
      values: [],
    }

    const dashboard = this.getActiveDashboard()

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

    const {selectedCell, isEditMode, isTemplating} = this.state

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
              cell={selectedCell}
              timeRange={timeRange}
              autoRefresh={autoRefresh}
              dashboardID={dashboardID}
              queryStatus={cellQueryStatus}
              onSave={this.handleSaveEditedCell}
              onCancel={this.handleDismissOverlay}
              templates={templatesIncludingDashTime}
              editQueryStatus={dashboardActions.editCellQueryStatus}
            />
          : null}
        {isEditMode
          ? <DashboardHeaderEdit
              dashboard={dashboard}
              onSave={this.handleRenameDashboard}
              onCancel={this.handleCancelEditDashboard}
            />
          : <DashboardHeader
              source={source}
              sourceID={sourceID}
              dashboard={dashboard}
              timeRange={timeRange}
              zoomedTimeRange={zoomedTimeRange}
              autoRefresh={autoRefresh}
              isHidden={inPresentationMode}
              onAddCell={this.handleAddCell}
              onEditDashboard={this.handleEditDashboard}
              buttonText={dashboard ? dashboard.name : ''}
              showTemplateControlBar={showTemplateControlBar}
              handleChooseAutoRefresh={handleChooseAutoRefresh}
              handleChooseTimeRange={this.handleChooseTimeRange}
              onToggleTempVarControls={this.handleToggleTempVarControls}
              handleClickPresentationButton={handleClickPresentationButton}
            >
              {dashboards
                ? dashboards.map((d, i) =>
                    <li className="dropdown-item" key={i}>
                      <Link to={`/sources/${sourceID}/dashboards/${d.id}`}>
                        {d.name}
                      </Link>
                    </li>
                  )
                : null}
            </DashboardHeader>}
        {dashboard
          ? <Dashboard
              source={source}
              dashboard={dashboard}
              timeRange={timeRange}
              autoRefresh={autoRefresh}
              onZoom={this.handleZoomedTimeRange}
              onAddCell={this.handleAddCell}
              synchronizer={this.synchronizer}
              inPresentationMode={inPresentationMode}
              onEditCell={this.handleEditDashboardCell}
              onPositionChange={this.handleUpdatePosition}
              onSelectTemplate={this.handleSelectTemplate}
              onCancelEditCell={this.handleCancelEditCell}
              onDeleteCell={this.handleDeleteDashboardCell}
              onUpdateCell={this.handleUpdateDashboardCell}
              showTemplateControlBar={showTemplateControlBar}
              onOpenTemplateManager={this.handleOpenTemplateManager}
              templatesIncludingDashTime={templatesIncludingDashTime}
              onSummonOverlayTechnologies={this.handleSummonOverlayTechnologies}
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
  params: shape({
    sourceID: string.isRequired,
    dashboardID: string.isRequired,
  }).isRequired,
  location: shape({
    pathname: string.isRequired,
    query: shape({}),
  }).isRequired,
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
  timeRange: shape({}).isRequired,
  showTemplateControlBar: bool.isRequired,
  inPresentationMode: bool.isRequired,
  handleClickPresentationButton: func,
  cellQueryStatus: shape({
    queryID: string,
    status: shape(),
  }).isRequired,
  errorThrown: func,
}

const mapStateToProps = state => {
  const {
    app: {
      ephemeral: {inPresentationMode},
      persisted: {autoRefresh, showTemplateControlBar},
    },
    dashboardUI: {dashboards, timeRange, cellQueryStatus},
  } = state

  return {
    dashboards,
    autoRefresh,
    timeRange,
    showTemplateControlBar,
    inPresentationMode,
    cellQueryStatus,
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
})

export default connect(mapStateToProps, mapDispatchToProps)(DashboardPage)
