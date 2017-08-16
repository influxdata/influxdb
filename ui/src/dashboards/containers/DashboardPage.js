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
      selectedCell: null,
      isEditMode: false,
      isTemplating: false,
      dygraphs: [],
    }

    this.handleAddCell = ::this.handleAddCell
    this.handleEditDashboard = ::this.handleEditDashboard
    this.handleSaveEditedCell = ::this.handleSaveEditedCell
    this.handleDismissOverlay = ::this.handleDismissOverlay
    this.handleUpdatePosition = ::this.handleUpdatePosition
    this.handleChooseTimeRange = ::this.handleChooseTimeRange
    this.handleRenameDashboard = ::this.handleRenameDashboard
    this.handleEditDashboardCell = ::this.handleEditDashboardCell
    this.handleCancelEditDashboard = ::this.handleCancelEditDashboard
    this.handleDeleteDashboardCell = ::this.handleDeleteDashboardCell
    this.handleOpenTemplateManager = ::this.handleOpenTemplateManager
    this.handleUpdateDashboardCell = ::this.handleUpdateDashboardCell
    this.handleSummonOverlayTechnologies = ::this
      .handleSummonOverlayTechnologies
    this.handleRunTemplateVariableQuery = ::this.handleRunTemplateVariableQuery
    this.handleEditTemplateVariables = ::this.handleEditTemplateVariables
    this.handleRunQueryFailure = ::this.handleRunQueryFailure
    this.handleToggleTempVarControls = ::this.handleToggleTempVarControls
    this.synchronizer = ::this.synchronizer
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

  handleOpenTemplateManager() {
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

  handleDismissOverlay() {
    this.setState({selectedCell: null})
  }

  handleSaveEditedCell(newCell) {
    this.props.dashboardActions
      .updateDashboardCell(this.getActiveDashboard(), newCell)
      .then(this.handleDismissOverlay)
  }

  handleSummonOverlayTechnologies(cell) {
    this.setState({selectedCell: cell})
  }

  handleChooseTimeRange(timeRange) {
    this.props.dashboardActions.setTimeRange(timeRange)
  }

  handleUpdatePosition(cells) {
    const newDashboard = {...this.getActiveDashboard(), cells}
    this.props.dashboardActions.updateDashboard(newDashboard)
    this.props.dashboardActions.putDashboard(newDashboard)
  }

  handleAddCell() {
    this.props.dashboardActions.addDashboardCellAsync(this.getActiveDashboard())
  }

  handleEditDashboard() {
    this.setState({isEditMode: true})
  }

  handleCancelEditDashboard() {
    this.setState({isEditMode: false})
  }

  handleRenameDashboard(name) {
    this.setState({isEditMode: false})
    const newDashboard = {...this.getActiveDashboard(), name}
    this.props.dashboardActions.updateDashboard(newDashboard)
    this.props.dashboardActions.putDashboard(newDashboard)
  }

  // Places cell into editing mode.
  handleEditDashboardCell(x, y, isEditing) {
    return () => {
      this.props.dashboardActions.editDashboardCell(
        this.getActiveDashboard(),
        x,
        y,
        !isEditing
      ) /* eslint-disable no-negated-condition */
    }
  }

  handleUpdateDashboardCell(newCell) {
    return () => {
      this.props.dashboardActions.updateDashboardCell(
        this.getActiveDashboard(),
        newCell
      )
    }
  }

  handleDeleteDashboardCell(cell) {
    const dashboard = this.getActiveDashboard()
    this.props.dashboardActions.deleteDashboardCellAsync(dashboard, cell)
  }

  handleSelectTemplate = templateID => values => {
    const {params: {dashboardID}} = this.props
    this.props.dashboardActions.templateVariableSelected(
      +dashboardID,
      templateID,
      values
    )
  }

  handleRunTemplateVariableQuery(
    templateVariable,
    {query, db, tempVars, type, tagKey, measurement}
  ) {
    const {source} = this.props
    this.props.dashboardActions.runTemplateVariableQueryAsync(
      templateVariable,
      {
        source,
        query,
        db,
        // rp, TODO
        tempVars,
        type,
        tagKey,
        measurement,
      }
    )
  }

  handleEditTemplateVariables(templates, onSaveTemplatesSuccess) {
    return async () => {
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
  }

  handleRunQueryFailure(error) {
    console.error(error)
    this.props.errorThrown(error)
  }

  synchronizer(dygraph) {
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

  handleToggleTempVarControls() {
    this.props.templateControlBarVisibilityToggled()
  }

  handleCancelEditCell(cellID) {
    this.props.dashboardActions.cancelEditCell(
      this.getActiveDashboard().id,
      cellID
    )
  }

  getActiveDashboard() {
    const {params: {dashboardID}, dashboards} = this.props
    return dashboards.find(d => d.id === +dashboardID)
  }

  render() {
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

    const lowerType = lower && lower.includes('Z') ? 'timeStamp' : 'constant'
    const upperType = upper && upper.includes('Z') ? 'timeStamp' : 'constant'

    const dashboardTime = {
      id: 'dashtime',
      tempVar: ':dashboardTime:',
      type: lowerType,
      values: [
        {
          value: lower,
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
          value: upper || 'now()',
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
                onClose={this.handleCloseTemplateManager}
                onEditTemplateVariables={this.handleEditTemplateVariables}
                source={source}
                templates={dashboard.templates}
                onRunQueryFailure={this.handleRunQueryFailure}
              />
            </OverlayTechnologies>
          : null}
        {selectedCell
          ? <CellEditorOverlay
              source={source}
              dashboardID={dashboardID}
              templates={templatesIncludingDashTime}
              cell={selectedCell}
              timeRange={timeRange}
              autoRefresh={autoRefresh}
              queryStatus={cellQueryStatus}
              onSave={this.handleSaveEditedCell}
              onCancel={this.handleDismissOverlay}
              editQueryStatus={dashboardActions.editCellQueryStatus}
            />
          : null}
        {isEditMode
          ? <DashboardHeaderEdit
              dashboard={dashboard}
              onCancel={this.handleCancelEditDashboard}
              onSave={this.handleRenameDashboard}
            />
          : <DashboardHeader
              buttonText={dashboard ? dashboard.name : ''}
              handleChooseAutoRefresh={handleChooseAutoRefresh}
              autoRefresh={autoRefresh}
              timeRange={timeRange}
              handleChooseTimeRange={this.handleChooseTimeRange}
              isHidden={inPresentationMode}
              handleClickPresentationButton={handleClickPresentationButton}
              dashboard={dashboard}
              sourceID={sourceID}
              source={source}
              onAddCell={this.handleAddCell}
              onEditDashboard={this.handleEditDashboard}
              onToggleTempVarControls={this.handleToggleTempVarControls}
              showTemplateControlBar={showTemplateControlBar}
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
              synchronizer={this.synchronizer}
              onAddCell={this.handleAddCell}
              onEditCell={this.handleEditDashboardCell}
              inPresentationMode={inPresentationMode}
              onPositionChange={this.handleUpdatePosition}
              onDeleteCell={this.handleDeleteDashboardCell}
              onUpdateCell={this.handleUpdateDashboardCell}
              onOpenTemplateManager={this.handleOpenTemplateManager}
              templatesIncludingDashTime={templatesIncludingDashTime}
              onSummonOverlayTechnologies={this.handleSummonOverlayTechnologies}
              onSelectTemplate={this.handleSelectTemplate}
              showTemplateControlBar={showTemplateControlBar}
              onCancelEditCell={::this.handleCancelEditCell}
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
