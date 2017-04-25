import React, {PropTypes, Component} from 'react'
import {Link} from 'react-router'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import OverlayTechnologies from 'src/shared/components/OverlayTechnologies'
import CellEditorOverlay from 'src/dashboards/components/CellEditorOverlay'
import DashboardHeader from 'src/dashboards/components/DashboardHeader'
import DashboardHeaderEdit from 'src/dashboards/components/DashboardHeaderEdit'
import Dashboard from 'src/dashboards/components/Dashboard'
import TemplateVariableManager
  from 'src/dashboards/components/TemplateVariableManager'

import * as dashboardActionCreators from 'src/dashboards/actions'

import {setAutoRefresh} from 'shared/actions/app'
import {presentationButtonDispatcher} from 'shared/dispatchers'

class DashboardPage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      selectedCell: null,
      isEditMode: false,
      isTemplating: false,
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
    this.handleRenameDashboardCell = ::this.handleRenameDashboardCell
    this.handleUpdateDashboardCell = ::this.handleUpdateDashboardCell
    this.handleCloseTemplateManager = ::this.handleCloseTemplateManager
    this.handleSummonOverlayTechnologies = ::this
      .handleSummonOverlayTechnologies
    this.handleRunTemplateVariableQuery = ::this.handleRunTemplateVariableQuery
    this.handleSelectTemplate = ::this.handleSelectTemplate
    this.handleEditTemplateVariables = ::this.handleEditTemplateVariables
    this.handleRunQueryFailure = ::this.handleRunQueryFailure
  }

  componentDidMount() {
    const {
      params: {dashboardID},
      dashboardActions: {getDashboardsAsync},
    } = this.props

    getDashboardsAsync(dashboardID)
  }

  handleOpenTemplateManager() {
    this.setState({isTemplating: true})
  }

  handleCloseTemplateManager() {
    this.setState({isTemplating: false})
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

  handleChooseTimeRange({lower}) {
    this.props.dashboardActions.setTimeRange({lower, upper: null})
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

  handleRenameDashboardCell(x, y) {
    return evt => {
      this.props.dashboardActions.renameDashboardCell(
        this.getActiveDashboard(),
        x,
        y,
        evt.target.value
      )
    }
  }

  handleUpdateDashboardCell(newCell) {
    return () => {
      this.props.dashboardActions.editDashboardCell(
        this.getActiveDashboard(),
        newCell.x,
        newCell.y,
        false
      )
      this.props.dashboardActions.putDashboard(this.getActiveDashboard())
    }
  }

  handleDeleteDashboardCell(cell) {
    this.props.dashboardActions.deleteDashboardCellAsync(cell)
  }

  handleSelectTemplate(templateID, values) {
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

  // TODO: make this work over array of template variables onSave in TVM
  handleEditTemplateVariables(staleTemplateVariable, editedTemplateVariable) {
    //   // this.props.dashboardActions.editTemplateVariableAsync(
    //   //   this.props.params.dashboardID,
    //   //   staleTemplateVariable,
    //   //   editedTemplateVariable
    //   // )
    //   console.log('hello')
  }

  handleRunQueryFailure(error) {
    console.error(error)
    // this.props.errorThrown(error)
  }

  getActiveDashboard() {
    const {params: {dashboardID}, dashboards} = this.props
    return dashboards.find(d => d.id === +dashboardID)
  }

  render() {
    const {
      source,
      timeRange,
      dashboards,
      autoRefresh,
      cellQueryStatus,
      dashboardActions,
      inPresentationMode,
      handleChooseAutoRefresh,
      handleClickPresentationButton,
      params: {sourceID, dashboardID},
    } = this.props

    const dashboard = dashboards.find(d => d.id === +dashboardID)

    const {selectedCell, isEditMode, isTemplating} = this.state

    return (
      <div className="page">
        {isTemplating
          ? <OverlayTechnologies>
              <TemplateVariableManager
                onClose={this.handleCloseTemplateManager}
                onEditTemplateVariables={this.handleEditTemplateVariables}
                handleClickOutside={this.handleCloseTemplateManager}
                source={source}
                templates={dashboard.templates}
                onRunQueryFailure={this.handleRunQueryFailure}
              />
            </OverlayTechnologies>
          : null}
        {selectedCell
          ? <CellEditorOverlay
              source={source}
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
            >
              {dashboards
                ? dashboards.map((d, i) => (
                    <li key={i}>
                      <Link
                        to={`/sources/${sourceID}/dashboards/${d.id}`}
                        className="role-option"
                      >
                        {d.name}
                      </Link>
                    </li>
                  ))
                : null}
            </DashboardHeader>}
        {dashboard
          ? <Dashboard
              source={source}
              dashboard={dashboard}
              timeRange={timeRange}
              autoRefresh={autoRefresh}
              onAddCell={this.handleAddCell}
              inPresentationMode={inPresentationMode}
              onEditCell={this.handleEditDashboardCell}
              onPositionChange={this.handleUpdatePosition}
              onDeleteCell={this.handleDeleteDashboardCell}
              onRenameCell={this.handleRenameDashboardCell}
              onUpdateCell={this.handleUpdateDashboardCell}
              onOpenTemplateManager={this.handleOpenTemplateManager}
              onSummonOverlayTechnologies={this.handleSummonOverlayTechnologies}
              onSelectTemplate={this.handleSelectTemplate}
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
    renameDashboardCell: func.isRequired,
  }).isRequired,
  dashboards: arrayOf(
    shape({
      id: number.isRequired,
      cells: arrayOf(shape({})).isRequired,
      templates: arrayOf(
        shape({
          type: string.isRequired,
          label: string.isRequired,
          tempVar: string.isRequired,
          query: shape({
            db: string.isRequired,
            rp: string,
            influxql: string.isRequired,
          }),
          values: arrayOf(
            shape({
              type: string.isRequired,
              value: string.isRequired,
              selected: bool,
            })
          ).isRequired,
        })
      ),
    })
  ),
  handleChooseAutoRefresh: func.isRequired,
  autoRefresh: number.isRequired,
  timeRange: shape({}).isRequired,
  inPresentationMode: bool.isRequired,
  handleClickPresentationButton: func,
  cellQueryStatus: shape({
    queryID: string,
    status: shape(),
  }).isRequired,
}

const mapStateToProps = state => {
  const {
    app: {ephemeral: {inPresentationMode}, persisted: {autoRefresh}},
    dashboardUI: {dashboards, timeRange, cellQueryStatus},
  } = state

  return {
    dashboards,
    autoRefresh,
    timeRange,
    inPresentationMode,
    cellQueryStatus,
  }
}

const mapDispatchToProps = dispatch => ({
  handleChooseAutoRefresh: bindActionCreators(setAutoRefresh, dispatch),
  handleClickPresentationButton: presentationButtonDispatcher(dispatch),
  dashboardActions: bindActionCreators(dashboardActionCreators, dispatch),
  // errorThrown: bindActionCreators(errorThrownAction, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(DashboardPage)
