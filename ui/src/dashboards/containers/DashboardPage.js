import React, {PropTypes} from 'react'
import {Link} from 'react-router'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import CellEditorOverlay from 'src/dashboards/components/CellEditorOverlay'
import DashboardHeader from 'src/dashboards/components/DashboardHeader'
import DashboardHeaderEdit from 'src/dashboards/components/DashboardHeaderEdit'
import Dashboard from 'src/dashboards/components/Dashboard'

import * as dashboardActionCreators from 'src/dashboards/actions'

import {setAutoRefresh} from 'shared/actions/app'
import {presentationButtonDispatcher} from 'shared/dispatchers'

const {
  arrayOf,
  bool,
  func,
  number,
  shape,
  string,
} = PropTypes

const DashboardPage = React.createClass({
  propTypes: {
    source: shape({
      links: shape({
        proxy: string,
        self: string,
      }),
    }),
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
    dashboards: arrayOf(shape({
      id: number.isRequired,
      cells: arrayOf(shape({})).isRequired,
    })),
    handleChooseAutoRefresh: func.isRequired,
    autoRefresh: number.isRequired,
    timeRange: shape({}).isRequired,
    inPresentationMode: bool.isRequired,
    handleClickPresentationButton: func,
    cellQueryStatus: shape({
      queryID: string,
      status: shape(),
    }).isRequired,
  },

  childContextTypes: {
    source: shape({
      links: shape({
        proxy: string.isRequired,
        self: string.isRequired,
      }).isRequired,
    }).isRequired,
  },

  getChildContext() {
    return {source: this.props.source}
  },

  getInitialState() {
    return {
      selectedCell: null,
      isEditMode: false,
    }
  },

  componentDidMount() {
    const {
      params: {dashboardID},
      dashboardActions: {getDashboardsAsync},
    } = this.props

    getDashboardsAsync(dashboardID)
  },

  handleDismissOverlay() {
    this.setState({selectedCell: null})
  },

  handleSaveEditedCell(newCell) {
    this.props.dashboardActions.updateDashboardCell(this.getActiveDashboard(), newCell)
    .then(this.handleDismissOverlay)
  },

  handleSummonOverlayTechnologies(cell) {
    this.setState({selectedCell: cell})
  },

  handleChooseTimeRange({lower}) {
    this.props.dashboardActions.setTimeRange({lower, upper: null})
  },

  handleUpdatePosition(cells) {
    const newDashboard = {...this.getActiveDashboard(), cells}
    this.props.dashboardActions.updateDashboard(newDashboard)
    this.props.dashboardActions.putDashboard(newDashboard)
  },

  handleAddCell() {
    this.props.dashboardActions.addDashboardCellAsync(this.getActiveDashboard())
  },

  handleEditDashboard() {
    this.setState({isEditMode: true})
  },

  handleCancelEditDashboard() {
    this.setState({isEditMode: false})
  },

  handleRenameDashboard(name) {
    this.setState({isEditMode: false})
    const newDashboard = {...this.getActiveDashboard(), name}
    this.props.dashboardActions.updateDashboard(newDashboard)
    this.props.dashboardActions.putDashboard(newDashboard)
  },

  // Places cell into editing mode.
  handleEditDashboardCell(x, y, isEditing) {
    return () => {
      this.props.dashboardActions.editDashboardCell(this.getActiveDashboard(), x, y, !isEditing) /* eslint-disable no-negated-condition */
    }
  },

  handleRenameDashboardCell(x, y) {
    return (evt) => {
      this.props.dashboardActions.renameDashboardCell(this.getActiveDashboard(), x, y, evt.target.value)
    }
  },

  handleUpdateDashboardCell(newCell) {
    return () => {
      this.props.dashboardActions.editDashboardCell(this.getActiveDashboard(), newCell.x, newCell.y, false)
      this.props.dashboardActions.putDashboard(this.getActiveDashboard())
    }
  },

  handleDeleteDashboardCell(cell) {
    this.props.dashboardActions.deleteDashboardCellAsync(cell)
  },

  getActiveDashboard() {
    const {params: {dashboardID}, dashboards} = this.props
    return dashboards.find(d => d.id === +dashboardID)
  },

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

    const {
      selectedCell,
      isEditMode,
    } = this.state

    return (
      <div className="page">
        {
          selectedCell ?
            <CellEditorOverlay
              source={source}
              cell={selectedCell}
              autoRefresh={autoRefresh}
              timeRange={timeRange}
              onCancel={this.handleDismissOverlay}
              onSave={this.handleSaveEditedCell}
              editQueryStatus={dashboardActions.editCellQueryStatus}
              queryStatus={cellQueryStatus}
            /> :
            null
        }
        {
          isEditMode ?
            <DashboardHeaderEdit
              dashboard={dashboard}
              onCancel={this.handleCancelEditDashboard}
              onSave={this.handleRenameDashboard}
            /> :
            <DashboardHeader
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
              {
                dashboards ?
                dashboards.map((d, i) => {
                  return (
                    <li key={i}>
                      <Link to={`/sources/${sourceID}/dashboards/${d.id}`} className="role-option">
                        {d.name}
                      </Link>
                    </li>
                  )
                }) :
                null
              }
            </DashboardHeader>
        }
        {
          dashboard ?
          <Dashboard
            dashboard={dashboard}
            inPresentationMode={inPresentationMode}
            source={source}
            autoRefresh={autoRefresh}
            timeRange={timeRange}
            onAddCell={this.handleAddCell}
            onPositionChange={this.handleUpdatePosition}
            onEditCell={this.handleEditDashboardCell}
            onRenameCell={this.handleRenameDashboardCell}
            onUpdateCell={this.handleUpdateDashboardCell}
            onDeleteCell={this.handleDeleteDashboardCell}
            onSummonOverlayTechnologies={this.handleSummonOverlayTechnologies}
          /> :
          null
        }
      </div>
    )
  },
})

const mapStateToProps = (state) => {
  const {
    app: {
      ephemeral: {inPresentationMode},
      persisted: {autoRefresh},
    },
    dashboardUI: {
      dashboards,
      timeRange,
      cellQueryStatus,
    },
  } = state

  return {
    dashboards,
    autoRefresh,
    timeRange,
    inPresentationMode,
    cellQueryStatus,
  }
}

const mapDispatchToProps = (dispatch) => ({
  handleChooseAutoRefresh: bindActionCreators(setAutoRefresh, dispatch),
  handleClickPresentationButton: presentationButtonDispatcher(dispatch),
  dashboardActions: bindActionCreators(dashboardActionCreators, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(DashboardPage)
