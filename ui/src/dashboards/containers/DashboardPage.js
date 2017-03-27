import React, {PropTypes} from 'react'
import {Link} from 'react-router'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import CellEditorOverlay from 'src/dashboards/components/CellEditorOverlay'
import Header from 'src/dashboards/components/DashboardHeader'
import EditHeader from 'src/dashboards/components/DashboardHeaderEdit'
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
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string,
        self: PropTypes.string,
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
      getDashboards: func.isRequired,
      setDashboard: func.isRequired,
      setTimeRange: func.isRequired,
      addDashboardCellAsync: func.isRequired,
      editDashboardCell: func.isRequired,
      renameDashboardCell: func.isRequired,
    }).isRequired,
    dashboards: arrayOf(shape({
      id: number.isRequired,
      cells: arrayOf(shape({})).isRequired,
    })).isRequired,
    dashboard: shape({
      id: number.isRequired,
      cells: arrayOf(shape({})).isRequired,
    }).isRequired,
    handleChooseAutoRefresh: func.isRequired,
    autoRefresh: number.isRequired,
    timeRange: shape({}).isRequired,
    inPresentationMode: bool.isRequired,
    handleClickPresentationButton: func,
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
    return {source: this.props.source};
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
      dashboardActions: {getDashboards},
    } = this.props;

    getDashboards(dashboardID)
  },

  componentWillReceiveProps(nextProps) {
    const {location: {pathname}} = this.props
    const {
      location: {pathname: nextPathname},
      params: {dashboardID: nextID},
      dashboardActions: {setDashboard},
    } = nextProps

    if (nextPathname.pathname === pathname) {
      return
    }

    setDashboard(nextID)
  },

  handleDismissOverlay() {
    this.setState({selectedCell: null})
  },

  handleSaveEditedCell(newCell) {
    this.props.dashboardActions.updateDashboardCell(newCell)
    .then(this.handleDismissOverlay)
  },

  handleSummonOverlayTechnologies(cell) {
    this.setState({selectedCell: cell})
  },

  handleChooseTimeRange({lower}) {
    this.props.dashboardActions.setTimeRange({lower, upper: null})
  },

  handleUpdatePosition(cells) {
    this.props.dashboardActions.updateDashboardCells(cells)
    this.props.dashboardActions.putDashboard()
  },

  handleAddCell() {
    const {dashboard} = this.props
    this.props.dashboardActions.addDashboardCellAsync(dashboard)
  },

  handleEditDashboard() {
    this.setState({isEditMode: true})
  },

  handleCancelEditDashboard() {
    this.setState({isEditMode: false})
  },

  handleRenameDashboard(name) {
    this.setState({isEditMode: false})
    const {dashboard} = this.props
    const newDashboard = {...dashboard, name}
    this.props.dashboardActions.updateDashboard(newDashboard)
    this.props.dashboardActions.putDashboard()
  },

  // Places cell into editing mode.
  handleEditDashboardCell(x, y, isEditing) {
    return () => {
      this.props.dashboardActions.editDashboardCell(x, y, !isEditing) /* eslint-disable no-negated-condition */
    }
  },

  handleRenameDashboardCell(x, y) {
    return (evt) => {
      this.props.dashboardActions.renameDashboardCell(x, y, evt.target.value)
    }
  },

  handleUpdateDashboardCell(newCell) {
    return () => {
      this.props.dashboardActions.editDashboardCell(newCell.x, newCell.y, false)
      this.props.dashboardActions.putDashboard()
    }
  },

  handleDeleteDashboardCell(cell) {
    this.props.dashboardActions.deleteDashboardCellAsync(cell)
  },

  render() {
    const {
      dashboards,
      dashboard,
      params: {sourceID},
      inPresentationMode,
      handleClickPresentationButton,
      source,
      handleChooseAutoRefresh,
      autoRefresh,
      timeRange,
    } = this.props

    const {
      selectedCell,
      isEditMode,
    } = this.state

    return (
      <div className="page">
        {
          selectedCell ?
            <CellEditorOverlay
              cell={selectedCell}
              autoRefresh={autoRefresh}
              timeRange={timeRange}
              onCancel={this.handleDismissOverlay}
              onSave={this.handleSaveEditedCell}
            /> :
            null
        }
        {
          isEditMode ?
            <EditHeader
              dashboard={dashboard}
              onCancel={this.handleCancelEditDashboard}
              onSave={this.handleRenameDashboard}
            /> :
            <Header
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
              {(dashboards).map((d, i) => {
                return (
                  <li key={i}>
                    <Link to={`/sources/${sourceID}/dashboards/${d.id}`} className="role-option">
                      {d.name}
                    </Link>
                  </li>
                );
              })}
            </Header>
        }
        <Dashboard
          dashboard={dashboard}
          inPresentationMode={inPresentationMode}
          source={source}
          autoRefresh={autoRefresh}
          timeRange={timeRange}
          onPositionChange={this.handleUpdatePosition}
          onEditCell={this.handleEditDashboardCell}
          onRenameCell={this.handleRenameDashboardCell}
          onUpdateCell={this.handleUpdateDashboardCell}
          onDeleteCell={this.handleDeleteDashboardCell}
          onSummonOverlayTechnologies={this.handleSummonOverlayTechnologies}
        />
      </div>
    );
  },
});

const mapStateToProps = (state) => {
  const {
    app: {
      ephemeral: {inPresentationMode},
      persisted: {autoRefresh},
    },
    dashboardUI: {
      dashboards,
      dashboard,
      timeRange,
    },
  } = state

  return {
    dashboards,
    dashboard,
    autoRefresh,
    timeRange,
    inPresentationMode,
  }
}

const mapDispatchToProps = (dispatch) => ({
  handleChooseAutoRefresh: bindActionCreators(setAutoRefresh, dispatch),
  handleClickPresentationButton: presentationButtonDispatcher(dispatch),
  dashboardActions: bindActionCreators(dashboardActionCreators, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(DashboardPage);
