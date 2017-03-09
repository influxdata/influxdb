import React, {PropTypes} from 'react'
import {Link} from 'react-router'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import Header from 'src/dashboards/components/DashboardHeader'
import EditHeader from 'src/dashboards/components/DashboardHeaderEdit'
import Dashboard from 'src/dashboards/components/Dashboard'
import timeRanges from 'hson!../../shared/data/timeRanges.hson'

import * as dashboardActionCreators from 'src/dashboards/actions'

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
      setEditMode: func.isRequired,
    }).isRequired,
    dashboards: arrayOf(shape({
      id: number.isRequired,
      cells: arrayOf(shape({})).isRequired,
    })).isRequired,
    dashboard: shape({
      id: number.isRequired,
      cells: arrayOf(shape({})).isRequired,
    }).isRequired,
    autoRefresh: number.isRequired,
    timeRange: shape({}).isRequired,
    inPresentationMode: bool.isRequired,
    isEditMode: bool.isRequired,
    handleClickPresentationButton: func,
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
      dashboardActions: {setDashboard, setEditMode},
    } = nextProps

    if (nextPathname.pathname === pathname) {
      return
    }

    setDashboard(nextID)
    setEditMode(nextPathname.includes('/edit'))
  },

  handleChooseTimeRange({lower}) {
    const timeRange = timeRanges.find((range) => range.queryValue === lower);
    this.props.dashboardActions.setTimeRange(timeRange)
  },

  handleUpdatePosition(cells) {
    this.props.dashboardActions.putDashboard({...this.props.dashboard, cells})
  },

  render() {
    const {
      dashboards,
      dashboard,
      params: {sourceID},
      inPresentationMode,
      isEditMode,
      handleClickPresentationButton,
      source,
      autoRefresh,
      timeRange,
    } = this.props

    return (
      <div className="page">
        {
          isEditMode ?
            <EditHeader dashboard={dashboard} onSave={() => {}} /> :
            <Header
              buttonText={dashboard ? dashboard.name : ''}
              autoRefresh={autoRefresh}
              timeRange={timeRange}
              handleChooseTimeRange={this.handleChooseTimeRange}
              isHidden={inPresentationMode}
              handleClickPresentationButton={handleClickPresentationButton}
              dashboard={dashboard}
              sourceID={sourceID}
              source={source}
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
          isEditMode={isEditMode}
          inPresentationMode={inPresentationMode}
          source={source}
          autoRefresh={autoRefresh}
          timeRange={timeRange}
          onPositionChange={this.handleUpdatePosition}
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
      isEditMode,
    },
  } = state

  return {
    dashboards,
    dashboard,
    autoRefresh,
    timeRange,
    isEditMode,
    inPresentationMode,
  }
}

const mapDispatchToProps = (dispatch) => ({
  handleClickPresentationButton: presentationButtonDispatcher(dispatch),
  dashboardActions: bindActionCreators(dashboardActionCreators, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(DashboardPage);
