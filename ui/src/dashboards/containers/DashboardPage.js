import React, {PropTypes} from 'react';
import {Link} from 'react-router';
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux';

import Header from 'src/dashboards/components/DashboardHeader';
import EditHeader from 'src/dashboards/components/DashboardHeaderEdit';
import Dashboard from 'src/dashboards/components/Dashboard';
import timeRanges from 'hson!../../shared/data/timeRanges.hson';

import * as dashboardActionCreators from 'src/dashboards/actions';

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
      getDashboards: func.isRequired,
      setDashboard: func.isRequired,
      setTimeRange: func.isRequired,
    }).isRequired,
    dashboards: arrayOf(shape({
      id: number.isRequired,
      cells: arrayOf(shape({})).isRequired,
    })).isRequired,
    dashboard: shape({
      id: number.isRequired,
      cells: arrayOf(shape({})).isRequired,
    }).isRequired,
    timeRange: shape({}).isRequired,
    inPresentationMode: bool.isRequired,
    handleClickPresentationButton: func,
  },

  getInitialState() {
    return {
      isEditMode: this.props.location.pathname.includes('/edit'),
    };
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

    this.setState({
      isEditMode: nextPathname.includes('/edit'),
    })
  },

  handleChooseTimeRange({lower}) {
    const timeRange = timeRanges.find((range) => range.queryValue === lower);
    this.props.dashboardActions.setTimeRange(timeRange)
  },

  render() {
    const {isEditMode} = this.state;

    const {
      dashboards,
      dashboard,
      params: {sourceID},
      inPresentationMode,
      handleClickPresentationButton,
      source,
      timeRange,
    } = this.props

    return (
      <div className="page">
        {
          isEditMode ?
            <EditHeader dashboard={dashboard} onSave={() => {}} /> :
            <Header
              buttonText={dashboard ? dashboard.name : ''}
              timeRange={timeRange}
              handleChooseTimeRange={this.handleChooseTimeRange}
              isHidden={inPresentationMode}
              handleClickPresentationButton={handleClickPresentationButton}
              dashboard={dashboard}
              sourceID={sourceID}
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
          timeRange={timeRange}
        />
      </div>
    );
  },
});

const mapStateToProps = (state) => {
  const {
    appUI,
    dashboardUI: {dashboards, dashboard, timeRange},
  } = state

  return {
    inPresentationMode: appUI.presentationMode,
    dashboards,
    dashboard,
    timeRange,
  }
}

const mapDispatchToProps = (dispatch) => ({
  handleClickPresentationButton: presentationButtonDispatcher(dispatch),
  dashboardActions: bindActionCreators(dashboardActionCreators, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(DashboardPage);
