import React, {PropTypes} from 'react';
import {Link} from 'react-router';
import {connect} from 'react-redux'
import _ from 'lodash';

import Header from 'src/dashboards/components/DashboardHeader';
import EditHeader from 'src/dashboards/components/DashboardHeaderEdit';
import Dashboard from 'src/dashboards/components/Dashboard';
import timeRanges from 'hson!../../shared/data/timeRanges.hson';

import {getDashboards} from '../apis';
import {presentationButtonDispatcher} from 'shared/dispatchers'

const {
  shape,
  string,
  bool,
  func,
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
    inPresentationMode: bool.isRequired,
    handleClickPresentationButton: func,
  },

  getInitialState() {
    const fifteenMinutesIndex = 1;

    return {
      dashboards: [],
      dashboard: null,
      timeRange: timeRanges[fifteenMinutesIndex],
      isEditMode: this.props.location.pathname.includes('/edit'),
    };
  },

  componentDidMount() {
    const {dashboardID} = this.props.params;

    getDashboards().then(({data: {dashboards}}) => {
      this.setState({
        dashboards,
        dashboard: _.find(dashboards, (d) => d.id.toString() === dashboardID),
      });
    });
  },

  componentWillReceiveProps(nextProps) {
    const {location: {pathname}} = this.props
    const {location: {pathname: nextPathname}, params: {dashboardID: nextID}} = nextProps

    if (nextPathname.pathname === pathname) {
      return
    }

    this.setState({
      isEditMode: nextPathname.includes('/edit'),
      dashboard: _.find(this.state.dashboards, (d) => d.id.toString() === nextID),
    })
  },

  currentDashboard(dashboards, dashboardID) {
    return _.find(dashboards, (d) => d.id.toString() === dashboardID);
  },

  handleChooseTimeRange({lower}) {
    const timeRange = timeRanges.find((range) => range.queryValue === lower);
    this.setState({timeRange});
  },

  render() {
    const {dashboards, timeRange, isEditMode, dashboard} = this.state;

    const {
      params: {sourceID},
      inPresentationMode,
      handleClickPresentationButton,
      source,
    } = this.props

    if (!dashboard) {
      return null
    }

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

const mapStateToProps = (state) => ({
  inPresentationMode: state.appUI.presentationMode,
})

const mapDispatchToProps = (dispatch) => ({
  handleClickPresentationButton: presentationButtonDispatcher(dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(DashboardPage);
