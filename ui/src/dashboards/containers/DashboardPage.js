import React, {PropTypes} from 'react';
import {Link} from 'react-router';
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux';
import _ from 'lodash';

import Header from 'src/dashboards/components/DashboardHeader';
import EditHeader from 'src/dashboards/components/DashboardHeaderEdit';
import Dashboard from 'src/dashboards/components/Dashboard';
import timeRanges from 'hson!../../shared/data/timeRanges.hson';

import * as dashboardActionCreators from 'src/dashboards/actions';

import {getDashboards} from '../apis';
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
      loadDashboards: func.isRequired,
    }).isRequired,
    dashboards: arrayOf(shape({
      id: number.isRequired,
      cells: arrayOf(shape({})).isRequired,
    })).isRequired,
    inPresentationMode: bool.isRequired,
    handleClickPresentationButton: func,
  },

  getInitialState() {
    const fifteenMinutesIndex = 1;

    return {
      dashboard: null,
      timeRange: timeRanges[fifteenMinutesIndex],
      isEditMode: this.props.location.pathname.includes('/edit'),
    };
  },

  componentDidMount() {
    const {params: {dashboardID}, dashboardActions: {loadDashboards}} = this.props;

    getDashboards().then(({data: {dashboards}}) => {
      loadDashboards(dashboards)
      this.setState({
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

  handleChooseTimeRange({lower}) {
    const timeRange = timeRanges.find((range) => range.queryValue === lower);
    this.setState({timeRange});
  },

  render() {
    const {timeRange, isEditMode, dashboard} = this.state;

    const {
      dashboards,
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
  dashboards: state.dashboardUI.dashboards,
})

const mapDispatchToProps = (dispatch) => ({
  handleClickPresentationButton: presentationButtonDispatcher(dispatch),
  dashboardActions: bindActionCreators(dashboardActionCreators, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(DashboardPage);
