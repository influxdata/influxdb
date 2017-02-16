import React, {PropTypes} from 'react';
import {Link} from 'react-router';
import {connect} from 'react-redux'
import _ from 'lodash';
import classnames from 'classnames'

import LayoutRenderer from 'shared/components/LayoutRenderer';
import DashboardHeader from 'src/dashboards/components/DashboardHeader';
import timeRanges from 'hson!../../shared/data/timeRanges.hson';

import {getDashboards} from '../apis';
import {getSource} from 'shared/apis';
import {presentationButtonDispatcher} from 'shared/dispatchers'

const {
  shape,
  string,
  bool,
  func,
} = PropTypes

const DashboardPage = React.createClass({
  propTypes: {
    params: shape({
      sourceID: string.isRequired,
      dashboardID: string.isRequired,
    }).isRequired,
    inPresentationMode: bool.isRequired,
    handleClickPresentationButton: func,
  },

  getInitialState() {
    const fifteenMinutesIndex = 1;

    return {
      dashboards: [],
      timeRange: timeRanges[fifteenMinutesIndex],
    };
  },

  componentDidMount() {
    getDashboards().then((resp) => {
      getSource(this.props.params.sourceID).then(({data: source}) => {
        this.setState({
          dashboards: resp.data.dashboards,
          source,
        });
      });
    });
  },

  currentDashboard(dashboards, dashboardID) {
    return _.find(dashboards, (d) => d.id.toString() === dashboardID);
  },

  renderDashboard(dashboard) {
    const autoRefreshMs = 15000;
    const {timeRange} = this.state;
    const {source} = this.state;

    const cellWidth = 4;
    const cellHeight = 4;

    const cells = dashboard.cells.map((cell, i) => {
      const dashboardCell = Object.assign(cell, {
        w: cellWidth,
        h: cellHeight,
        queries: cell.queries,
        i: i.toString(),
      });

      dashboardCell.queries.forEach((q) => {
        q.text = q.query;
        q.database = source.telegraf;
      });
      return dashboardCell;
    });

    return (
      <LayoutRenderer
        timeRange={timeRange}
        cells={cells}
        autoRefreshMs={autoRefreshMs}
        source={source.links.proxy}
      />
    );
  },

  handleChooseTimeRange({lower}) {
    const timeRange = timeRanges.find((range) => range.queryValue === lower);
    this.setState({timeRange});
  },

  render() {
    const {dashboards, timeRange} = this.state;
    const {params: {dashboardID, sourceID}, inPresentationMode, handleClickPresentationButton} = this.props
    const dashboard = this.currentDashboard(dashboards, dashboardID);

    return (
      <div className="page">
        <DashboardHeader
          buttonText={dashboard ? dashboard.name : ''}
          timeRange={timeRange}
          handleChooseTimeRange={this.handleChooseTimeRange}
          isHidden={inPresentationMode}
          handleClickPresentationButton={handleClickPresentationButton}
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
        </DashboardHeader>
        <div className={classnames({
          'page-contents': true,
          'presentation-mode': inPresentationMode,
        })}>
            <div className="container-fluid full-width">
            { dashboard ? this.renderDashboard(dashboard) : '' }
          </div>
        </div>
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
