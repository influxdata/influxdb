import React, {PropTypes} from 'react';
import {Link} from 'react-router';
import _ from 'lodash';

import LayoutRenderer from 'shared/components/LayoutRenderer';
import DashboardHeader from 'src/dashboards/components/DashboardHeader';
import timeRanges from 'hson!../../shared/data/timeRanges.hson';

import {getDashboards} from '../apis';
import {getSource} from 'shared/apis';

const DashboardPage = React.createClass({
  propTypes: {
    params: PropTypes.shape({
      sourceID: PropTypes.string.isRequired,
      dashboardID: PropTypes.string.isRequired,
    }).isRequired,
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
    const dashboard = this.currentDashboard(dashboards, this.props.params.dashboardID);

    return (
      <div className="page">
        <DashboardHeader buttonText={dashboard ? dashboard.name : ''} timeRange={timeRange} handleChooseTimeRange={this.handleChooseTimeRange}>
          {(dashboards).map((d, i) => {
            return (
              <li key={i}>
                <Link to={`/sources/${this.props.params.sourceID}/dashboards/${d.id}`} className="role-option">
                  {d.name}
                </Link>
              </li>
            );
          })}
        </DashboardHeader>
        <div className="page-contents">
          <div className="container-fluid full-width">
            { dashboard ? this.renderDashboard(dashboard) : '' }
          </div>
        </div>
      </div>
    );
  },
});

export default DashboardPage;
