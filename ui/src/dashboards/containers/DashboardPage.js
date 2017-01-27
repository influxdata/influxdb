import React, {PropTypes} from 'react';
import ReactTooltip from 'react-tooltip';

import LayoutRenderer from 'shared/components/LayoutRenderer';
import TimeRangeDropdown from '../../shared/components/TimeRangeDropdown';
import timeRanges from 'hson!../../shared/data/timeRanges.hson';

import {getDashboard} from '../apis';
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
      timeRange: timeRanges[fifteenMinutesIndex],
    };
  },

  componentDidMount() {
    getDashboard(this.props.params.dashboardID).then((resp) => {
      getSource(this.props.params.sourceID).then(({data: source}) => {
        this.setState({
          dashboard: resp.data,
          source,
        });
      });
    });
  },

  renderDashboard(dashboard) {
    const autoRefreshMs = 15000;
    const {timeRange} = this.state;
    const {source} = this.state;

    const cellWidth = 4;
    const cellHeight = 4;
    const pageWidth = 12;

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
        host={this.props.params.sourceID}
      />
    );
  },

  handleChooseTimeRange({lower}) {
    const timeRange = timeRanges.find((range) => range.queryValue === lower);
    this.setState({timeRange});
  },

  render() {
    const {dashboard, timeRange} = this.state;
    const dashboardName = dashboard ? dashboard.name : '';

    return (
      <div className="page">
        <div className="page-header full-width">
          <div className="page-header__container">
            <div className="page-header__left">
              <div className="dropdown page-header-dropdown">
                <button className="dropdown-toggle" type="button" data-toggle="dropdown">
                  <span className="button-text">{dashboardName}</span>
                </button>
              </div>
            </div>
            <div className="page-header__right">
              <div className="btn btn-info btn-sm" data-for="graph-tips-tooltip" data-tip="<p><code>Click + Drag</code> Zoom in (X or Y)</p><p><code>Shift + Click</code> Pan Graph Window</p><p><code>Double Click</code> Reset Graph Window</p>">
                <span className="icon heart"></span>
                Graph Tips
              </div>
              <ReactTooltip id="graph-tips-tooltip" effect="solid" html={true} offset={{top: 2}} place="bottom" class="influx-tooltip place-bottom" />
              <TimeRangeDropdown onChooseTimeRange={this.handleChooseTimeRange} selected={timeRange.inputValue} />
            </div>
          </div>
        </div>
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
