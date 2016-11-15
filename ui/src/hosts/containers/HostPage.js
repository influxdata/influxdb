import React, {PropTypes} from 'react';
import LayoutRenderer from 'shared/components/LayoutRenderer';
import TimeRangeDropdown from '../../shared/components/TimeRangeDropdown';
import timeRanges from 'hson!../../shared/data/timeRanges.hson';
import {getMappings, getAppsForHosts, getMeasurementsForHost} from 'src/hosts/apis';
import {fetchLayouts} from 'shared/apis';
import _ from 'lodash';

export const HostPage = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
      telegraf: PropTypes.string.isRequired,
    }),
    params: PropTypes.shape({
      hostID: PropTypes.string.isRequired,
    }).isRequired,
    location: PropTypes.shape({
      query: PropTypes.shape({
        app: PropTypes.string,
      }),
    }),
  },

  getInitialState() {
    const fifteenMinutesIndex = 1;

    return {
      layouts: [],
      timeRange: timeRanges[fifteenMinutesIndex],
    };
  },

  componentDidMount() {
    const {source, params} = this.props;
    const hosts = {[params.hostID]: {name: params.hostID}};

    // fetching layouts and mappings can be done at the same time
    fetchLayouts().then(({data: {layouts}}) => {
      getMappings().then(({data: {mappings}}) => {
        getAppsForHosts(source.links.proxy, hosts, mappings, source.telegraf).then((newHosts) => {
          getMeasurementsForHost(source, params.hostID).then((measurements) => {
            const host = newHosts[this.props.params.hostID];
            const filteredLayouts = layouts.filter((layout) => {
              const focusedApp = this.props.location.query.app;
              if (focusedApp) {
                return layout.app === focusedApp;
              }

              return host.apps && host.apps.includes(layout.app) && measurements.includes(layout.measurement);
            });
            this.setState({layouts: filteredLayouts});
          });
        });
      });
    });
  },

  handleChooseTimeRange({lower}) {
    const timeRange = timeRanges.find((range) => range.queryValue === lower);
    this.setState({timeRange});
  },

  renderLayouts(layouts) {
    const autoRefreshMs = 15000;
    const {timeRange} = this.state;
    const {source} = this.props;

    const autoflowLayouts = _.remove(layouts, (layout) => {
      return layout.autoflow === true;
    });
    let autoflowCells = [];

    const cellWidth = 4;
    const cellHeight = 4;
    const pageWidth = 12;

    autoflowLayouts.forEach((layout, i) => {
      layout.cells.forEach((cell, j) => {
        cell.w = cellWidth;
        cell.h = cellHeight;
        cell.x = ((i + j) * cellWidth % pageWidth);
        cell.y = Math.floor(((i + j) * cellWidth / pageWidth)) * cellHeight;
        autoflowCells = autoflowCells.concat(cell);
      });
    });

    const autoflowLayout = {
      cells: autoflowCells,
      autoflow: false,
    };

    const staticLayouts = _.remove(layouts, (layout) => {
      return layout.autoflow === false;
    });
    staticLayouts.unshift(autoflowLayout);

    let layoutCells = [];
    let translateY = 0;
    staticLayouts.forEach((layout) => {
      let maxY = 0;
      layout.cells.forEach((cell) => {
        cell.y += translateY;
        if (cell.y > translateY) {
          maxY = cell.y;
        }
        cell.queries.forEach((q) => {
          q.text = q.query;
          q.database = source.telegraf;
        });
      });
      translateY = maxY;

      layoutCells = layoutCells.concat(layout.cells);
    });


    return (
      <LayoutRenderer
        timeRange={timeRange}
        cells={layoutCells}
        autoRefreshMs={autoRefreshMs}
        source={source.links.proxy}
        host={this.props.params.hostID}
      />
    );
  },

  render() {
    const hostID = this.props.params.hostID;
    const {layouts, timeRange} = this.state;

    return (
      <div className="host-dashboard hosts-page">
        <div className="chronograf-header hosts-dashboard-header">
          <div className="chronograf-header__container">
            <div className="chronograf-header__left">
              <h1>{hostID}</h1>
            </div>
            <div className="chronograf-header__right">
              <h1>Range:</h1>
              <TimeRangeDropdown onChooseTimeRange={this.handleChooseTimeRange} selected={timeRange.inputValue} />
            </div>
          </div>
        </div>
        <div className="hosts-page-scroll-container">
          <div className="container-fluid hosts-dashboard">
            <div className="row">
              { (layouts.length > 0) ? this.renderLayouts(layouts) : '' }
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default HostPage;
