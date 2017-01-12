import React, {PropTypes} from 'react';
import LayoutRenderer from 'shared/components/LayoutRenderer';
import TimeRangeDropdown from '../../shared/components/TimeRangeDropdown';
import ReactTooltip from 'react-tooltip';
import timeRanges from 'hson!../../shared/data/timeRanges.hson';
import {getMappings, getAppsForHosts, getMeasurementsForHost, getHosts} from 'src/hosts/apis';
import {fetchLayouts} from 'shared/apis';

export const HostPage = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
      telegraf: PropTypes.string.isRequired,
      id: PropTypes.string.isRequired,
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
      hosts: [],
      timeRange: timeRanges[fifteenMinutesIndex],
    };
  },

  componentDidMount() {
    const {source, params, location} = this.props;
    const hostsToGet = {[params.hostID]: {name: params.hostID}};

    // fetching layouts and mappings can be done at the same time
    fetchLayouts().then(({data: {layouts}}) => {
      getMappings().then(({data: {mappings}}) => {
        getAppsForHosts(source.links.proxy, hostsToGet, mappings, source.telegraf).then((newHosts) => {
          getMeasurementsForHost(source, params.hostID).then((measurements) => {
            const host = newHosts[this.props.params.hostID];
            const filteredLayouts = layouts.filter((layout) => {
              const focusedApp = location.query.app;
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

    getHosts(source.links.proxy, source.telegraf).then((hosts) => {
      this.setState({hosts});
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

    const autoflowLayouts = layouts.filter((layout) => !!layout.autoflow);

    const cellWidth = 4;
    const cellHeight = 4;
    const pageWidth = 12;

    const autoflowCells = autoflowLayouts.reduce((allCells, layout, i) => {
      return allCells.concat(layout.cells.map((cell, j) => {
        return Object.assign(cell, {
          w: cellWidth,
          h: cellHeight,
          x: ((i + j) * cellWidth % pageWidth),
          y: Math.floor(((i + j) * cellWidth / pageWidth)) * cellHeight,
        });
      }));
    }, []);

    const staticLayouts = layouts.filter((layout) => !layout.autoflow);
    staticLayouts.unshift({cells: autoflowCells});

    let translateY = 0;
    const layoutCells = staticLayouts.reduce((allCells, layout) => {
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

      return allCells.concat(layout.cells);
    }, []);

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
    const {layouts, timeRange, hosts} = this.state;

    return (
      <div className="page">
        <div className="page-header full-width">
          <div className="page-header__container">
            <div className="page-header__left">
              <div className="dropdown minimal-dropdown">
                <button className="dropdown-toggle" type="button" data-toggle="dropdown">
                  <span className="button-text btn-info">{hostID}</span>
                  <span className="caret"></span>
                </button>
                <ul className="dropdown-menu" aria-labelledby="dropdownMenu1">
                  {hosts.map((host, i) => {
                    return (
                      <li key={i}>
                        <a href={`/sources/${this.props.source.id}/hosts/${host}`} className="role-option">
                          {host}
                        </a>
                      </li>
                    );
                  })}
                </ul>
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
            { (layouts.length > 0) ? this.renderLayouts(layouts) : '' }
          </div>
        </div>
      </div>
    );
  },
});

export default HostPage;
