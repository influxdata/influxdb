import React, {PropTypes} from 'react';
import LayoutRenderer from '../components/LayoutRenderer';
import TimeRangeDropdown from '../../shared/components/TimeRangeDropdown';
import timeRanges from 'hson!../../shared/data/timeRanges.hson';
import {fetchLayouts} from '../apis';
import _ from 'lodash';

export const HostPage = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
    params: PropTypes.shape({
      hostID: PropTypes.string.isRequired,
    }).isRequired,
  },

  getInitialState() {
    const fifteenMinutesIndex = 1;

    return {
      layouts: [],
      timeRange: timeRanges[fifteenMinutesIndex],
    };
  },

  componentDidMount() {
    fetchLayouts().then((ls) => {
      this.setState({layouts: ls.data.layouts});
    });
  },

  handleChooseTimeRange({lower}) {
    const timeRange = timeRanges.find((range) => range.queryValue === lower);
    this.setState({timeRange});
  },

  render() {
    const autoRefreshMs = 15000;
    const source = this.props.source.links.proxy;
    const hostID = this.props.params.hostID;
    const {timeRange} = this.state;

    const layout = _.head(this.state.layouts);

    let layoutComponent;
    if (layout) {
      layout.cells.forEach((cell) => {
        cell.queries.forEach((q) => {
          q.text = q.query;
          q.database = q.db;
        });
      });

      layoutComponent = (
        <LayoutRenderer
          timeRange={timeRange.queryValue}
          cells={layout.cells}
          autoRefreshMs={autoRefreshMs}
          source={source}
          host={this.props.params.hostID}
        />
      );
    } else {
      layoutComponent = <div />;
    }

    return (
      <div className="host-dashboard hosts-page">
        <div className="enterprise-header hosts-dashboard-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>{hostID}</h1>
            </div>
            <div className="enterprise-header__right">
              <p>Uptime: <strong>2d 4h 33m</strong></p>
            </div>
            <div className="enterprise-header__right">
              <TimeRangeDropdown onChooseTimeRange={this.handleChooseTimeRange} selected={timeRange.inputValue} />
            </div>
          </div>
        </div>
        <div className="container-fluid hosts-dashboard">
          <div className="row">
            {layoutComponent}
          </div>
        </div>
      </div>
    );
  },
});

export default HostPage;
