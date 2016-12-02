import React, {PropTypes} from 'react';
import LayoutRenderer from 'shared/components/LayoutRenderer';
import TimeRangeDropdown from '../../shared/components/TimeRangeDropdown';
import timeRanges from 'hson!../../shared/data/timeRanges.hson';

export const KubernetesPage = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
      telegraf: PropTypes.string.isRequired,
    }),
    layouts: PropTypes.arrayOf(PropTypes.shape().isRequired).isRequired,
  },

  getInitialState() {
    const fifteenMinutesIndex = 1;
    return {
      timeRange: timeRanges[fifteenMinutesIndex],
    };
  },

  renderLayouts(layouts) {
    const autoRefreshMs = 15000;
    const {timeRange} = this.state;
    const {source} = this.props;

    let layoutCells = [];
    layouts.forEach((layout) => {
      layoutCells = layoutCells.concat(layout.cells);
    });

    layoutCells.forEach((cell, i) => {
      cell.queries.forEach((q) => {
        q.text = q.query;
        q.database = source.telegraf;
      });
      cell.x = (i * 4 % 12); // eslint-disable-line no-magic-numbers
      cell.y = 0;
    });

    return (
      <LayoutRenderer
        timeRange={timeRange}
        cells={layoutCells}
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
    const {layouts} = this.props;
    const {timeRange} = this.state;
    const emptyState = (
      <div className="generic-empty-state">
        <span className="icon alert-triangle"></span>
        <h4>No Kubernetes configuration found</h4>
      </div>
    );

    return (
      <div className="page">
        <div className="page-header full-width">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1>Kubernetes Dashboard</h1>
            </div>
            <div className="page-header__right">
              <TimeRangeDropdown onChooseTimeRange={this.handleChooseTimeRange} selected={timeRange.inputValue} />
            </div>
          </div>
        </div>
        <div className="page-contents">
          <div className="container-fluid full-width">
            {layouts.length ? this.renderLayouts(layouts) : emptyState}
          </div>
        </div>
      </div>
    );
  },
});
export default KubernetesPage;
