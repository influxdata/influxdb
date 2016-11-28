import React, {PropTypes} from 'react';
import Dygraph from './Dygraph';
import shallowCompare from 'react-addons-shallow-compare';

import timeSeriesToDygraph from 'utils/timeSeriesToDygraph';

const {array, string, arrayOf, number, bool, shape} = PropTypes;

export default React.createClass({
  displayName: 'LineGraph',
  propTypes: {
    data: array.isRequired, // eslint-disable-line react/forbid-prop-types
    title: string,
    isFetchingInitially: PropTypes.bool,
    isRefreshing: PropTypes.bool,
    yRange: arrayOf(number.isRequired),
    ylabels: arrayOf(string.isRequired),
    underlayCallback: PropTypes.func,
    isGraphFilled: bool,
    overrideLineColors: array,
    queries: arrayOf(shape({}).isRequired),
  },

  getDefaultProps() {
    return {
      underlayCallback: () => {},
      isGraphFilled: true,
      overrideLineColors: null,
    };
  },

  shouldComponentUpdate(nextProps, nextState) {
    return shallowCompare(this, nextProps, nextState);
  },

  componentWillMount() {
    this._timeSeries = timeSeriesToDygraph(this.props.data);
  },

  componentWillUpdate(nextProps) {
    if (this.props.data !== nextProps.data) {
      this._timeSeries = timeSeriesToDygraph(nextProps.data);
    }
  },

  render() {
    const {isFetchingInitially, title, underlayCallback, ylabels} = this.props;
    const {labels, timeSeries, dygraphSeries} = this._timeSeries;
    // If data for this graph is being fetched for the first time, show a graph-wide spinner.
    if (isFetchingInitially) {
      return (
        <div className="graph-panel__graph-fetching">
          <h3 className="graph-panel__spinner" />
        </div>
      );
    }

    const options = {
      labels,
      connectSeparatedPoints: true,
      labelsKMB: true,
      height: 300,
      axisLineColor: '#383846',
      gridLineColor: '#383846',
      title,
      rightGap: 0,
      yRangePad: 10,
      drawAxesAtZero: true,
      underlayCallback,
      ylabel: ylabels && ylabels[0],
    };

    return (
      <div>
        {this.props.isRefreshing ? <h3 className="graph-panel__spinner--small" /> : null}
        <Dygraph
          containerStyle={{width: '100%', height: '300px'}}
          overrideLineColors={this.props.overrideLineColors}
          isGraphFilled={this.props.isGraphFilled}
          timeSeries={timeSeries}
          labels={labels}
          options={options}
          dygraphSeries={dygraphSeries}
          ranges={this.getRanges()}
        />
      </div>
    );
  },

  getRanges() {
    const {queries} = this.props;
    const ranges = {};

    if (queries) {
      queries.forEach((q, i) => {
        if (i === 0 && q.range) {
          ranges.y = q.range;
        }

        if (i === 1 && q.range) {
          ranges.y2 = q.range;
        }
      });
    }

    return ranges;
  },
});
