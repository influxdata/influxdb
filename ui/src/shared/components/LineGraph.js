import React, {PropTypes} from 'react';
import Dygraph from './Dygraph';
import shallowCompare from 'react-addons-shallow-compare';
import _ from 'lodash';

import timeSeriesToDygraph from 'utils/timeSeriesToDygraph';

const {array, string, arrayOf, bool, shape} = PropTypes;

export default React.createClass({
  displayName: 'LineGraph',
  propTypes: {
    data: arrayOf(shape({}).isRequired).isRequired,
    title: string,
    isFetchingInitially: PropTypes.bool,
    isRefreshing: PropTypes.bool,
    underlayCallback: PropTypes.func,
    isGraphFilled: bool,
    overrideLineColors: array,
    queries: arrayOf(shape({}).isRequired).isRequired,
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
    const {isFetchingInitially, title, underlayCallback, queries} = this.props;
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
      ylabel: _.get(queries, ['0', 'label'], ''),
      y2label: _.get(queries, ['1', 'label'], ''),
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
    if (!queries) {
      return {};
    }

    const ranges = {};
    const q0 = queries[0];
    const q1 = queries[1];

    if (q0 && q0.range) {
      ranges.y = [q0.range.lower, q0.range.upper];
    }

    if (q1 && q1.range) {
      ranges.y2 = [q1.range.lower, q1.range.upper];
    }

    return ranges;
  },
});
