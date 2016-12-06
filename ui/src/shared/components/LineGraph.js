import React, {PropTypes} from 'react';
import Dygraph from './Dygraph';
import classNames from 'classnames';
import shallowCompare from 'react-addons-shallow-compare';
import _ from 'lodash';

import timeSeriesToDygraph from 'utils/timeSeriesToDygraph';
import lastValues from 'src/shared/parsing/lastValues';

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
    showSingleStat: bool,
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
    const {data, isFetchingInitially, isRefreshing, isGraphFilled, overrideLineColors, title, underlayCallback, queries, showSingleStat} = this.props;
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
      axisLabelWidth: 38,
      drawAxesAtZero: true,
      underlayCallback,
      ylabel: _.get(queries, ['0', 'label'], ''),
      y2label: _.get(queries, ['1', 'label'], ''),
    };

    let roundedValue;
    if (showSingleStat) {
      const lastValue = lastValues(data)[1];

      const precision = 100.0;
      roundedValue = Math.round(+lastValue * precision) / precision;
    }

    return (
      <div className={classNames({"graph--hasYLabel": !!(options.ylabel || options.y2label)})}>
        {isRefreshing ? this.renderSpinner() : null}
        <Dygraph
          containerStyle={{width: '100%', height: '300px'}}
          overrideLineColors={overrideLineColors}
          isGraphFilled={isGraphFilled}
          timeSeries={timeSeries}
          labels={labels}
          options={options}
          dygraphSeries={dygraphSeries}
          ranges={this.getRanges()}
        />
        {showSingleStat ? <div className="graph-single-stat single-stat">{roundedValue}</div> : null}
      </div>
    );
  },

  renderSpinner() {
    return (
      <div className="graph-panel__refreshing">
        <div></div>
        <div></div>
        <div></div>
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
