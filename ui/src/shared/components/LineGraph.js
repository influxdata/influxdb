import React, {PropTypes} from 'react';
import Dygraph from './Dygraph';
import shallowCompare from 'react-addons-shallow-compare';

import timeSeriesToDygraph from 'utils/timeSeriesToDygraph';

const {array, string, arrayOf, number, bool} = PropTypes;

export default React.createClass({
  displayName: 'LineGraph',
  propTypes: {
    data: array.isRequired, // eslint-disable-line react/forbid-prop-types
    title: string,
    isFetchingInitially: PropTypes.bool,
    isRefreshing: PropTypes.bool,
    yRange: arrayOf(number.isRequired),
    underlayCallback: PropTypes.func,
    isGraphFilled: bool,
    overrideLineColors: array,
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
    const {fields, timeSeries} = this._timeSeries;

    // If data for this graph is being fetched for the first time, show a graph-wide spinner.
    if (this.props.isFetchingInitially) {
      return (
        <div className="graph-panel__graph-fetching">
          <h3 className="graph-panel__spinner" />
        </div>
      );
    }

    const options = {
      labels: fields,
      connectSeparatedPoints: true,
      labelsKMB: true,
      axisLineColor: '#383846',
      gridLineColor: '#383846',
      title: this.props.title,
      rightGap: 0,
      yRangePad: 10,
      drawAxesAtZero: true,
      underlayCallback: this.props.underlayCallback,
    };

    return (
      <div>
        {this.props.isRefreshing ? <h3 className="graph-panel__spinner--small" /> : null}
        <Dygraph containerStyle={{width: '100%', height: '300px', backgroundColor: '#f00'}} overrideLineColors={this.props.overrideLineColors} isGraphFilled={this.props.isGraphFilled} timeSeries={timeSeries} fields={fields} options={options} />
      </div>
    );
  },
});
