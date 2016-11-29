/* eslint-disable no-magic-numbers */
import React, {PropTypes} from 'react';
import Dygraph from '../../external/dygraph';
import 'style/_Graph.css';

const {arrayOf, object, array, number, bool} = PropTypes;

const LINE_COLORS = [
  '#00C9FF',
  '#9394FF',
  '#4ED8A0',
  '#ff0054',
  '#ffcc00',
  '#33aa99',
  '#9dfc5d',
  '#92bcc3',
  '#ca96fb',
  '#ff00f0',
  '#38b94a',
  '#3844b9',
  '#a0725b',
];

export default React.createClass({
  displayName: 'Dygraph',

  propTypes: {
    yRange: arrayOf(number.isRequired),
    timeSeries: array.isRequired, // eslint-disable-line react/forbid-prop-types
    fields: array.isRequired, // eslint-disable-line react/forbid-prop-types
    options: object, // eslint-disable-line react/forbid-prop-types
    containerStyle: object, // eslint-disable-line react/forbid-prop-types
    isGraphFilled: bool,
    overrideLineColors: array,
  },

  getDefaultProps() {
    return {
      containerStyle: {},
      isGraphFilled: true,
      overrideLineColors: null,
    };
  },

  getTimeSeries() {
    // Avoid 'Can't plot empty data set' errors by falling back to a
    // default dataset that's valid for Dygraph.
    return this.props.timeSeries.length ? this.props.timeSeries : [[0]];
  },

  componentDidMount() {
    const timeSeries = this.getTimeSeries();
    const {yRange} = this.props;

    const refs = this.refs;
    const graphContainerNode = refs.graphContainer;
    const legendContainerNode = refs.legendContainer;
    const markerNode = refs.graphVerticalMarker;
    let finalLineColors = this.props.overrideLineColors;

    if (finalLineColors === null) {
      finalLineColors = LINE_COLORS;
    }

    const defaultOptions = {
      labelsSeparateLines: false,
      labelsDiv: legendContainerNode,
      labelsKMB: true,
      rightGap: 0,
      leftGap: 0,
      highlightSeriesBackgroundAlpha: 1,
      fillGraph: this.props.isGraphFilled,
      axisLineWidth: 2,
      gridLineWidth: 1,
      strokeWidth: 1.5,
      highlightCircleSize: 3,
      colors: finalLineColors,
      valueRange: getRange(timeSeries, yRange),
      highlightSeriesOpts: {
        strokeWidth: 2,
        highlightCircleSize: 5,
      },
      highlightCallback(e, x, points) {
        // Move the Legend on hover
        const graphRect = graphContainerNode.getBoundingClientRect();
        const legendRect = legendContainerNode.getBoundingClientRect();
        const graphWidth = graphRect.width + 32; // Factoring in padding from parent
        const legendWidth = legendRect.width;
        const legendMaxLeft = graphWidth - (legendWidth / 2);
        const trueGraphX = (e.pageX - graphRect.left);
        let legendLeft = trueGraphX;
        // Enforcing max & min legend offsets
        if (trueGraphX < (legendWidth / 2)) {
          legendLeft = legendWidth / 2;
        } else if (trueGraphX > legendMaxLeft) {
          legendLeft = legendMaxLeft;
        }

        legendContainerNode.style.left = `${legendLeft}px`;
        setMarker(points);
      },
      unhighlightCallback() {
        removeMarker();
      },
    };

    const options = Object.assign({}, defaultOptions, this.props.options);

    this.dygraph = new Dygraph(graphContainerNode, timeSeries, options);

    function setMarker(points) {
      markerNode.style.left = `${points[0].canvasx}px`;
      markerNode.style.display = 'block';
    }

    function removeMarker() {
      markerNode.style.display = 'none';
    }
  },

  componentWillUnmount() {
    this.dygraph.destroy();
    delete this.dygraph;
  },

  componentDidUpdate() {
    const dygraph = this.dygraph;
    if (!dygraph) {
      throw new Error('Dygraph not configured in time; this should not be possible!');
    }

    const timeSeries = this.getTimeSeries();
    const {fields, yRange} = this.props;

    dygraph.updateOptions({
      labels: fields,
      file: timeSeries,
      valueRange: getRange(timeSeries, yRange),
      underlayCallback: this.props.options.underlayCallback,
    });

    dygraph.resize();
  },

  render() {
    return (
      <div ref="self">
        <div ref="graphContainer" style={this.props.containerStyle} />
        <div className="container--dygraph-legend" ref="legendContainer" />
        <div className="graph-vertical-marker" ref="graphVerticalMarker" />
      </div>
    );
  },
});

function getRange(timeSeries, override) {
  if (override) {
    return override;
  }

  let max = null;
  let min = null;

  timeSeries.forEach((series) => {
    for (let i = 1; i < series.length; i++) {
      const val = series[i];

      if (max === null) {
        max = val;
      }

      if (min === null) {
        min = val;
      }

      if (val) {
        min = Math.min(min, val);
        max = Math.max(max, val);
      }
    }
  });

  // Dygraph will not reliably plot X / Y axis labels if min and max are both 0
  if (min === 0 && max === 0) {
    return [null, null];
  }

  return [min, max];
}
