import React, {PropTypes} from 'react'
import Dygraph from './Dygraph'
import shallowCompare from 'react-addons-shallow-compare'

import timeSeriesToDygraph from 'utils/timeSeriesToDygraph'

export default React.createClass({
  displayName: 'MiniGraph',
  propTypes: {
    data: PropTypes.array.isRequired, // eslint-disable-line react/forbid-prop-types
    title: PropTypes.string,
    queryDescription: PropTypes.string,
    yRange: PropTypes.arrayOf(PropTypes.number.isRequired),
    options: PropTypes.shape({
      combineSeries: PropTypes.bool,
    }),
  },

  getDefaultProps() {
    return {
      options: {},
    }
  },

  shouldComponentUpdate(nextProps, nextState) {
    return shallowCompare(this, nextProps, nextState)
  },

  render() {
    const results = timeSeriesToDygraph(this.props.data)
    const {fields, timeSeries} = this.props.options.combineSeries
      ? this.combineSeries(results)
      : results

    if (!timeSeries.length) {
      return null
    }

    const options = {
      labels: fields,
      showLabelsOnHighlight: false,
      fillGraph: false,
      connectSeparatedPoints: true,
      axisLineColor: '#23232C',
      gridLineColor: '#2E2E38',
      gridLineWidth: 1,
      strokeWidth: 1.5,
      highlightCircleSize: 0,
      highlightSeriesOpts: {
        strokeWidth: 0,
        highlightCircleSize: 0,
      },
      highlightCallback() {},
      legend: 'never',
      axes: {
        x: {
          drawGrid: false,
          drawAxis: false,
        },
        y: {
          drawGrid: false,
          drawAxis: false,
        },
      },
      title: this.props.title,
      rightGap: 0,
      yRangePad: 10,
      interactionModel: {},
    }

    const truncPrecision = 100000
    const latestValue = timeSeries[timeSeries.length - 1][1]
    const truncated = Math.round(latestValue * truncPrecision) / truncPrecision

    const statText = (
      <div className="cluster-stat--label">
        <span>
          {this.props.queryDescription}
        </span>
        <span>
          <strong>
            {truncated}
          </strong>
        </span>
      </div>
    )

    return (
      <div className="cluster-stat">
        <Dygraph
          containerStyle={{width: '100%', height: '30px'}}
          timeSeries={timeSeries}
          fields={fields}
          options={options}
          yRange={this.props.yRange}
        />
        {statText}
      </div>
    )
  },

  /**
   * If we have a series with multiple points, sometimes we want to sum all
   * values into a single value (e.g. on the overview page, where we might
   * calculate active writes per node, but also across the entire cluster.
   *
   * [<timestamp>, 5, 10] => [<timestamp>, 15]
   */
  combineSeries(results) {
    const fields = results.fields.slice(0, 2) // Hack, but good enough for now for the sparklines (which have no labels).
    const timeSeries = results.timeSeries
      .filter(point => {
        // Filter out any points that don't report results for *all* of the series
        // we're trying to combine..
        //
        // e.g. [<timestamp>, null, null, 5] would be removed.
        //
        // We use `combineSeries` when we want to combine the values for multiple series
        // into a single series.  It makes sense to only report points where all
        // series are represented, so we can accurately take the sum.
        return point.slice(1).every(v => v !== null)
      })
      .map(point => {
        const timestamp = point[0]
        const total = point.slice(1).reduce((sum, n) => {
          return n ? sum + n : sum
        }, 0)
        return [timestamp, total]
      })
    return {fields, timeSeries}
  },
})
