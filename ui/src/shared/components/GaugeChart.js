import React, {PropTypes, Component} from 'react'
import shallowCompare from 'react-addons-shallow-compare'
import lastValues from 'shared/parsing/lastValues'
import Gauge from 'shared/components/Gauge'

import {DEFAULT_COLORS} from 'src/dashboards/constants/gaugeColors'

class GaugeChart extends Component {
  shouldComponentUpdate(nextProps, nextState) {
    return shallowCompare(this, nextProps, nextState)
  }

  render() {
    const {data, cellHeight, isFetchingInitially, colors} = this.props

    // If data for this graph is being fetched for the first time, show a graph-wide spinner.
    if (isFetchingInitially) {
      return (
        <div className="graph-empty">
          <h3 className="graph-spinner" />
        </div>
      )
    }

    const lastValue = lastValues(data)[1]

    const precision = 100.0
    const roundedValue = Math.round(+lastValue * precision) / precision

    const trueHeight = (cellHeight * 83).toString()

    return (
      <div className="single-stat">
        <Gauge
          width="400"
          height={trueHeight || 200}
          colors={colors}
          gaugePosition={roundedValue}
        />
      </div>
    )
  }
}

const {arrayOf, bool, number, shape, string} = PropTypes

GaugeChart.defaultProps = {
  colors: DEFAULT_COLORS,
}

GaugeChart.propTypes = {
  data: arrayOf(shape()).isRequired,
  isFetchingInitially: bool,
  cellHeight: number,
  colors: arrayOf(
    shape({
      type: string.isRequired,
      hex: string.isRequired,
      id: string.isRequired,
      name: string.isRequired,
      value: string.isRequired,
    }).isRequired
  ),
}

export default GaugeChart
