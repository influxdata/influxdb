import React, {PropTypes, Component} from 'react'
import shallowCompare from 'react-addons-shallow-compare'
import lastValues from 'shared/parsing/lastValues'

import Gauge from 'shared/components/Gauge'

class GaugeChart extends Component {
  shouldComponentUpdate(nextProps, nextState) {
    return shallowCompare(this, nextProps, nextState)
  }

  render() {
    const {data, cellHeight, isFetchingInitially} = this.props

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
          minValue={0}
          maxValue={900}
          lowerThreshold={600}
          upperThreshold={800}
          gaugePosition={roundedValue}
          theme="summer"
          inverse={true}
        />
      </div>
    )
  }
}

const {arrayOf, bool, number, shape} = PropTypes

GaugeChart.propTypes = {
  data: arrayOf(shape()).isRequired,
  isFetchingInitially: bool,
  cellHeight: number,
}

export default GaugeChart
