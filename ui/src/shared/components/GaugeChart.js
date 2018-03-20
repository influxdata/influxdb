import React, {PureComponent} from 'react'
import PropTypes from 'prop-types'
import lastValues from 'shared/parsing/lastValues'
import Gauge from 'shared/components/Gauge'

import {DEFAULT_GAUGE_COLORS} from 'src/shared/constants/thresholds'
import {stringifyColorValues} from 'src/shared/constants/colorOperations'
import {DASHBOARD_LAYOUT_ROW_HEIGHT} from 'src/shared/constants'

class GaugeChart extends PureComponent {
  render() {
    const {
      data,
      cellID,
      cellHeight,
      isFetchingInitially,
      colors,
      resizeCoords,
      resizerTopHeight,
      prefix,
      suffix,
    } = this.props

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

    // When a new height is passed the Gauge component resizes internally
    // Passing in a new often ensures the gauge appears sharp

    const thisGaugeIsResizing = resizeCoords ? cellID === resizeCoords.i : false

    const initialCellHeight =
      cellHeight && (cellHeight * DASHBOARD_LAYOUT_ROW_HEIGHT).toString()

    const resizeCoordsHeight =
      resizeCoords &&
      thisGaugeIsResizing &&
      (resizeCoords.h * DASHBOARD_LAYOUT_ROW_HEIGHT).toString()

    const height = (resizeCoordsHeight ||
      initialCellHeight ||
      resizerTopHeight ||
      300)
      .toString()

    return (
      <div className="single-stat">
        <Gauge
          width="900"
          height={height}
          colors={colors}
          gaugePosition={roundedValue}
          prefix={prefix}
          suffix={suffix}
        />
      </div>
    )
  }
}

const {arrayOf, bool, number, shape, string} = PropTypes

GaugeChart.defaultProps = {
  colors: stringifyColorValues(DEFAULT_GAUGE_COLORS),
}

GaugeChart.propTypes = {
  data: arrayOf(shape()).isRequired,
  isFetchingInitially: bool,
  cellID: string,
  cellHeight: number,
  resizerTopHeight: number,
  resizeCoords: shape(),
  colors: arrayOf(
    shape({
      type: string.isRequired,
      hex: string.isRequired,
      id: string.isRequired,
      name: string.isRequired,
      value: string.isRequired,
    }).isRequired
  ),
  prefix: string.isRequired,
  suffix: string.isRequired,
}

export default GaugeChart
