import React, {PureComponent} from 'react'
import uuid from 'uuid'
import {ScaleLinear, ScaleTime} from 'd3-scale'

import {Margins} from 'src/types/histogram'

const Y_TICK_COUNT = 5
const Y_TICK_PADDING_RIGHT = 5
const X_TICK_COUNT = 10
const X_TICK_PADDING_TOP = 8

interface Props {
  width: number
  height: number
  margins: Margins
  xScale: ScaleTime<number, number>
  yScale: ScaleLinear<number, number>
}

class HistogramChartAxes extends PureComponent<Props> {
  public render() {
    const {xTickData, yTickData} = this

    return (
      <>
        {this.renderYTicks(yTickData)}
        {this.renderYLabels(yTickData)}
        {this.renderXLabels(xTickData)}
      </>
    )
  }

  private renderYTicks(yTickData) {
    return yTickData.map(({x1, x2, y}) => (
      <line className="y-tick" key={uuid.v4()} x1={x1} x2={x2} y1={y} y2={y} />
    ))
  }

  private renderYLabels(yTickData) {
    return yTickData.map(({x1, y, label}) => (
      <text
        className="y-label"
        key={uuid.v4()}
        x={x1 - Y_TICK_PADDING_RIGHT}
        y={y}
      >
        {label}
      </text>
    ))
  }

  private renderXLabels(xTickData) {
    return xTickData.map(({x, y, label}) => (
      <text className="x-label" key={uuid.v4()} y={y} x={x}>
        {label}
      </text>
    ))
  }

  private get xTickData() {
    const {margins, xScale, width, height} = this.props

    const y = height - margins.bottom + X_TICK_PADDING_TOP
    const formatTime = xScale.tickFormat()

    return xScale
      .ticks(X_TICK_COUNT)
      .filter(val => {
        const x = xScale(val)

        // Don't render labels that will be cut off
        return x > margins.left && x < width - margins.right
      })
      .map(val => {
        const x = xScale(val)

        return {
          x,
          y,
          label: formatTime(val),
        }
      })
  }

  private get yTickData() {
    const {width, margins, yScale} = this.props

    return yScale.ticks(Y_TICK_COUNT).map(val => {
      return {
        label: val,
        x1: margins.left,
        x2: margins.left + width,
        y: margins.top + yScale(val),
      }
    })
  }
}

export default HistogramChartAxes
