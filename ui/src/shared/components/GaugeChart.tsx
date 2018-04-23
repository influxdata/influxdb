import React, {PureComponent} from 'react'
import getLastValues, {TimeSeriesResponse} from 'src/shared/parsing/lastValues'
import Gauge from 'src/shared/components/Gauge'
import _ from 'lodash'

import {DEFAULT_GAUGE_COLORS} from 'src/shared/constants/thresholds'
import {stringifyColorValues} from 'src/shared/constants/colorOperations'
import {DASHBOARD_LAYOUT_ROW_HEIGHT} from 'src/shared/constants'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Color {
  type: string
  hex: string
  id: string
  name: string
  value: string
}

interface Props {
  data: TimeSeriesResponse[]
  isFetchingInitially: boolean
  cellID: string
  cellHeight?: number
  colors?: Color[]
  prefix: string
  suffix: string
  resizerTopHeight?: number
  resizeCoords?: {
    i: string
    h: number
  }
}

@ErrorHandling
class GaugeChart extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    colors: stringifyColorValues(DEFAULT_GAUGE_COLORS),
  }

  public render() {
    const {isFetchingInitially, colors, prefix, suffix} = this.props

    if (isFetchingInitially) {
      return (
        <div className="graph-empty">
          <h3 className="graph-spinner" />
        </div>
      )
    }

    return (
      <div className="single-stat">
        <Gauge
          width="900"
          height={this.height}
          colors={colors}
          gaugePosition={this.lastValueForGauge}
          prefix={prefix}
          suffix={suffix}
        />
      </div>
    )
  }

  private get height(): string {
    const {resizerTopHeight} = this.props

    return (
      this.resizeCoordsHeight ||
      this.initialCellHeight ||
      resizerTopHeight ||
      300
    ).toString()
  }

  private get resizeCoordsHeight(): string {
    const {resizeCoords} = this.props

    if (resizeCoords && this.isResizing) {
      return (resizeCoords.h * DASHBOARD_LAYOUT_ROW_HEIGHT).toString()
    }

    return null
  }

  private get initialCellHeight(): string {
    const {cellHeight} = this.props

    if (cellHeight) {
      return (cellHeight * DASHBOARD_LAYOUT_ROW_HEIGHT).toString()
    }

    return null
  }

  private get isResizing(): boolean {
    const {resizeCoords, cellID} = this.props
    return resizeCoords ? cellID === resizeCoords.i : false
  }

  private get lastValueForGauge(): number {
    const {data} = this.props
    const {lastValues} = getLastValues(data)
    const precision = 100.0

    return Math.round(_.get(lastValues, 0, 0) * precision) / precision
  }
}

export default GaugeChart
