import React, {PureComponent} from 'react'
import classnames from 'classnames'
import getLastValues from 'src/shared/parsing/lastValues'
import _ from 'lodash'

import {SMALL_CELL_HEIGHT} from 'src/shared/graphs/helpers'
import {DYGRAPH_CONTAINER_V_MARGIN} from 'src/shared/constants'
import {generateThresholdsListHexs} from 'src/shared/constants/colorOperations'
import {ColorNumber} from 'src/types/colors'
import {CellType} from 'src/types/dashboards'
import {Data} from 'src/types/dygraphs'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  isFetchingInitially: boolean
  cellHeight: number
  colors: ColorNumber[]
  prefix?: string
  suffix?: string
  lineGraph: boolean
  staticLegendHeight: number
  data: Data
}

@ErrorHandling
class SingleStat extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    prefix: '',
    suffix: '',
  }

  public render() {
    const {isFetchingInitially} = this.props

    if (isFetchingInitially) {
      return (
        <div className="graph-empty">
          <h3 className="graph-spinner" />
        </div>
      )
    }

    return (
      <div className="single-stat" style={this.styles}>
        <span className={this.className}>
          {this.completeValue}
          {this.renderShadow}
        </span>
      </div>
    )
  }

  private get renderShadow(): JSX.Element {
    const {lineGraph} = this.props

    return lineGraph && <div className="single-stat--shadow" />
  }

  private get completeValue(): string {
    const {prefix, suffix} = this.props

    return `${prefix}${this.roundedLastValue}${suffix}`
  }

  private get roundedLastValue(): string {
    const {data} = this.props
    const {lastValues, series} = getLastValues(data)
    const firstAlphabeticalSeriesName = _.sortBy(series)[0]

    const firstAlphabeticalindex = _.indexOf(
      series,
      firstAlphabeticalSeriesName
    )
    const lastValue = lastValues[firstAlphabeticalindex]
    const HUNDRED = 100.0
    const roundedValue = Math.round(+lastValue * HUNDRED) / HUNDRED

    return `${roundedValue}`
  }

  private get styles() {
    const {data, colors, lineGraph, staticLegendHeight} = this.props

    const {lastValues, series} = getLastValues(data)
    const firstAlphabeticalSeriesName = _.sortBy(series)[0]

    const firstAlphabeticalindex = _.indexOf(
      series,
      firstAlphabeticalSeriesName
    )
    const lastValue = lastValues[firstAlphabeticalindex]

    const {bgColor, textColor} = generateThresholdsListHexs({
      colors,
      lastValue,
      cellType: lineGraph ? CellType.LinePlusSingleStat : CellType.SingleStat,
    })

    const backgroundColor = bgColor
    const color = textColor

    const height = `calc(100% - ${staticLegendHeight +
      DYGRAPH_CONTAINER_V_MARGIN * 2}px)`

    return staticLegendHeight
      ? {
          backgroundColor,
          color,
          height,
        }
      : {
          backgroundColor,
          color,
        }
  }

  private get className(): string {
    const {cellHeight} = this.props

    return classnames('single-stat--value', {
      'single-stat--small': cellHeight === SMALL_CELL_HEIGHT,
    })
  }
}

export default SingleStat
