import React, {PureComponent, CSSProperties} from 'react'
import classnames from 'classnames'
import getLastValues from 'src/shared/parsing/lastValues'
import _ from 'lodash'

import {SMALL_CELL_HEIGHT} from 'src/shared/graphs/helpers'
import {DYGRAPH_CONTAINER_V_MARGIN} from 'src/shared/constants'
import {generateThresholdsListHexs} from 'src/shared/constants/colorOperations'
import {ColorString} from 'src/types/colors'
import {CellType, DecimalPlaces} from 'src/types/dashboards'
import {TimeSeriesServerResponse} from 'src/types/series'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  decimalPlaces: DecimalPlaces
  cellHeight: number
  colors: ColorString[]
  prefix?: string
  suffix?: string
  lineGraph: boolean
  staticLegendHeight?: number
  data: TimeSeriesServerResponse[]
}

@ErrorHandling
class SingleStat extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    prefix: '',
    suffix: '',
  }

  public render() {
    return (
      <div className="single-stat" style={this.containerStyle}>
        {this.resizerBox}
      </div>
    )
  }

  private get renderShadow(): JSX.Element {
    const {lineGraph} = this.props

    return lineGraph && <div className="single-stat--shadow" />
  }

  private get prefixSuffixValue(): string {
    const {prefix, suffix} = this.props

    return `${prefix}${this.roundedLastValue}${suffix}`
  }

  private get lastValue(): number {
    const {data} = this.props
    const {lastValues, series} = getLastValues(data)
    const firstAlphabeticalSeriesName = _.sortBy(series)[0]

    const firstAlphabeticalIndex = _.indexOf(
      series,
      firstAlphabeticalSeriesName
    )

    return lastValues[firstAlphabeticalIndex]
  }

  private get roundedLastValue(): string {
    const {decimalPlaces} = this.props

    if (this.lastValue === null) {
      return `${0}`
    }

    let roundedValue = `${this.lastValue}`

    if (decimalPlaces.isEnforced) {
      roundedValue = this.lastValue.toFixed(decimalPlaces.digits)
    }

    return this.formatToLocale(+roundedValue)
  }

  private formatToLocale(n: number): string {
    const maximumFractionDigits = 20
    return n.toLocaleString(undefined, {maximumFractionDigits})
  }

  private get containerStyle(): CSSProperties {
    const {staticLegendHeight} = this.props

    const height = `calc(100% - ${staticLegendHeight +
      DYGRAPH_CONTAINER_V_MARGIN * 2}px)`

    const {backgroundColor} = this.coloration

    if (staticLegendHeight) {
      return {
        backgroundColor,
        height,
      }
    }

    return {
      backgroundColor,
    }
  }

  private get coloration(): CSSProperties {
    const {data, colors, lineGraph} = this.props

    const {lastValues, series} = getLastValues(data)
    const firstAlphabeticalSeriesName = _.sortBy(series)[0]

    const firstAlphabeticalIndex = _.indexOf(
      series,
      firstAlphabeticalSeriesName
    )
    const lastValue = lastValues[firstAlphabeticalIndex]

    const {bgColor, textColor} = generateThresholdsListHexs({
      colors,
      lastValue,
      cellType: lineGraph ? CellType.LinePlusSingleStat : CellType.SingleStat,
    })

    return {
      backgroundColor: bgColor,
      color: textColor,
    }
  }

  private get resizerBox(): JSX.Element {
    const {lineGraph, cellHeight} = this.props
    const {color} = this.coloration

    if (lineGraph) {
      const className = classnames('single-stat--value', {
        small: cellHeight <= SMALL_CELL_HEIGHT,
      })

      return (
        <span className={className} style={{color}}>
          {this.prefixSuffixValue}
          {this.renderShadow}
        </span>
      )
    }

    const viewBox = `0 0 ${this.prefixSuffixValue.length * 55} 100`

    return (
      <div className="single-stat--resizer">
        <svg width="100%" height="100%" viewBox={viewBox}>
          <text
            className="single-stat--text"
            fontSize="100"
            y="59%"
            x="50%"
            dominantBaseline="middle"
            textAnchor="middle"
            style={{fill: color}}
          >
            {this.prefixSuffixValue}
          </text>
        </svg>
      </div>
    )
  }
}

export default SingleStat
