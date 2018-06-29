import React, {PureComponent, CSSProperties} from 'react'
import getLastValues from 'src/shared/parsing/lastValues'
import _ from 'lodash'

import {DYGRAPH_CONTAINER_V_MARGIN} from 'src/shared/constants'
import {generateThresholdsListHexs} from 'src/shared/constants/colorOperations'
import {ColorNumber} from 'src/types/colors'
import {CellType} from 'src/types/dashboards'
import {Data} from 'src/types/dygraphs'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  isFetchingInitially: boolean
  // cellHeight: number
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
      <div className="single-stat" style={this.containerStyle}>
        {this.resizerBox}
        <span className="single-stat--value">{this.renderShadow}</span>
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

    return {
      backgroundColor: bgColor,
      color: textColor,
    }
  }

  private get resizerBox(): JSX.Element {
    const {color} = this.coloration

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
