// Libraries
import React, {PureComponent, CSSProperties} from 'react'
import _ from 'lodash'

// Constants
import {generateThresholdsListHexs} from 'src/shared/constants/colorOperations'

// Types
import {ViewType} from 'src/types/v2/dashboards'
import {SingleStatView} from 'src/types/v2/dashboards'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  properties: SingleStatView
  stat: number
}

@ErrorHandling
class SingleStat extends PureComponent<Props> {
  public render() {
    return (
      <div className="single-stat" style={this.containerStyle}>
        {this.resizerBox}
      </div>
    )
  }

  private get prefixSuffixValue(): string {
    const {prefix, suffix} = this.props.properties

    return `${prefix}${this.roundedLastValue}${suffix}`
  }

  private get lastValue(): number {
    return this.props.stat
  }

  private get roundedLastValue(): string {
    const {decimalPlaces} = this.props.properties

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
    const {backgroundColor} = this.coloration

    return {
      backgroundColor,
    }
  }

  private get coloration(): CSSProperties {
    const {colors} = this.props.properties

    const {bgColor, textColor} = generateThresholdsListHexs({
      colors,
      lastValue: this.props.stat,
      cellType: ViewType.SingleStat,
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
