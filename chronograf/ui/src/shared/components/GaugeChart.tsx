// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import Gauge from 'src/shared/components/Gauge'

// Parsing
import getLastValues from 'src/shared/parsing/flux/fluxToSingleStat'

// Types
import {FluxTable} from 'src/types'
import {GaugeView} from 'src/types/v2/dashboards'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  tables: FluxTable[]
  properties: GaugeView
}

@ErrorHandling
class GaugeChart extends PureComponent<Props> {
  public render() {
    const {colors, prefix, suffix, decimalPlaces} = this.props.properties

    return (
      <div className="single-stat">
        <Gauge
          width="900"
          height="300"
          colors={colors}
          prefix={prefix}
          suffix={suffix}
          gaugePosition={this.lastValueForGauge}
          decimalPlaces={decimalPlaces}
        />
      </div>
    )
  }

  private get lastValueForGauge(): number {
    const {tables} = this.props
    const {values} = getLastValues(tables)
    const lastValue = _.get(values, 0, 0)

    return lastValue
  }
}

export default GaugeChart
