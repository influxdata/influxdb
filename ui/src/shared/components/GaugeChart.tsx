// Libraries
import React, {PureComponent} from 'react'
import memoizeOne from 'memoize-one'
import _ from 'lodash'

// Components
import Gauge from 'src/shared/components/Gauge'

// Parsing
import {lastValue} from 'src/shared/parsing/flux/lastValue'

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
  private lastValue = memoizeOne(lastValue)

  public render() {
    const {tables} = this.props
    const {colors, prefix, suffix, decimalPlaces} = this.props.properties

    const lastValue = this.lastValue(tables) || 0

    return (
      <div className="single-stat">
        <Gauge
          width="900"
          height="300"
          colors={colors}
          prefix={prefix}
          suffix={suffix}
          gaugePosition={lastValue}
          decimalPlaces={decimalPlaces}
        />
      </div>
    )
  }
}

export default GaugeChart
