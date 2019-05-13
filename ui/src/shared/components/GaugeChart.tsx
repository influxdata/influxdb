// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import Gauge from 'src/shared/components/Gauge'

// Types
import {GaugeView} from 'src/types/dashboards'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  value: number
  properties: GaugeView
}

@ErrorHandling
class GaugeChart extends PureComponent<Props> {
  public render() {
    const {value} = this.props
    const {colors, prefix, suffix, decimalPlaces} = this.props.properties

    return (
      <div className="single-stat">
        <Gauge
          width="900"
          height="300"
          colors={colors}
          prefix={prefix}
          suffix={suffix}
          gaugePosition={value}
          decimalPlaces={decimalPlaces}
        />
      </div>
    )
  }
}

export default GaugeChart
