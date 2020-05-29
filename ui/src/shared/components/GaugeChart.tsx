// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {AutoSizer} from 'react-virtualized'

// Components
import Gauge from 'src/shared/components/Gauge'

// Types
import {GaugeViewProperties} from 'src/types/dashboards'
import {Theme} from 'src/types'

// Constants
import {
  GAUGE_THEME_LIGHT,
  GAUGE_THEME_DARK,
} from 'src/shared/constants/gaugeSpecs'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  value: number
  properties: GaugeViewProperties
  theme: Theme
}

@ErrorHandling
class GaugeChart extends PureComponent<Props> {
  public render() {
    const {value, theme} = this.props
    const {
      colors,
      prefix,
      tickPrefix,
      suffix,
      tickSuffix,
      decimalPlaces,
    } = this.props.properties

    const currentTheme =
      theme === 'light' ? GAUGE_THEME_LIGHT : GAUGE_THEME_DARK

    return (
      <AutoSizer>
        {({width, height}) => (
          <div className="gauge">
            <Gauge
              width={width}
              height={height}
              colors={colors}
              prefix={prefix}
              tickPrefix={tickPrefix}
              suffix={suffix}
              tickSuffix={tickSuffix}
              gaugePosition={value}
              decimalPlaces={decimalPlaces}
              theme={currentTheme}
            />
          </div>
        )}
      </AutoSizer>
    )
  }
}

export default GaugeChart
