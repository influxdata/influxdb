// Libraries
import React, {Component} from 'react'
import {Plot, Config} from '@influxdata/vis'

// Types
import {UsageTable} from 'src/types'

const DEFAULT_CONFIG = {
  axisColor: '#545667',
  gridColor: '#545667',
  gridOpacity: 0.5,
  legendFont: '12px Rubik',
  legendFontColor: '#8e91a1',
  legendFontBrightColor: '#c6cad3',
  legendBackgroundColor: '#1c1c21',
  legendBorder: '1px solid #202028',
  legendCrosshairColor: '#434453',
  table: null,
  showAxes: true,
  yTickFormatter: (x: string | number) => x,
  layers: [],
}

const DEFAULT_LAYER = {
  type: 'line' as const,
  x: '_time',
  y: '_value',
}

interface Props {
  isGrouped: boolean
  groupColumns?: string[]
  column: string
  table: UsageTable
  yFormatter: (x: string | number) => string | number
}

class SparkLineContents extends Component<Props> {
  render() {
    return (
      <div className="usage--plot">
        <Plot config={this.getConfig()} />
      </div>
    )
  }

  getConfig(): Config {
    const {isGrouped, groupColumns, column, table} = this.props
    if (isGrouped && groupColumns) {
      const legendColumns = ['_time', column, ...groupColumns]

      return {
        ...DEFAULT_CONFIG,
        legendColumns,
        layers: [
          {
            ...DEFAULT_LAYER,
            y: column,
            fill: groupColumns,
          },
        ],
        table,
      }
    }

    return {
      ...DEFAULT_CONFIG,
      layers: [
        {
          ...DEFAULT_LAYER,
          y: column,
        },
      ],
      table,
    }
  }
}

export default SparkLineContents
