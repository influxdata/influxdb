import React, {SFC} from 'react'
import _ from 'lodash'

import {Margins} from 'src/types/histogram'

const NUM_TICKS = 5

interface Props {
  width: number
  height: number
  margins: Margins
}

const HistogramChartSkeleton: SFC<Props> = props => {
  const {margins, width, height} = props

  const spacing = (height - margins.top - margins.bottom) / NUM_TICKS
  const y1 = height - margins.bottom
  const tickYs = _.range(0, NUM_TICKS).map(i => y1 - i * spacing)

  return (
    <svg className="histogram-chart-skeleton" width={width} height={height}>
      {tickYs.map((y, i) => (
        <line
          key={i}
          className="y-tick"
          x1={margins.left}
          x2={width - margins.right}
          y1={y}
          y2={y}
        />
      ))}
    </svg>
  )
}

export default HistogramChartSkeleton
