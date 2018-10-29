import React, {SFC, CSSProperties} from 'react'
import _ from 'lodash'

import {HoverData, ColorScale, HistogramColor} from 'src/types/histogram'

interface Props {
  data: HoverData
  colorScale: ColorScale
  colors: HistogramColor[]
}

const HistogramChartTooltip: SFC<Props> = props => {
  const {colorScale, colors} = props
  const {data, x, y, anchor = 'left'} = props.data

  const tooltipStyle: CSSProperties = {
    position: 'fixed',
    top: y,
  }

  if (anchor === 'left') {
    tooltipStyle.left = x
  } else {
    tooltipStyle.right = x
  }

  return (
    <div className="histogram-chart-tooltip" style={tooltipStyle}>
      <div className="histogram-chart-tooltip--column">
        {data.map(d => {
          const groupColor = colors.find(c => c.group === d.group)
          return (
            <div
              key={d.key}
              style={{
                color: colorScale(_.get(groupColor, 'color', ''), d.group),
              }}
            >
              {d.value}
            </div>
          )
        })}
      </div>
      <div className="histogram-chart-tooltip--column">
        {data.map(d => {
          const groupColor = colors.find(c => c.group === d.group)
          return (
            <div
              key={d.key}
              style={{
                color: colorScale(_.get(groupColor, 'color', ''), d.group),
              }}
            >
              {d.group}
            </div>
          )
        })}
      </div>
    </div>
  )
}

export default HistogramChartTooltip
