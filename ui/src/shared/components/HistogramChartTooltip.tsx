import React, {SFC, CSSProperties} from 'react'

import {HoverData, ColorScale} from 'src/types/histogram'

interface Props {
  data: HoverData
  colorScale: ColorScale
}

const HistogramChartTooltip: SFC<Props> = props => {
  const {colorScale} = props
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
        {data.map(d => (
          <div key={d.key} style={{color: colorScale(d.group)}}>
            {d.value}
          </div>
        ))}
      </div>
      <div className="histogram-chart-tooltip--column">
        {data.map(d => (
          <div key={d.key} style={{color: colorScale(d.group)}}>
            {d.group}
          </div>
        ))}
      </div>
    </div>
  )
}

export default HistogramChartTooltip
