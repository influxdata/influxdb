import React, {SFC, CSSProperties} from 'react'

import {HistogramDatum, TooltipAnchor} from 'src/types/histogram'

interface Props {
  datum: HistogramDatum
  x: number
  y: number
  anchor?: TooltipAnchor
}

const HistogramChartTooltip: SFC<Props> = props => {
  const {datum, x, y, anchor = 'left'} = props

  if (!datum) {
    return null
  }

  const style: CSSProperties = {
    position: 'fixed',
    top: y,
  }

  if (anchor === 'left') {
    style.left = x
  } else {
    style.right = x
  }

  return (
    <div className="histogram-chart-tooltip" style={style}>
      <div className="histogram-chart-tooltip--value">{datum.value}</div>
      <div className="histogram-chart-tooltip--group">{datum.group}</div>
    </div>
  )
}

export default HistogramChartTooltip
