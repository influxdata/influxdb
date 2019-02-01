import React, {useRef, SFC} from 'react'

import {Scale, HistogramTooltipProps} from 'src/minard'
import {useLayoutStyle} from 'src/minard/utils/useLayoutStyle'

const MARGIN_X = 15
const MARGIN_Y = 10

interface Props {
  hoverX: number
  hoverY: number
  x: string
  fill: string
  tooltip?: (props: HistogramTooltipProps) => JSX.Element
  width: number
  height: number
  xMinCol: number[]
  xMaxCol: number[]
  yMinCol: number[]
  yMaxCol: number[]
  fillCol: string[] | boolean[]
  fillScale: Scale<string | number | boolean, string>
  hoveredRowIndices: number[] | null
}

const HistogramTooltip: SFC<Props> = ({
  hoverX,
  hoverY,
  x,
  fill,
  tooltip,
  width,
  height,
  xMinCol,
  xMaxCol,
  yMinCol,
  yMaxCol,
  fillCol,
  fillScale,
  hoveredRowIndices,
}: Props) => {
  const tooltipEl = useRef<HTMLDivElement>(null)

  useLayoutStyle(tooltipEl, ({offsetWidth, offsetHeight}) => {
    let dx = MARGIN_X
    let dy = MARGIN_Y

    if (hoverX + MARGIN_X + offsetWidth > width) {
      // If the tooltip overflows off the right edge of the visualization,
      // position it on the left side of the mouse instead
      dx = 0 - MARGIN_X - offsetWidth
    }

    if (hoverY + MARGIN_Y + offsetHeight > height) {
      // If the tooltip overflows off the bottom edge of the visualization,
      // position it on the top side of the mouse instead
      dy = 0 - MARGIN_Y - offsetHeight
    }

    return {
      position: 'absolute',
      left: `${hoverX + dx}px`,
      top: `${hoverY + dy}px`,
    }
  })

  if (!hoveredRowIndices) {
    return null
  }

  const tooltipProps: HistogramTooltipProps = {
    x,
    fill,
    xMin: xMinCol[hoveredRowIndices[0]],
    xMax: xMaxCol[hoveredRowIndices[0]],
    counts: hoveredRowIndices.map(i => ({
      fill: fillCol ? fillCol[i] : null,
      count: yMaxCol[i] - yMinCol[i],
      color: fillScale(fillCol[i]),
    })),
  }

  return (
    <div className="minard-histogram-tooltip" ref={tooltipEl}>
      {/* TODO: Provide a default tooltip implementation */}
      {tooltip ? tooltip(tooltipProps) : null}
    </div>
  )
}

export default HistogramTooltip
