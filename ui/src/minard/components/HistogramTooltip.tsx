import React, {useRef, SFC} from 'react'

import {HistogramTooltipProps, HistogramLayer} from 'src/minard'
import {useLayoutStyle} from 'src/minard/utils/useLayoutStyle'
import {useMousePos} from 'src/minard/utils/useMousePos'
import {getHistogramTooltipProps} from 'src/minard/utils/getHistogramTooltipProps'

const MARGIN_X = 15
const MARGIN_Y = 10

interface Props {
  hoverX: number
  hoverY: number
  tooltip?: (props: HistogramTooltipProps) => JSX.Element
  layer: HistogramLayer
  hoveredRowIndices: number[] | null
}

const HistogramTooltip: SFC<Props> = ({
  hoverX,
  hoverY,
  tooltip,
  layer,
  hoveredRowIndices,
}: Props) => {
  const tooltipEl = useRef<HTMLDivElement>(null)
  const {x: absoluteHoverX, y: absoluteHoverY} = useMousePos(document.body)

  // Position the tooltip next to the mouse cursor, like this:
  //
  //                   ┌─────────────┐
  //                   │             │
  //          (mouse)  │   tooltip   │
  //                   │             │
  //                   └─────────────┘
  //
  // The positioning is subject to the following restrictions:
  //
  // - If the tooltip overflows the right side of the screen, position it on
  //   the left side of the cursor instead
  //
  // - If the tooltip overflows the top or bottom of the screen (with a bit of
  //   margin), shift it just enough so that it is fully back inside the screen
  //
  useLayoutStyle(tooltipEl, ({offsetWidth, offsetHeight}) => {
    let dx = MARGIN_X
    let dy = 0 - offsetHeight / 2

    if (absoluteHoverX + MARGIN_X + offsetWidth > window.innerWidth) {
      dx = 0 - MARGIN_X - offsetWidth
    }

    if (absoluteHoverY + offsetHeight / 2 + MARGIN_Y > window.innerHeight) {
      dy = 0 - (absoluteHoverY + offsetHeight - window.innerHeight) - MARGIN_Y
    }

    if (absoluteHoverY - offsetHeight / 2 - MARGIN_Y < 0) {
      dy = 0 - absoluteHoverY + MARGIN_Y
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

  const tooltipProps = getHistogramTooltipProps(layer, hoveredRowIndices)

  return (
    <div className="minard-histogram-tooltip" ref={tooltipEl}>
      {/* TODO: Provide a default tooltip implementation */}
      {tooltip ? tooltip(tooltipProps) : null}
    </div>
  )
}

export default HistogramTooltip
