// Libraries
import * as React from 'react'
import {useLayoutEffect, FunctionComponent, CSSProperties} from 'react'

import {DragEvent} from '../utils/useDragEvent'
import {getRectDimensions} from '../utils/brush'

const MIN_SELECTION_SIZE = 5 // pixels

interface Props {
  event: DragEvent | null
  width: number
  height: number
  onXBrushEnd: (xRange: number[]) => void
  onYBrushEnd: (yRange: number[]) => void
}

export const Brush: FunctionComponent<Props> = ({
  event,
  width,
  height,
  onXBrushEnd,
  onYBrushEnd,
}) => {
  useLayoutEffect(() => {
    if (!event || !event.dragMode || event.type !== 'dragend') {
      return
    }

    let p0
    let p1
    let callback

    if (event.dragMode === 'brushX') {
      p0 = Math.min(event.initialX, event.x)
      p1 = Math.max(event.initialX, event.x)
      callback = onXBrushEnd
    } else if (event.dragMode === 'brushY') {
      p0 = Math.min(event.initialY, event.y)
      p1 = Math.max(event.initialY, event.y)
      callback = onYBrushEnd
    } else {
      return
    }

    if (p1 - p0 < MIN_SELECTION_SIZE) {
      return
    }

    callback([p0, p1])
  })

  if (!event || !event.dragMode || event.type === 'dragend') {
    return null
  }

  const {x, y, width: brushWidth, height: brushHeight} = getRectDimensions(
    event,
    width,
    height
  )

  const selectionStyle: CSSProperties = {
    display: event.initialX === null ? 'none' : 'inherit',
    position: 'absolute',
    left: `${x}px`,
    width: `${brushWidth}px`,
    top: `${y}px`,
    height: `${brushHeight}px`,
    opacity: 0.1,
    backgroundColor: 'aliceblue',
  }

  return <div className="vis-brush-selection" style={selectionStyle} />
}
