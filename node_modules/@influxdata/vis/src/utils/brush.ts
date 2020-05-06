import {Scale} from '../types'
import {DragEvent} from '../utils/useDragEvent'

export const getRectDimensions = (
  event: DragEvent,
  plotWidth: number,
  plotHeight: number
) => {
  if (event.dragMode === 'brushX') {
    const x = Math.max(0, Math.min(event.initialX, event.x))

    const width = Math.min(Math.max(event.initialX, event.x) - x, plotWidth - x)

    return {
      x,
      y: 0,
      width,
      height: plotHeight,
    }
  }

  if (event.dragMode === 'brushY') {
    const y = Math.max(0, Math.min(event.initialY, event.y))

    const height = Math.min(
      Math.max(event.initialY, event.y) - y,
      plotHeight - y
    )

    return {
      x: 0,
      y,
      width: plotWidth,
      height: height,
    }
  }

  return null
}

export const rangeToDomain = (
  [p0, p1]: number[],
  scale: Scale<number, number>,
  length: number
): number[] => [
  scale.invert(Math.max(Math.min(p0, p1), 0)),
  scale.invert(Math.min(Math.max(p0, p1), length)),
]
