import {Scale} from 'src/minard'
import {range} from 'd3-array'

export const findHoveredRowIndices = (
  xMinCol: number[],
  xMaxCol: number[],
  yMaxCol: number[],
  hoverX: number,
  hoverY: number,
  xScale: Scale,
  yScale: Scale
) => {
  if (!hoverX || !hoverY) {
    return null
  }

  const dataX = xScale.invert(hoverX)
  const dataY = yScale.invert(hoverY)

  // Find all bins whose x extent contain the mouse x position
  const hoveredRowIndices = range(0, xMinCol.length).filter(
    i => xMinCol[i] <= dataX && xMaxCol[i] > dataX
  )

  // If the mouse y position is above every one of those bars, then the mouse
  // isn't hovering over them
  if (!hoveredRowIndices.some(i => yMaxCol[i] >= dataY)) {
    return null
  }

  return hoveredRowIndices
}
