import {range} from 'd3-array'

export const findHoveredRowIndices = (
  xMinCol: number[],
  xMaxCol: number[],
  yMaxCol: number[],
  dataX: number,
  dataY: number
) => {
  if (isNaN(dataX) || isNaN(dataY)) {
    return null
  }

  const hoveredRowIndices = range(0, xMinCol.length).filter(
    i => xMinCol[i] <= dataX && xMaxCol[i] > dataX
  )

  if (!hoveredRowIndices.some(i => yMaxCol[i] >= dataY)) {
    return null
  }

  return hoveredRowIndices
}
