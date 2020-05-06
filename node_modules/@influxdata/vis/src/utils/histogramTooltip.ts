import {range} from 'd3-array'

import {Table, TooltipData, Scale, ColumnType} from '../types'
import {getRangeLabel} from './tooltip'
import {getTooltipGroupColumns} from './tooltip'
import {X_MIN, X_MAX, Y_MAX, COUNT, FILL} from '../constants/columnKeys'

export const findHoveredRowIndices = (
  table: Table,
  hoverX: number,
  hoverY: number,
  xScale: Scale,
  yScale: Scale
) => {
  if (!hoverX || !hoverY) {
    return null
  }

  const xMinData = table.getColumn(X_MIN, 'number')
  const xMaxData = table.getColumn(X_MAX, 'number')
  const yMaxData = table.getColumn(Y_MAX, 'number')
  const dataX = xScale.invert(hoverX)
  const dataY = yScale.invert(hoverY)

  // Find all bins whose x extent contain the mouse x position
  const hoveredRowIndices = range(0, xMinData.length).filter(
    i => xMinData[i] <= dataX && xMaxData[i] > dataX
  )

  // If the mouse y position is above every one of those bars, then the mouse
  // isn't hovering over them
  if (!hoveredRowIndices.some(i => yMaxData[i] >= dataY)) {
    return null
  }

  return hoveredRowIndices
}

export const getTooltipData = (
  hoveredRowIndices: number[],
  table: Table,
  xColKey: string,
  fillColKeys: string[],
  getValueFormatter: (colKey: string) => (x: any) => string,
  fillScale: Scale<string, string>
): TooltipData => {
  if (!hoveredRowIndices || hoveredRowIndices.length === 0) {
    return null
  }

  const xMinData = table.getColumn(X_MIN, 'number')
  const xMaxData = table.getColumn(X_MAX, 'number')
  const countData = table.getColumn(COUNT, 'number')
  const groupCol = table.getColumn(FILL, 'string')
  const colors = hoveredRowIndices.map(i => fillScale(groupCol[i]))
  const xFormatter = getValueFormatter(xColKey)
  const countFormatter = getValueFormatter('yMin')

  const xColumn = {
    key: xColKey,
    name: xColKey,
    type: 'number' as ColumnType,
    colors,
    values: hoveredRowIndices.map(i =>
      getRangeLabel(xMinData[i], xMaxData[i], xFormatter)
    ),
  }

  const countColumn = {
    key: 'count',
    name: 'count',
    type: 'number' as ColumnType,
    colors,
    values: hoveredRowIndices.map(i => countFormatter(countData[i])),
  }

  const fillColumns = getTooltipGroupColumns(
    table,
    hoveredRowIndices,
    fillColKeys,
    getValueFormatter,
    colors
  )

  return [xColumn, countColumn, ...fillColumns]
}
