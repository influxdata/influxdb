import {range} from 'd3-array'

import {Table, TooltipData, ColumnType} from '../types'
import {getRangeLabel} from './tooltip'
import {X_MIN, X_MAX, Y_MIN, Y_MAX, COUNT} from '../constants/columnKeys'

export const findHoveredRowIndices = (
  table: Table,
  dataX: number,
  dataY: number
): number[] => {
  const xMinData = table.getColumn(X_MIN, 'number')
  const xMaxData = table.getColumn(X_MAX, 'number')
  const yMinData = table.getColumn(Y_MIN, 'number')
  const yMaxData = table.getColumn(Y_MAX, 'number')
  const countData = table.getColumn(COUNT, 'number')

  const hoveredRowIndices = range(table.length).filter(
    i =>
      xMinData[i] <= dataX &&
      xMaxData[i] > dataX &&
      yMinData[i] <= dataY &&
      yMaxData[i] > dataY &&
      countData[i] !== 0
  )

  return hoveredRowIndices
}

export const getTooltipData = (
  hoveredRowIndices: number[] | null,
  xColKey: string,
  yColKey: string,
  xColName: string,
  yColName: string,
  getValueFormatter: (colKey: string) => (x: any) => string,
  table: Table
): TooltipData => {
  if (!hoveredRowIndices || hoveredRowIndices.length === 0) {
    return null
  }

  const xMinData = table.getColumn(X_MIN, 'number')
  const xMaxData = table.getColumn(X_MAX, 'number')
  const yMinData = table.getColumn(Y_MIN, 'number')
  const yMaxData = table.getColumn(Y_MAX, 'number')
  const countData = table.getColumn(COUNT, 'number')
  const xFormatter = getValueFormatter(xColKey)
  const yFormatter = getValueFormatter(yColKey)
  const countFormatter = getValueFormatter('count')

  const xColumn = {
    key: xColKey,
    name: xColName,
    type: 'number' as ColumnType,
    colors: null,
    values: hoveredRowIndices.map(i =>
      getRangeLabel(xMinData[i], xMaxData[i], xFormatter)
    ),
  }

  const yColumn = {
    key: yColKey,
    name: yColName,
    type: 'number' as ColumnType,
    colors: null,
    values: hoveredRowIndices.map(i =>
      getRangeLabel(yMinData[i], yMaxData[i], yFormatter)
    ),
  }

  const countColumn = {
    key: 'count',
    name: 'count',
    type: 'number' as ColumnType,
    colors: null,
    values: hoveredRowIndices.map(i => countFormatter(countData[i])),
  }

  return [xColumn, yColumn, countColumn]
}
