import {getTooltipGroupColumns} from './tooltip'
import {TooltipData, Table, Scale} from '../types'

export const getTooltipData = (
  hoveredRowIndices: number[],
  table: Table,
  xColKey: string,
  yColKey: string,
  groupColKey: string,
  getValueFormatter: (colKey: string) => (x: any) => string,
  fillColKeys: string[],
  fillScale: Scale<string, string>
): TooltipData => {
  const xColData = table.getColumn(xColKey, 'number')
  const yColData = table.getColumn(yColKey, 'number')
  const groupColData = table.getColumn(groupColKey, 'string')
  const colors = hoveredRowIndices.map(i => fillScale(groupColData[i]))
  const xFormatter = getValueFormatter(xColKey)
  const yFormatter = getValueFormatter(yColKey)

  const tooltipXCol = {
    key: xColKey,
    name: table.getColumnName(xColKey),
    type: table.getColumnType(xColKey),
    colors,
    values: hoveredRowIndices.map(i => xFormatter(xColData[i])),
  }

  const tooltipYCol = {
    key: yColKey,
    name: table.getColumnName(yColKey),
    type: table.getColumnType(yColKey),
    colors,
    values: hoveredRowIndices.map(i => yFormatter(yColData[i])),
  }

  const fillColumns = getTooltipGroupColumns(
    table,
    hoveredRowIndices,
    fillColKeys,
    getValueFormatter,
    colors
  )

  return [tooltipXCol, tooltipYCol, ...fillColumns]
}
