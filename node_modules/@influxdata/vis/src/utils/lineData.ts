import {Table, Scale} from '../types'
import {simplify} from '../utils/simplify'
import {FILL} from '../constants/columnKeys'

export type LineData = {
  [groupKey: string]: {
    xs: number[] | Float64Array
    ys: number[] | Float64Array
    fill: string
  }
}

export const collectLineData = (
  table: Table,
  xColKey: string,
  yColKey: string,
  fillScale: Scale<string, string>
): LineData => {
  const xCol = table.getColumn(xColKey, 'number')
  const yCol = table.getColumn(yColKey, 'number')
  const groupCol = table.getColumn(FILL, 'string')

  const result = {}

  for (let i = 0; i < table.length; i++) {
    const groupKey = groupCol[i]

    if (!result[groupKey]) {
      result[groupKey] = {
        xs: [],
        ys: [],
        fill: fillScale(groupKey),
      }
    }

    result[groupKey].xs.push(xCol[i])
    result[groupKey].ys.push(yCol[i])
  }

  return result
}

export const simplifyLineData = (
  lineData: LineData,
  xScale,
  yScale
): LineData => {
  const result = {}

  for (const [k, {xs, ys, fill}] of Object.entries(lineData)) {
    const [simplifedXs, simplifiedYs] = simplify(
      xs.map(x => xScale(x)),
      ys.map(y => yScale(y)),
      1
    )

    result[k] = {xs: simplifedXs, ys: simplifiedYs, fill}
  }

  return result
}
