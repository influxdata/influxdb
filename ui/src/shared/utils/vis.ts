// Libraries
import {format} from 'd3-format'
import {isNumeric, Table} from '@influxdata/vis'

// Types
import {XYViewGeom, Axis} from 'src/types'

const MAX_DECIMALS = 3

const formatSmallNumber = format(`.${MAX_DECIMALS}~f`) // e.g. "0.032"

const formatLargeNumber = format(`.${MAX_DECIMALS}~s`) // e.g. "2.452M"

export const formatNumber = (t: number): string => {
  if (t >= -1 && t <= 1) {
    return formatSmallNumber(t)
  }

  return formatLargeNumber(t)
}

/*
  A geom may be stored as "line", "step", "bar", or "stacked", but we currently
  only support the "line" and "step" geoms.
*/
export const resolveGeom = (geom: XYViewGeom) => {
  if (geom === XYViewGeom.Step) {
    return geom
  }

  return XYViewGeom.Line
}

const NOISY_LEGEND_COLUMNS = new Set(['_start', '_stop', 'result'])

/*
  Some columns (e.g. `_start` and `_stop`) appear frequently in Flux group
  keys, but rarely affect the actual grouping of data since every value in the
  response for these columns is equal. When this is the case, we hide these
  columns in the hover legend.
*/
export const filterNoisyColumns = (columns: string[], table: Table): string[] =>
  columns.filter(key => {
    if (!NOISY_LEGEND_COLUMNS.has(key)) {
      return true
    }

    const keyData = table.columns[key].data

    return !keyData.every(d => d === keyData[0])
  })

export const parseBounds = (
  bounds: Axis['bounds']
): [number, number] | null => {
  if (
    !bounds ||
    !bounds[0] ||
    !bounds[1] ||
    isNaN(+bounds[0]) ||
    isNaN(+bounds[1])
  ) {
    return null
  }

  return [+bounds[0], +bounds[1]]
}

export const extent = (xs: number[]): [number, number] | null => {
  let low = Infinity
  let high = -Infinity

  for (const x of xs) {
    if (x < low) {
      low = x
    }

    if (x > high) {
      high = x
    }
  }

  if (low === Infinity || high === -Infinity) {
    return null
  }

  return [low, high]
}

export const chooseXColumn = (table: Table): string | null => {
  const columnKeys = new Set(Object.keys(table.columns))

  if (columnKeys.has('_time')) {
    return '_time'
  }

  if (columnKeys.has('_stop')) {
    return '_stop'
  }

  if (columnKeys.has('_start')) {
    return '_start'
  }

  return null
}

export const chooseYColumn = (table: Table): string | null => {
  return Object.keys(table.columns).find(
    k => k.startsWith('_value') && isNumeric(table.columns[k].type)
  )
}
