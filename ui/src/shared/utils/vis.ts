// Libraries
import {get} from 'lodash'
import {
  Table,
  ColumnType,
  LineInterpolation,
  FromFluxResult,
} from '@influxdata/giraffe'

// Utils
import {formatNumber} from 'src/shared/utils/formatNumber'

// Types
import {XYViewGeom, Axis, Base} from 'src/types'

/*
  A geom may be stored as "line", "step", "monotoneX", "bar", or "stacked", but
  we currently only support the "line", "step", and "monotoneX" geoms.
*/
export const resolveGeom = (geom: XYViewGeom) => {
  if (geom === XYViewGeom.Step || geom === XYViewGeom.MonotoneX) {
    return geom
  }

  return XYViewGeom.Line
}

export const geomToInterpolation = (geom: XYViewGeom): LineInterpolation => {
  const resolvedGeom = resolveGeom(geom)

  switch (resolvedGeom) {
    case XYViewGeom.Step:
      return 'step'
    case XYViewGeom.MonotoneX:
      return 'monotoneX'
    default:
      return 'linear'
  }
}

export const getFormatter = (
  columnType: ColumnType,
  prefix: string = '',
  suffix: string = '',
  base: Base = ''
): null | ((x: any) => string) => {
  return columnType === 'number'
    ? x => `${prefix}${formatNumber(x, base)}${suffix}`
    : null
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

    const keyData = table.getColumn(key)

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
  const columnKeys = new Set(table.columnKeys)

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
  return table.columnKeys.find(
    k =>
      k.startsWith('_value') &&
      (table.getColumnType(k) === 'number' || table.getColumnType(k) === 'time')
  )
}

export const checkResultsLength = (giraffeResult: FromFluxResult): boolean => {
  return get(giraffeResult, 'table.length', 0) > 0
}
