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

export const checkResultsLength = (giraffeResult: FromFluxResult): boolean => {
  return get(giraffeResult, 'table.length', 0) > 0
}

export const getNumericColumns = (table: Table): string[] => {
  const numericColumnKeys = table.columnKeys.filter(k => {
    if (k === 'result' || k === 'table') {
      return false
    }

    const columnType = table.getColumnType(k)

    return columnType === 'time' || columnType === 'number'
  })

  return numericColumnKeys
}

export const getGroupableColumns = (table: Table): string[] => {
  const invalidGroupColumns = new Set(['_value', '_time', 'table'])
  const groupableColumns = table.columnKeys.filter(
    name => !invalidGroupColumns.has(name)
  )

  return groupableColumns
}

/*
  Previously we would automatically select an x and y column setting for an
  `XYView` based on the current Flux response.  We then added support for an
  explicit x and y column setting by adding `xColumn` and `yColumn` fields to
  the `XYView`.
  
  We did not migrate existing views when adding the fields, so the fields are
  considered optional. Thus to resolve the correct x and y column selections
  for an `XYView`, we need to:

  1. Use the `xColumn` and `yColumn` fields if they exist
  2. Fall back to automatically selecting and x and y column if not

  A `null` result from this function indicates that no valid selection could be
  made.
*/
export const defaultXColumn = (
  table: Table,
  preferredColumnKey?: string
): string | null => {
  const validColumnKeys = getNumericColumns(table)

  if (validColumnKeys.includes(preferredColumnKey)) {
    return preferredColumnKey
  }

  for (const key of ['_time', '_stop', '_start']) {
    if (validColumnKeys.includes(key)) {
      return key
    }
  }

  if (validColumnKeys.length) {
    return validColumnKeys[0]
  }

  return null
}

/*
  See `defaultXColumn`.
*/
export const defaultYColumn = (
  table: Table,
  preferredColumnKey?: string
): string | null => {
  const validColumnKeys = getNumericColumns(table)

  if (validColumnKeys.includes(preferredColumnKey)) {
    return preferredColumnKey
  }

  for (const key of validColumnKeys) {
    if (key.startsWith('_value')) {
      return key
    }
  }

  if (validColumnKeys.length) {
    return validColumnKeys[0]
  }

  return null
}
