import {extent, range, thresholdSturges} from 'd3-array'

import {
  Table,
  HistogramMappings,
  HistogramPosition,
  HistogramScales,
  NumericColumnData,
} from '../types'

import {getGroupKey} from '../utils/getGroupKey'
import {getFillScale} from '../utils/getFillScale'
import {appendGroupingCol} from '../utils/appendGroupingCol'
import {X_MIN, X_MAX, Y_MIN, Y_MAX, COUNT, FILL} from '../constants/columnKeys'
import {newTable} from '../utils/newTable'

export const getHistogramTable = (
  table: Table,
  xColKey: string,
  xDomain: number[],
  groupColKeys: string[] = [],
  binCount: number,
  position: HistogramPosition
): Table =>
  appendGroupingCol(
    bin(table, xColKey, xDomain, groupColKeys, binCount, position),
    groupColKeys,
    FILL
  )

export const getHistogramMappings = (fill: string[]): HistogramMappings => ({
  xMin: 'xMin',
  xMax: 'xMax',
  yMin: 'yMin',
  yMax: 'yMax',
  fill,
})

export const getHistogramScales = (
  table: Table,
  colors: string[]
): HistogramScales => ({
  fill: getFillScale(table, colors),
})

/*
  Compute the data of a histogram visualization.

  The column specified by the `xColName` will be divided into `binCount` evenly
  spaced bins, and the number of rows in each bin will be counted.

  If the `groupKeyCols` option is passed, rows in each bin are further grouped
  by the set of values for the `groupKeyCols` for the row.

  The returned result is a table where each row represents a bar in a
  (potentially stacked) histogram. For example, a histogram with two bins and
  two groups in each bin (four bars total) might have a table that looks like
  this:

      xMin | xMax | yMin | yMax | host | cpu
      --------------------------------------
         0 |   10 |    0 |   21 |  "a" |   1
         0 |   10 |   21 |   30 |  "b" |   1
        10 |   20 |    0 |    4 |  "a" |   1
        10 |   20 |    4 |    6 |  "b" |   1

  If `binCount` is not provided, a default value will be provided using
  [Sturges' formula][0].

  [0]: https://en.wikipedia.org/wiki/Histogram#Sturges'_formula
*/
export const bin = (
  table: Table,
  xColKey: string,
  xDomain: number[],
  groupColKeys: string[] = [],
  binCount: number,
  position: HistogramPosition
): Table => {
  const xColData = table.getColumn(xColKey, 'number')
  const xColType = table.getColumnType(xColKey) as 'number'

  if (!binCount) {
    binCount = thresholdSturges(xColData)
  }

  xDomain = resolveXDomain(xColData, xDomain)

  const bins = createBins(xDomain, binCount)

  // A group is the set of key-value pairs that a row takes on for the column
  // names specified in `groupColKeys`. The group key is a hashable
  // representation of the values of these pairs. This object ends up looking
  // like:
  //
  //     {
  //       'cpu0,free': {cpu: 'cpu0', _field: 'free'},
  //       'cpu1,free': {cpu: 'cpu1', _field: 'free'}
  //     }
  //
  const groupsByGroupKey = {}

  // Count x values by bin and group
  for (let i = 0; i < xColData.length; i++) {
    const x = xColData[i]

    const shouldSkipPoint =
      x === undefined ||
      x === null ||
      isNaN(x) ||
      x < xDomain[0] ||
      x > xDomain[1]

    if (shouldSkipPoint) {
      continue
    }

    const group = getGroup(table, groupColKeys, i)
    const groupKey = getGroupKey(Object.values(group))
    const xPercentage = (x - xDomain[0]) / (xDomain[1] - xDomain[0])

    let binIndex = Math.floor(xPercentage * binCount)

    if (binIndex === bins.length) {
      // Special case: the maximum value should be clamped to the last bin
      binIndex = bins.length - 1
    }

    const bin = bins[binIndex]

    groupsByGroupKey[groupKey] = group

    if (!bin.values[groupKey]) {
      bin.values[groupKey] = 1
    } else {
      bin.values[groupKey] += 1
    }
  }

  // Next, build up a tabular representation of each of these bins by group
  const groupKeys = Object.keys(groupsByGroupKey)

  const xMinData: number[] = []
  const xMaxData: number[] = []
  const yMinData: number[] = []
  const yMaxData: number[] = []
  const countData: number[] = []
  const groupKeyData: {[k: string]: any[]} = groupColKeys.reduce(
    (acc, k) => ({...acc, [k]: []}),
    {}
  )

  for (let i = 0; i < groupKeys.length; i++) {
    const groupKey = groupKeys[i]

    for (const bin of bins) {
      const count = bin.values[groupKey] || 0

      let yMin = 0

      if (position === 'stacked') {
        yMin = groupKeys
          .slice(0, i)
          .reduce((sum, k) => sum + (bin.values[k] || 0), 0)
      }

      xMinData.push(bin.min)
      xMaxData.push(bin.max)
      yMinData.push(yMin)
      yMaxData.push(yMin + count)
      countData.push(count)

      for (const [k, v] of Object.entries(groupsByGroupKey[groupKey])) {
        groupKeyData[k].push(v)
      }
    }
  }

  let binTable = newTable(binCount * groupKeys.length)
    .addColumn(X_MIN, xColType, xMinData)
    .addColumn(X_MAX, xColType, xMaxData)
    .addColumn(Y_MIN, 'number', yMinData)
    .addColumn(Y_MAX, 'number', yMaxData)
    .addColumn(COUNT, 'number', countData)

  for (const [key, data] of Object.entries(groupKeyData)) {
    const type = table.getColumnType(key)
    const name = table.getColumnName(key)

    binTable = binTable.addColumn(key, type, data, name)
  }

  return binTable
}

const createBins = (
  xDomain: number[],
  binCount: number
): Array<{max: number; min: number; values: {}}> => {
  const domainWidth = xDomain[1] - xDomain[0]
  const binWidth = domainWidth / binCount
  const binMinimums = range(xDomain[0], xDomain[1], binWidth)

  const bins = binMinimums.map((min, i) => {
    const isLastBin = i === binMinimums.length - 1
    const max = isLastBin ? xDomain[1] : binMinimums[i + 1]

    return {min, max, values: {}}
  })

  return bins
}

const resolveXDomain = (
  xColData: NumericColumnData,
  preferredXDomain?: number[]
): [number, number] => {
  let domain: [number, number]

  if (preferredXDomain) {
    domain = [preferredXDomain[0], preferredXDomain[1]]
  } else {
    domain = extent(xColData)
  }

  if (domain[0] === domain[1]) {
    // Widen domains of zero width by an arbitrary amount so that they can be
    // divided into bins
    domain[1] = domain[0] + 1
  }

  return domain
}

const getGroup = (table: Table, groupColKeys: string[], i: number) => {
  const result = {}

  for (const key of groupColKeys) {
    result[key] = table.getColumn(key)[i]
  }

  return result
}
