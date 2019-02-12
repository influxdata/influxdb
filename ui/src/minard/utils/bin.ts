import {extent, range, thresholdSturges} from 'd3-array'

import {Table, HistogramPosition, ColumnType} from 'src/minard'
import {assert} from 'src/minard/utils/assert'
import {getGroupKey} from 'src/minard/utils/getGroupKey'

/*
  Compute the data of a histogram visualization.

  The column specified by the `xColKey` will be divided into `binCount` evenly
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
  groupColKeys: string[] = [],
  binCount: number,
  position: HistogramPosition
) => {
  const xCol = table.columns[xColKey]
  const xColType = table.columnTypes[xColKey]

  assert(`could not find column "${xColKey}"`, !!xCol)
  assert(
    `unsupported value column type "${xColType}"`,
    xColType === ColumnType.Numeric || xColType === ColumnType.Temporal
  )

  const bins = createBins(xCol, binCount)

  // A group is the set of key-value pairs that a row takes on for the column
  // names specified in `groupColKeys`. The group key is a hashable
  // representation of the values of these pairs.
  const groupsByGroupKey = {}

  // Count x values by bin and group
  for (let i = 0; i < xCol.length; i++) {
    const xDatum = xCol[i]
    const group = getGroup(table, groupColKeys, i)
    const groupKey = getGroupKey(Object.values(group))

    const bin = bins.find(
      (b, i) => (xDatum < b.max && xDatum >= b.min) || i === bins.length - 1
    )

    groupsByGroupKey[groupKey] = group

    if (!bin.values[groupKey]) {
      bin.values[groupKey] = 1
    } else {
      bin.values[groupKey] += 1
    }
  }

  // Next, build up a tabular representation of each of these bins by group
  const statTable = {
    columns: {xMin: [], xMax: [], yMin: [], yMax: []},
    columnTypes: {
      xMin: xColType,
      xMax: xColType,
      yMin: ColumnType.Numeric,
      yMax: ColumnType.Numeric,
    },
  }

  // Include original columns used to group data in the resulting table
  for (const key of groupColKeys) {
    statTable.columns[key] = []
    statTable.columnTypes[key] = table.columnTypes[key]
  }

  const groupKeys = Object.keys(groupsByGroupKey)

  for (let i = 0; i < groupKeys.length; i++) {
    const groupKey = groupKeys[i]

    for (const bin of bins) {
      let yMin = 0

      if (position === HistogramPosition.Stacked) {
        yMin = groupKeys
          .slice(0, i)
          .reduce((sum, k) => sum + (bin.values[k] || 0), 0)
      }

      statTable.columns.xMin.push(bin.min)
      statTable.columns.xMax.push(bin.max)
      statTable.columns.yMin.push(yMin)
      statTable.columns.yMax.push(yMin + (bin.values[groupKey] || 0))

      for (const [k, v] of Object.entries(groupsByGroupKey[groupKey])) {
        statTable.columns[k].push(v)
      }
    }
  }

  const mappings: any = {
    xMin: 'xMin',
    xMax: 'xMax',
    yMin: 'yMin',
    yMax: 'yMax',
    fill: groupColKeys,
  }

  return [statTable, mappings]
}

const createBins = (
  col: number[],
  binCount: number
): Array<{max: number; min: number; values: {}}> => {
  if (!binCount) {
    binCount = thresholdSturges(col)
  }

  const domain = extent(col)

  if (domain[0] === domain[1]) {
    // Widen domains of zero width by an arbitrary amount so that they can be
    // divided into bins
    domain[1] += 1
  }

  const domainWidth = domain[1] - domain[0]
  const binWidth = domainWidth / binCount
  const binMinimums = range(domain[0], domain[1], binWidth)

  const bins = binMinimums.map((min, i) => {
    const max = i !== binMinimums.length - 1 ? binMinimums[i + 1] : domain[1]

    return {min, max, values: {}}
  })

  return bins
}

const getGroup = (table: Table, groupColKeys: string[], i: number) => {
  const result = {}

  for (const key of groupColKeys) {
    result[key] = table.columns[key][i]
  }

  return result
}
