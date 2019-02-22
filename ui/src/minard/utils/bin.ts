import {extent, range, thresholdSturges} from 'd3-array'

import {
  Table,
  HistogramTable,
  HistogramMappings,
  HistogramPosition,
  NumericColumnType,
  isNumeric,
} from 'src/minard'
import {assert} from 'src/minard/utils/assert'
import {getGroupKey} from 'src/minard/utils/getGroupKey'

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
  xColName: string,
  xDomain: [number, number],
  groupColNames: string[] = [],
  binCount: number,
  position: HistogramPosition
): [HistogramTable, HistogramMappings] => {
  const col = table.columns[xColName]

  assert(`could not find column "${xColName}"`, !!col)
  assert(`unsupported value column type "${col.type}"`, isNumeric(col.type))

  const xCol = col.data as number[]
  const xColType = col.type as NumericColumnType

  if (!binCount) {
    binCount = thresholdSturges(xCol)
  }

  xDomain = resolveXDomain(xCol, xDomain)

  const bins = createBins(xDomain, binCount)

  // A group is the set of key-value pairs that a row takes on for the column
  // names specified in `groupColNames`. The group key is a hashable
  // representation of the values of these pairs.
  const groupsByGroupKey = {}

  // Count x values by bin and group
  for (let i = 0; i < xCol.length; i++) {
    const x = xCol[i]

    const shouldSkipPoint =
      x === undefined ||
      x === null ||
      isNaN(x) ||
      x < xDomain[0] ||
      x > xDomain[1]

    if (shouldSkipPoint) {
      continue
    }

    const group = getGroup(table, groupColNames, i)
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
  const statTable = {
    columns: {
      xMin: {
        data: [],
        type: xColType,
      },
      xMax: {
        data: [],
        type: xColType,
      },
      yMin: {
        data: [],
        type: 'int',
      },
      yMax: {
        data: [],
        type: 'int',
      },
    },
    length: binCount * groupKeys.length,
  }

  // Include original columns used to group data in the resulting table
  for (const name of groupColNames) {
    statTable.columns[name] = {
      data: [],
      type: table.columns[name].type,
    }
  }

  for (let i = 0; i < groupKeys.length; i++) {
    const groupKey = groupKeys[i]

    for (const bin of bins) {
      let yMin = 0

      if (position === HistogramPosition.Stacked) {
        yMin = groupKeys
          .slice(0, i)
          .reduce((sum, k) => sum + (bin.values[k] || 0), 0)
      }

      statTable.columns.xMin.data.push(bin.min)
      statTable.columns.xMax.data.push(bin.max)
      statTable.columns.yMin.data.push(yMin)
      statTable.columns.yMax.data.push(yMin + (bin.values[groupKey] || 0))

      for (const [k, v] of Object.entries(groupsByGroupKey[groupKey])) {
        statTable.columns[k].data.push(v)
      }
    }
  }

  const mappings: HistogramMappings = {
    xMin: 'xMin',
    xMax: 'xMax',
    yMin: 'yMin',
    yMax: 'yMax',
    fill: groupColNames,
  }

  return [statTable as HistogramTable, mappings]
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
  xCol: number[],
  preferredXDomain?: [number, number]
): [number, number] => {
  let domain: [number, number]

  if (preferredXDomain) {
    domain = [preferredXDomain[0], preferredXDomain[1]]
  } else {
    domain = extent(xCol)
  }

  if (domain[0] === domain[1]) {
    // Widen domains of zero width by an arbitrary amount so that they can be
    // divided into bins
    domain[1] = domain[0] + 1
  }

  return domain
}

const getGroup = (table: Table, groupColNames: string[], i: number) => {
  const result = {}

  for (const key of groupColNames) {
    result[key] = table.columns[key].data[i]
  }

  return result
}
