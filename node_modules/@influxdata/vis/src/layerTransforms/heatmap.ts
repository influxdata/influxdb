import {extent, range} from 'd3-array'
import {interpolateRgbBasis} from 'd3-interpolate'
import {scaleSequential} from 'd3-scale'

import {newTable} from '../utils/newTable'
import {Table, HeatmapScales, HeatmapMappings, Scale} from '../types'
import {X_MIN, X_MAX, Y_MIN, Y_MAX, COUNT} from '../constants/columnKeys'

export const getHeatmapTable = (
  table: Table,
  xColKey: string,
  yColKey: string,
  xDomain: number[],
  yDomain: number[],
  width: number,
  height: number,
  binSize: number
): Table =>
  bin2d(table, xColKey, yColKey, xDomain, yDomain, width, height, binSize)

export const getHeatmapScales = (
  table: Table,
  colors: string[]
): HeatmapScales => {
  const domain = extent(table.getColumn(COUNT, 'number'))
  const colorScheme = interpolateRgbBasis(colors)
  const fillScale = scaleSequential(colorScheme).domain(domain)

  return {
    fill: fillScale as Scale<number, string>,
  }
}

export const getHeatmapMappings = (): HeatmapMappings => ({
  xMin: 'xMin',
  xMax: 'xMax',
  yMin: 'yMin',
  yMax: 'yMax',
  fill: 'count',
})

export const bin2d = (
  table: Table,
  xColKey: string,
  yColKey: string,
  xDomain: number[],
  yDomain: number[],
  width: number,
  height: number,
  binSize: number
): Table => {
  const xColData = table.getColumn(xColKey, 'number')
  const yColData = table.getColumn(yColKey, 'number')
  const xColType = table.getColumnType(xColKey) as 'time' | 'number'
  const yColType = table.getColumnType(yColKey) as 'time' | 'number'

  if (!xDomain) {
    xDomain = extent(xColData)
  }

  if (!yDomain) {
    yDomain = extent(yColData)
  }

  const xBinCount = Math.floor(width / binSize)
  const yBinCount = Math.floor(height / binSize)

  // Count occurences in each bin in a `xBinCount` by `yBinCount` matrix
  //
  //                 4th y bin
  //
  //                     │
  //                     │
  //                     v
  //       [
  //           [0, 1, 2, 0, 0],
  //           [0, 1, 0, 2, 0],  <──── 2nd x bin
  //           [1, 0, 5, 7, 3]
  //       ]
  //
  const bins = range(xBinCount).map(__ => new Array(yBinCount).fill(0))

  for (let i = 0; i < table.length; i++) {
    const x = xColData[i]
    const y = yColData[i]

    const shouldSkipPoint =
      !x ||
      !y ||
      x < xDomain[0] ||
      x > xDomain[1] ||
      y < yDomain[0] ||
      y > yDomain[1]

    if (shouldSkipPoint) {
      continue
    }

    const xBinIndex = getBinIndex(x, xDomain, xBinCount)
    const yBinIndex = getBinIndex(y, yDomain, yBinCount)

    bins[xBinIndex][yBinIndex] += 1
  }

  // Now build a `Table` from that matrix

  const xBinWidth = (xDomain[1] - xDomain[0]) / xBinCount
  const yBinWidth = (yDomain[1] - yDomain[0]) / yBinCount

  const xMinData: number[] = []
  const xMaxData: number[] = []
  const yMinData: number[] = []
  const yMaxData: number[] = []
  const countData: number[] = []

  for (let i = 0; i < xBinCount; i++) {
    for (let j = 0; j < yBinCount; j++) {
      xMinData.push(xDomain[0] + i * xBinWidth)
      xMaxData.push(xDomain[0] + (i + 1) * xBinWidth)
      yMinData.push(yDomain[0] + j * yBinWidth)
      yMaxData.push(yDomain[0] + (j + 1) * yBinWidth)
      countData.push(bins[i][j])
    }
  }

  const heatmapTable = newTable(xMinData.length)
    .addColumn(X_MIN, xColType, xMinData)
    .addColumn(X_MAX, xColType, xMaxData)
    .addColumn(Y_MIN, yColType, yMinData)
    .addColumn(Y_MAX, yColType, yMaxData)
    .addColumn(COUNT, 'number', countData)

  return heatmapTable
}

const getBinIndex = (val: number, domain: number[], binCount: number) => {
  const domainWidth = domain[1] - domain[0]
  const percentage = (val - domain[0]) / domainWidth

  let binIndex = Math.floor(percentage * binCount)

  if (binIndex === binCount) {
    // Special case: last bin is inclusive
    binIndex = binCount - 1
  }

  return binIndex
}
