import {range} from 'd3-array'

import {Scale, NumericColumnData} from '../types'
import {useLazyMemo} from './useLazyMemo'
import {isDefined} from './isDefined'
import {minBy} from './extrema'

export const useHoverLineIndices = (
  mode: 'x' | 'y' | 'xy',
  mouseX: number,
  mouseY: number,
  xColData: NumericColumnData,
  yColData: NumericColumnData,
  groupColData: string[],
  xScale: Scale<number, number>,
  yScale: Scale<number, number>,
  width: number,
  height: number
): number[] => {
  const isActive =
    mouseX !== undefined && mouseX !== null && mouseX >= 0 && mouseX < width

  const index = useLazyMemo(
    () => buildIndex(xColData, yColData, xScale, yScale, width, height),
    [xColData, yColData, xScale, yScale, width, height],
    isActive
  )

  if (!isActive) {
    return null
  }

  let hoverLineIndices

  if (mode === 'x') {
    hoverLineIndices = lookupIndex1D(
      index.xBins,
      mouseX,
      xScale.invert(mouseX),
      xColData,
      groupColData,
      width
    )
  } else if (mode === 'y') {
    hoverLineIndices = lookupIndex1D(
      index.yBins,
      mouseY,
      yScale.invert(mouseY),
      yColData,
      groupColData,
      height
    )
  } else {
    hoverLineIndices = lookupIndex2D(
      index.xyBins,
      mouseX,
      mouseY,
      xScale.invert(mouseX),
      yScale.invert(mouseY),
      xColData,
      yColData,
      width,
      height
    )
  }

  return hoverLineIndices
}

const INDEX_BIN_WIDTH = 30

type IndexTable = {
  xBins: number[][]
  yBins: number[][]
  xyBins: number[][][] // :-]
}

/*
  When a user hovers over a line graph, we need to show the points on each line
  closest to the mouse position. Since we don't want to look through the entire
  table to find the closest points on every hover event, we build an index on
  the first hover event that makes subsequent lookups fast.

  The index works by dividing the graph into discrete bins, each of them with a
  side length of `INDEX_BIN_WIDTH` pixels. In each bin we store the indices of
  rows in the original user-supplied table whose corresponding point lies
  within that bin.
  
  Then on every hover event, we can use some quick math to decide which bin the
  current hover corresponds to. Then we perform a linear search within each bin
  to find the point on each line closest to the mouse position.  This limits
  the amount of work we have to do, since we only need to scan through a single
  bin, rather than through the entire table.

  See `lookupIndex2D` and `lookupIndex1D` for more info.
*/
const buildIndex = (
  xColData: NumericColumnData,
  yColData: NumericColumnData,
  xScale: Scale<number, number>,
  yScale: Scale<number, number>,
  width: number,
  height: number
): IndexTable => {
  const xBinCount = Math.ceil(width / INDEX_BIN_WIDTH)
  const yBinCount = Math.ceil(height / INDEX_BIN_WIDTH)

  const indexTable = {
    xBins: range(xBinCount).map(_ => []),
    yBins: range(yBinCount).map(_ => []),
    xyBins: range(xBinCount).map(_ => range(yBinCount).map(_ => [])),
  }

  for (let i = 0; i < xColData.length; i++) {
    const x = xScale(xColData[i])
    const y = yScale(yColData[i])

    if (!isDefined(x) || !isDefined(y)) {
      continue
    }

    const xBinIndex = getBinIndex(x, width)
    const yBinIndex = getBinIndex(y, height)

    if (
      xBinIndex < 0 ||
      xBinIndex >= xBinCount ||
      yBinIndex < 0 ||
      yBinIndex >= yBinCount
    ) {
      continue
    }

    indexTable.xBins[xBinIndex].push(i)
    indexTable.yBins[yBinIndex].push(i)
    indexTable.xyBins[xBinIndex][yBinIndex].push(i)
  }

  return indexTable
}

const getBinIndex = (x: number, length: number): number => {
  const binCount = Math.ceil(length / INDEX_BIN_WIDTH)
  const binIndex = Math.floor((x / length) * binCount)

  return binIndex
}

/* 
  This function finds the single point in a line plot closest to an (x, y)
  position using a precomputed `IndexTable`.
  
  The `IndexTable` partitions the plot into a discrete collection of
  rectangles called "bins".
  
      -----------------------------
      | | | | | | | | | | | | | | |
      -----------------------------
      | | | | | | | | | | | | | | |
      -----------------------------
      | | | | | | | | | | | | | | |
      -----------------------------
      | | | | | | | | | | | | | | |
      -----------------------------
      | | | | | | | | | | | | | | |
      -----------------------------
  
  Each bin stores references to the points contained within the bin in the
  visualization. Each point reference is the index of a row in the original
  user-supplied table that contains the data of the point.
  
  When the user hovers over the plot, we can lookup the bin corresponding to
  the (x, y) position of their mouse in constant time. In the following
  picture, that bin is marked with an `x`:
  
      -----------------------------
      | | | | | | | | | | | | | | |
      -----------------------------
      | | | | | | | | | | | | | | |
      -----------------------------
      | | | | | | | | | |x| | | | |
      -----------------------------
      | | | | | | | | | | | | | | |
      -----------------------------
      | | | | | | | | | | | | | | |
      -----------------------------
  
  We search through points in that bin to find the point closet to the exact
  (x, y) position of the mouse. If we find such a point, we return it
  immediately (as an index reference). However, there may be no points in the
  bin, in which case we need to expand our selection to nearby bins. We do so
  by searching the bins that border the initial bin:
  
      -----------------------------
      | | | | | | | | | | | | | | |
      -----------------------------
      | | | | | | | | |x|x|x| | | |
      -----------------------------
      | | | | | | | | |x| |x| | | |
      -----------------------------
      | | | | | | | | |x|x|x| | | |
      -----------------------------
      | | | | | | | | | | | | | | |
      -----------------------------
  
  If we still don't find a point, we expand the search again:
  
      -----------------------------
      | | | | | | | |x|x|x|x|x| | |
      -----------------------------
      | | | | | | | |x| | | |x| | |
      -----------------------------
      | | | | | | | |x| | | |x| | |
      -----------------------------
      | | | | | | | |x| | | |x| | |
      -----------------------------
      | | | | | | | |x|x|x|x|x| | |
      -----------------------------
  
  Expansion will stop at the bins that border the plot. So the next iteration
  would only expand horizontally, but not vertically:
  
      -----------------------------
      | | | | | | |x|x|x|x|x|x|x| |
      -----------------------------
      | | | | | | |x| | | | | |x| |
      -----------------------------
      | | | | | | |x| | | | | |x| |
      -----------------------------
      | | | | | | |x| | | | | |x| |
      -----------------------------
      | | | | | | |x|x|x|x|x|x|x| |
      -----------------------------

  When there is no where left to expand, we stop the search. 
*/

const lookupIndex2D = (
  bins: number[][][],
  mouseX: number,
  mouseY: number,
  dataX: number,
  dataY: number,
  xColData: NumericColumnData,
  yColData: NumericColumnData,
  width: number,
  height: number
): number[] => {
  const xBinCount = bins.length
  const yBinCount = bins[0].length

  let i0 = Math.max(0, getBinIndex(mouseX, width))
  let i1 = Math.min(xBinCount - 1, getBinIndex(mouseX, width))
  let j0 = Math.max(0, getBinIndex(mouseY, height))
  let j1 = Math.min(yBinCount - 1, getBinIndex(mouseY, height))

  let closestRowIndices = bins[i0][j0]
  let hasSearchedEverything =
    i0 === 0 && i1 === xBinCount - 1 && j0 === 0 && j1 === yBinCount - 1

  while (closestRowIndices.length === 0 && !hasSearchedEverything) {
    i0 = Math.max(0, i0 - 1)
    i1 = Math.min(xBinCount - 1, i1 + 1)
    j0 = Math.max(0, j0 - 1)
    j1 = Math.min(yBinCount - 1, j1 + 1)

    closestRowIndices = collectClosestRowIndices(i0, i1, j0, j1, bins)

    hasSearchedEverything =
      i0 === 0 && i1 === xBinCount - 1 && j0 === 0 && j1 === yBinCount - 1
  }

  if (closestRowIndices.length === 0) {
    return []
  }

  const nearestIndex = minBy(
    rowIndex => sqDist(dataX, dataY, xColData[rowIndex], yColData[rowIndex]),
    closestRowIndices
  )

  return [nearestIndex]
}

const collectClosestRowIndices = (i0, i1, j0, j1, bins): number[] => {
  let closestRowIndices = []

  let x = i0
  let y = j0

  while (x <= i1) {
    closestRowIndices.push(...bins[x][y])
    x++
  }

  x--
  y++

  while (y <= j1) {
    closestRowIndices.push(...bins[x][y])
    y++
  }

  y--
  x--

  while (x >= i0) {
    closestRowIndices.push(...bins[x][y])
    x--
  }

  x++
  y--

  while (y >= j0) {
    closestRowIndices.push(...bins[x][y])
    y--
  }

  return closestRowIndices
}

const sqDist = (x0: number, y0: number, x1: number, y1: number): number => {
  return (x1 - x0) ** 2 + (y1 - y0) ** 2
}

interface NearestIndexByGroup {
  [groupKey: string]: {i: number; distance: number}
}

/*
  This function is the one-dimensional analogue of `lookupIndex2D`; see
  `lookupIndex2D` function for more info.
*/
const lookupIndex1D = (
  bins: number[][],
  mouseCoord: number,
  dataCoord: number,
  colData: NumericColumnData,
  groupColData: string[],
  length: number
): number[] => {
  const initialBinIndex = getBinIndex(mouseCoord, length)
  const maxBinIndex = bins.length - 1

  let leftBinIndex = Math.max(0, initialBinIndex - 1)
  let rightBinIndex = Math.min(initialBinIndex, maxBinIndex)
  let leftRowIndices = bins[leftBinIndex]
  let rightRowIndices = bins[rightBinIndex]

  // If no points are within the current left and right bins, expand the search
  // outward to the next nearest bins
  while (
    !leftRowIndices.length &&
    !rightRowIndices.length &&
    (leftBinIndex !== 0 || rightBinIndex !== maxBinIndex)
  ) {
    leftBinIndex = Math.max(0, leftBinIndex - 1)
    rightBinIndex = Math.min(rightBinIndex + 1, maxBinIndex)
    leftRowIndices = bins[leftBinIndex]
    rightRowIndices = bins[rightBinIndex]
  }

  let nearestIndexByGroup: NearestIndexByGroup = {}

  collectNearestIndices(
    nearestIndexByGroup,
    leftRowIndices,
    dataCoord,
    colData,
    groupColData
  )

  collectNearestIndices(
    nearestIndexByGroup,
    rightRowIndices,
    dataCoord,
    colData,
    groupColData
  )

  const nearestRows = Object.values(nearestIndexByGroup)

  if (!nearestRows.length) {
    return []
  }

  const nearestDistance = Math.min(...nearestRows.map(d => d.distance))

  return nearestRows.filter(d => d.distance === nearestDistance).map(d => d.i)
}

const collectNearestIndices = (
  acc: NearestIndexByGroup,
  rowIndices: number[],
  dataCoord: number,
  colData: NumericColumnData,
  groupColData: string[]
): void => {
  for (const i of rowIndices) {
    const group = groupColData[i]
    const distance = Math.floor(Math.abs(dataCoord - colData[i]))

    if (!acc[group] || distance < acc[group].distance) {
      acc[group] = {i, distance}
    }
  }
}
