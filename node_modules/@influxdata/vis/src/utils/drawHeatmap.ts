import {range} from 'd3-array'
import {color} from 'd3-color'

import {Table, Scale} from '../types'
import {clearCanvas} from '../utils/clearCanvas'
import {X_MIN, X_MAX, Y_MIN, Y_MAX, COUNT} from '../constants/columnKeys'

interface DrawHeatmapOptions {
  canvas: HTMLCanvasElement
  width: number
  height: number
  table: Table
  xScale: Scale<number, number>
  yScale: Scale<number, number>
  fillScale: Scale<number, string>
  rows?: number[]
  brighten?: number
}

export const drawHeatmap = ({
  canvas,
  width,
  height,
  table,
  xScale,
  yScale,
  fillScale,
  rows,
  brighten,
}: DrawHeatmapOptions) => {
  clearCanvas(canvas, width, height)

  const context = canvas.getContext('2d')
  const indices = rows ? rows : range(table.length)

  const xMinData = table.getColumn(X_MIN, 'number')
  const xMaxData = table.getColumn(X_MAX, 'number')
  const yMinData = table.getColumn(Y_MIN, 'number')
  const yMaxData = table.getColumn(Y_MAX, 'number')
  const countData = table.getColumn(COUNT, 'number')

  for (const i of indices) {
    const xMin = xMinData[i]
    const xMax = xMaxData[i]
    const yMin = yMinData[i]
    const yMax = yMaxData[i]
    const count = countData[i]

    const squareX = xScale(xMin)
    const squareY = yScale(yMax)
    const squareWidth = xScale(xMax) - squareX
    const squareHeight = yScale(yMin) - squareY

    let fill = fillScale(count)

    if (brighten) {
      fill = color(fill)
        .brighter(3)
        .hex()
    }

    context.beginPath()
    context.rect(squareX, squareY, squareWidth, squareHeight)
    context.fillStyle = fill
    context.fill()
  }
}
