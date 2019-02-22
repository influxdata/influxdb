import React, {useRef, useLayoutEffect, SFC} from 'react'

import {Scale, HistogramPosition, Layer} from 'src/minard'
import {clearCanvas} from 'src/minard/utils/clearCanvas'
import {getBarFill} from 'src/minard/utils/getBarFill'

const BAR_TRANSPARENCY = 0.5
const BAR_TRANSPARENCY_HOVER = 0.7
const BAR_PADDING = 1.5

interface Props {
  width: number
  height: number
  layer: Layer
  xScale: Scale<number, number>
  yScale: Scale<number, number>
  position: HistogramPosition
  hoveredRowIndices: number[] | null
}

const drawBars = (
  canvas: HTMLCanvasElement,
  {width, height, layer, xScale, yScale, hoveredRowIndices}: Props
): void => {
  clearCanvas(canvas, width, height)

  const {table, mappings} = layer
  const xMinCol = table.columns[mappings.xMin]
  const xMaxCol = table.columns[mappings.xMax]
  const yMinCol = table.columns[mappings.yMin]
  const yMaxCol = table.columns[mappings.yMax]

  const context = canvas.getContext('2d')

  for (let i = 0; i < yMaxCol.length; i++) {
    if (yMinCol[i] === yMaxCol[i]) {
      // Skip 0-height bars
      continue
    }

    const x = xScale(xMinCol[i])
    const y = yScale(yMaxCol[i])
    const width = xScale(xMaxCol[i]) - x - BAR_PADDING
    const height = yScale(yMinCol[i]) - y - BAR_PADDING
    const fill = getBarFill(layer, i)
    const alpha =
      hoveredRowIndices && hoveredRowIndices.includes(i)
        ? BAR_TRANSPARENCY_HOVER
        : BAR_TRANSPARENCY

    // See https://stackoverflow.com/a/45125187
    context.beginPath()
    context.rect(x, y, width, height)
    context.save()
    context.clip()
    context.lineWidth = 2
    context.globalAlpha = alpha
    context.fillStyle = fill
    context.fill()
    context.globalAlpha = 1
    context.strokeStyle = fill
    context.stroke()
    context.restore()
  }
}

const HistogramBars: SFC<Props> = props => {
  const canvas = useRef<HTMLCanvasElement>(null)

  useLayoutEffect(() => drawBars(canvas.current, props))

  return <canvas className="minard-layer histogram" ref={canvas} />
}

export default React.memo(HistogramBars)
