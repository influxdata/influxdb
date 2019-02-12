import React, {useRef, useLayoutEffect, SFC} from 'react'

import {Scale, HistogramPosition} from 'src/minard'
import {clearCanvas} from 'src/minard/utils/clearCanvas'

const BAR_TRANSPARENCY = 0.5
const BAR_TRANSPARENCY_HOVER = 0.7
const BAR_PADDING = 1.5

interface Props {
  width: number
  height: number
  xMinCol: number[]
  xMaxCol: number[]
  yMinCol: number[]
  yMaxCol: number[]
  fillCol: string[] | boolean[] | number[]
  xScale: Scale<number, number>
  yScale: Scale<number, number>
  fillScale: Scale<string | number | boolean, string>
  position: HistogramPosition
  hoveredRowIndices: number[] | null
}

const drawBars = (
  canvas: HTMLCanvasElement,
  {
    width,
    height,
    xMinCol,
    xMaxCol,
    yMinCol,
    yMaxCol,
    fillCol,
    xScale,
    yScale,
    fillScale,
    hoveredRowIndices,
  }: Props
): void => {
  clearCanvas(canvas, width, height)

  const context = canvas.getContext('2d')

  for (let i = 0; i < yMaxCol.length; i++) {
    if (yMinCol[i] === yMaxCol[i]) {
      continue
    }

    const x = xScale(xMinCol[i])
    const y = yScale(yMaxCol[i])
    const width = xScale(xMaxCol[i]) - x - BAR_PADDING
    const height = yScale(yMinCol[i]) - y - BAR_PADDING
    const fill = fillScale(fillCol[i])
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
