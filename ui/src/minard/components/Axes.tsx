import React, {useRef, useLayoutEffect, SFC} from 'react'

import {PlotEnv, TICK_PADDING_RIGHT, TICK_PADDING_TOP} from 'src/minard'
import {clearCanvas} from 'src/minard/utils/clearCanvas'

interface Props {
  env: PlotEnv
  axesStroke?: string
  tickFont?: string
  tickFill?: string
}

export const drawAxes = (
  canvas: HTMLCanvasElement,
  env: PlotEnv,
  axesStroke: string,
  tickFont: string,
  tickFill: string
) => {
  const {
    width,
    height,
    margins,
    xTicks,
    yTicks,
    baseLayer: {
      scales: {x: xScale, y: yScale},
    },
  } = env

  clearCanvas(canvas, width, height)

  const context = canvas.getContext('2d')
  const xAxisY = height - margins.bottom

  // Draw x axis line
  context.strokeStyle = axesStroke
  context.beginPath()
  context.moveTo(margins.left, xAxisY)
  context.lineTo(width - margins.right, xAxisY)
  context.stroke()

  // Draw y axis line
  context.beginPath()
  context.moveTo(margins.left, xAxisY)
  context.lineTo(margins.left, margins.top)
  context.stroke()

  context.font = tickFont
  context.fillStyle = tickFill
  context.textAlign = 'center'
  context.textBaseline = 'top'

  // Draw and label each tick on the x axis
  for (const xTick of xTicks) {
    const x = xScale(xTick) + margins.left

    context.beginPath()
    context.moveTo(x, xAxisY)
    context.lineTo(x, margins.top)
    context.stroke()

    context.fillText(String(xTick), x, xAxisY + TICK_PADDING_TOP)
  }

  context.textAlign = 'end'
  context.textBaseline = 'middle'

  // Draw and label each tick on the y axis
  for (const yTick of yTicks) {
    const y = yScale(yTick) + margins.top

    context.beginPath()
    context.moveTo(margins.left, y)
    context.lineTo(width - margins.right, y)
    context.stroke()

    context.fillText(String(yTick), margins.left - TICK_PADDING_RIGHT, y)
  }
}

export const Axes: SFC<Props> = props => {
  const {children, env, tickFill, tickFont, axesStroke} = props
  const canvas = useRef<HTMLCanvasElement>(null)

  useLayoutEffect(
    () => drawAxes(canvas.current, env, axesStroke, tickFont, tickFill),
    [canvas.current, env, axesStroke, tickFont, tickFill]
  )

  return (
    <>
      {children}
      <canvas className="minard-axes" ref={canvas} />
    </>
  )
}
