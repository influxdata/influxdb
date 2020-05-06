import {line, curveLinear} from 'd3-shape'
import {range} from 'd3-array'

import {LineInterpolation} from '../types'
import {LineData} from './lineData'
import {CURVES} from '../constants'
import {isDefined} from '../utils/isDefined'

interface DrawLinesOptions {
  canvas: HTMLCanvasElement
  interpolation: LineInterpolation
  lineData: LineData
  lineWidth?: number
}

export const drawLines = ({
  canvas,
  lineData,
  interpolation,
  lineWidth = 1,
}: DrawLinesOptions): void => {
  const context = canvas.getContext('2d')

  context.lineWidth = lineWidth

  for (const {xs, ys, fill} of Object.values(lineData)) {
    const lineGenerator: any = line()
      .context(context)
      .x((i: any) => xs[i])
      .y((i: any) => ys[i])
      .defined((i: any) => isDefined(xs[i]) && isDefined(ys[i]))
      .curve(CURVES[interpolation] || curveLinear)

    context.strokeStyle = fill
    context.beginPath()
    lineGenerator(range(0, xs.length))
    context.stroke()
  }
}
