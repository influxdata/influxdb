import {getTextMetrics} from './getTextMetrics'
import {maxBy} from './extrema'

import {
  TICK_PADDING_TOP,
  TICK_PADDING_RIGHT,
  AXIS_LABEL_PADDING_BOTTOM,
} from '../constants'

export const getMargins = (
  showAxes: boolean,
  xAxisLabel: string,
  yAxisLabel: string,
  yTicks: number[],
  yTickFormatter: (tick: number) => string,
  tickFont: string
) => {
  if (!showAxes) {
    return {top: 1, right: 1, bottom: 1, left: 1}
  }

  const longestYTick = maxBy(d => d.length, yTicks.map(t => yTickFormatter(t)))

  const {width: maxTextWidth, height: textHeight} = getTextMetrics(
    tickFont,
    longestYTick
  )

  const xAxisLabelHeight = xAxisLabel
    ? textHeight + AXIS_LABEL_PADDING_BOTTOM
    : 0

  const yAxisLabelHeight = yAxisLabel
    ? textHeight + AXIS_LABEL_PADDING_BOTTOM
    : 0

  return {
    top: textHeight / 2,
    right: 1,
    bottom: textHeight + TICK_PADDING_TOP + xAxisLabelHeight,
    left: maxTextWidth + TICK_PADDING_RIGHT + yAxisLabelHeight,
  }
}
