import {ThresholdOperators} from 'src/kapacitor/constants'

const HIGHLIGHT = 'rgba(78, 216, 160, 0.3)'
const BACKGROUND = 'rgba(41, 41, 51, 1)'

const getFillColor = (operator: ThresholdOperators) => {
  const backgroundColor = BACKGROUND
  const highlightColor = HIGHLIGHT

  if (operator === ThresholdOperators.OutsideRange) {
    return backgroundColor
  }

  if (operator === ThresholdOperators.NotEqualTo) {
    return backgroundColor
  }

  return highlightColor
}

interface Area {
  x: number
  y: number
  w: number
  h: number
}

const underlayCallback = rule => (
  canvas: CanvasRenderingContext2D,
  area: Area,
  dygraph: Dygraph
) => {
  const {values} = rule
  const {operator, value} = values

  if (rule.trigger !== 'threshold' || value === '' || !isFinite(value)) {
    return
  }

  const theOnePercent = 0.01
  let highlightStart = 0
  let highlightEnd = 0

  switch (operator) {
    case ThresholdOperators.EqualToOrGreaterThan:
    case ThresholdOperators.GreaterThan: {
      highlightStart = value
      highlightEnd = dygraph.yAxisRange()[1]
      break
    }

    case ThresholdOperators.EqualToOrLessThan:
    case ThresholdOperators.LessThan: {
      highlightStart = dygraph.yAxisRange()[0]
      highlightEnd = value
      break
    }

    case ThresholdOperators.LessThan: {
      const width =
        theOnePercent * (dygraph.yAxisRange()[1] - dygraph.yAxisRange()[0])
      highlightStart = +value - width
      highlightEnd = +value + width
      break
    }

    case ThresholdOperators.NotEqualTo: {
      const width =
        theOnePercent * (dygraph.yAxisRange()[1] - dygraph.yAxisRange()[0])
      highlightStart = +value - width
      highlightEnd = +value + width

      canvas.fillStyle = HIGHLIGHT
      canvas.fillRect(area.x, area.y, area.w, area.h)
      break
    }

    case ThresholdOperators.OutsideRange: {
      highlightStart = Math.min(+value, +values.rangeValue)
      highlightEnd = Math.max(+value, +values.rangeValue)

      canvas.fillStyle = HIGHLIGHT
      canvas.fillRect(area.x, area.y, area.w, area.h)
      break
    }

    case ThresholdOperators.InsideRange: {
      highlightStart = Math.min(+value, +values.rangeValue)
      highlightEnd = Math.max(+value, +values.rangeValue)
      break
    }
  }

  const bottom = dygraph.toDomYCoord(highlightStart)
  const top = dygraph.toDomYCoord(highlightEnd)

  const fillColor = getFillColor(operator)
  canvas.fillStyle = fillColor
  canvas.fillRect(area.x, top, area.w, bottom - top)
}

export default underlayCallback
