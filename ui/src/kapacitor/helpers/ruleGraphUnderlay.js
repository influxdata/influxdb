import {
  EQUAL_TO,
  LESS_THAN,
  NOT_EQUAL_TO,
  GREATER_THAN,
  INSIDE_RANGE,
  OUTSIDE_RANGE,
  EQUAL_TO_OR_LESS_THAN,
  EQUAL_TO_OR_GREATER_THAN,
} from 'src/kapacitor/constants'

const HIGHLIGHT = 'rgba(78, 216, 160, 0.3)'
const BACKGROUND = 'rgba(41, 41, 51, 1)'

const getFillColor = operator => {
  const backgroundColor = BACKGROUND
  const highlightColor = HIGHLIGHT

  if (operator === OUTSIDE_RANGE) {
    return backgroundColor
  }

  if (operator === NOT_EQUAL_TO) {
    return backgroundColor
  }

  return highlightColor
}

const underlayCallback = rule => (canvas, area, dygraph) => {
  const {values} = rule
  const {operator, value} = values

  if (rule.trigger !== 'threshold' || value === '' || !isFinite(value)) {
    return
  }

  const theOnePercent = 0.01
  let highlightStart = 0
  let highlightEnd = 0

  switch (operator) {
    case `${EQUAL_TO_OR_GREATER_THAN}`:
    case `${GREATER_THAN}`: {
      highlightStart = value
      highlightEnd = dygraph.yAxisRange()[1]
      break
    }

    case `${EQUAL_TO_OR_LESS_THAN}`:
    case `${LESS_THAN}`: {
      highlightStart = dygraph.yAxisRange()[0]
      highlightEnd = value
      break
    }

    case `${EQUAL_TO}`: {
      const width =
        theOnePercent * (dygraph.yAxisRange()[1] - dygraph.yAxisRange()[0])
      highlightStart = +value - width
      highlightEnd = +value + width
      break
    }

    case `${NOT_EQUAL_TO}`: {
      const width =
        theOnePercent * (dygraph.yAxisRange()[1] - dygraph.yAxisRange()[0])
      highlightStart = +value - width
      highlightEnd = +value + width

      canvas.fillStyle = HIGHLIGHT
      canvas.fillRect(area.x, area.y, area.w, area.h)
      break
    }

    case `${OUTSIDE_RANGE}`: {
      highlightStart = Math.min(+value, +values.rangeValue)
      highlightEnd = Math.max(+value, +values.rangeValue)

      canvas.fillStyle = HIGHLIGHT
      canvas.fillRect(area.x, area.y, area.w, area.h)
      break
    }

    case `${INSIDE_RANGE}`: {
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
