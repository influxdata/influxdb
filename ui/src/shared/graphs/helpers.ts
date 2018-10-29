import {toRGB} from 'src/shared/graphs/toRGB'

export const LINE_COLORS = [
  '#00C9FF',
  '#9394FF',
  '#4ED8A0',
  '#ff0054',
  '#ffcc00',
  '#33aa99',
  '#9dfc5d',
  '#92bcc3',
  '#ca96fb',
  '#ff00f0',
  '#38b94a',
  '#3844b9',
  '#a0725b',
]

export const SMALL_CELL_HEIGHT = 2

export const darkenColor = colorStr => {
  // Defined in dygraph-utils.js
  const color = toRGB(colorStr)
  color.r = Math.floor((255 + color.r) / 2)
  color.g = Math.floor((255 + color.g) / 2)
  color.b = Math.floor((255 + color.b) / 2)
  return `rgb(${color.r},${color.g},${color.b})`
}

// Bar Graph code below is adapted from http://dygraphs.com/tests/plotters.html
export const barPlotter = e => {
  // We need to handle all the series simultaneously.
  if (e.seriesIndex !== 0) {
    return
  }

  const g = e.dygraph
  const ctx = e.drawingContext
  const sets = e.allSeriesPoints
  const yBottom = e.dygraph.toDomYCoord(0)

  // Find the minimum separation between x-values.
  // This determines the bar width.
  let minSep = Infinity
  for (let j = 0; j < sets.length; j++) {
    const points = sets[j]
    for (let i = 1; i < points.length; i++) {
      const sep = points[i].canvasx - points[i - 1].canvasx
      if (sep < minSep) {
        minSep = sep
      }
    }
  }

  // calculate bar width using some graphics math while
  // ensuring a bar is never smaller than one px, so it is always rendered
  const barWidth = Math.max(Math.floor(2.0 / 3.0 * minSep), 1.0)

  const fillColors = []
  const strokeColors = g.getColors()

  let selPointX
  if (g.selPoints_ && g.selPoints_.length) {
    selPointX = g.selPoints_[0].canvasx
  }

  for (let i = 0; i < strokeColors.length; i++) {
    fillColors.push(darkenColor(strokeColors[i]))
  }

  ctx.lineWidth = 2

  for (let j = 0; j < sets.length; j++) {
    ctx.strokeStyle = strokeColors[j]
    for (let i = 0; i < sets[j].length; i++) {
      const p = sets[j][i]
      const centerX = p.canvasx
      ctx.fillStyle = fillColors[j]
      const xLeft =
        sets.length === 1
          ? centerX - barWidth
          : centerX - barWidth * (1 - j / sets.length)

      ctx.fillRect(
        xLeft,
        p.canvasy,
        barWidth / sets.length,
        yBottom - p.canvasy
      )

      // hover highlighting
      if (selPointX === centerX) {
        ctx.strokeRect(
          xLeft,
          p.canvasy,
          barWidth / sets.length,
          yBottom - p.canvasy
        )
      }
    }
  }
}

export const makeLegendStyles = (graph, legend, hoverTimeX) => {
  if (!graph || !legend || hoverTimeX === null) {
    return {}
  }

  // Move the Legend on hover
  const chronografChromeSize = 60 // Width & Height of navigation page elements
  const graphRect = graph.getBoundingClientRect()
  const legendRect = legend.getBoundingClientRect()

  const graphWidth = graphRect.width + 32 // Factoring in padding from parent
  const graphHeight = graphRect.height
  const graphBottom = graphRect.bottom
  const legendWidth = legendRect.width
  const legendHeight = legendRect.height
  const screenHeight = window.innerHeight
  const legendMaxLeft = graphWidth - legendWidth / 2

  let legendLeft = hoverTimeX

  // Enforcing max & min legend offsets
  if (hoverTimeX < legendWidth / 2) {
    legendLeft = legendWidth / 2
  } else if (hoverTimeX > legendMaxLeft) {
    legendLeft = legendMaxLeft
  }

  // Disallow screen overflow of legend
  const isLegendBottomClipped = graphBottom + legendHeight > screenHeight
  const isLegendTopClipped = legendHeight > graphRect.top - chronografChromeSize
  const willLegendFitLeft = hoverTimeX - chronografChromeSize > legendWidth

  let legendTop = graphHeight + 8

  // If legend is only clipped on the bottom, position above graph
  if (isLegendBottomClipped && !isLegendTopClipped) {
    legendTop = -legendHeight
  }

  // If legend is clipped on top and bottom, posiition on either side of crosshair
  if (isLegendBottomClipped && isLegendTopClipped) {
    legendTop = 0

    if (willLegendFitLeft) {
      legendLeft = hoverTimeX - legendWidth / 2
      legendLeft -= 8
    } else {
      legendLeft = hoverTimeX + legendWidth / 2
      legendLeft += 32
    }
  }

  return {
    left: `${legendLeft}px`,
    top: `${legendTop}px`,
  }
}

// globally matches anything that ends in a '.'
export const removeMeasurement = (label = '') => {
  const [measurement] = label.match(/^(.*)[.]/g) || ['']
  return label.replace(measurement, '')
}

export const OPTIONS = {
  rightGap: 0,
  axisLineWidth: 2,
  gridLineWidth: 1,
  animatedZooms: true,
  labelsSeparateLines: false,
  hideOverlayOnMouseOut: false,
  highlightSeriesBackgroundAlpha: 1.0,
  highlightSeriesBackgroundColor: 'rgb(41, 41, 51)',
}

export const hasherino = (str, len) =>
  str
    .split('')
    .map(char => char.charCodeAt(0))
    .reduce((hash, code) => hash + code, 0) % len

export const LABEL_WIDTH = 44
export const CHAR_PIXELS = 7
