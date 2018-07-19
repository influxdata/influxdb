import _ from 'lodash'
import chroma from 'chroma-js'

import {
  THRESHOLD_COLORS,
  THRESHOLD_TYPE_BASE,
  THRESHOLD_TYPE_TEXT,
} from 'src/shared/constants/thresholds'

import {CellType} from 'src/types/dashboards'

const getLegibleTextColor = bgColorHex => {
  const darkText = '#292933'
  const lightText = '#ffffff'

  const [red, green, blue] = chroma(bgColorHex).rgb()
  const average = (red + green + blue) / 3
  const mediumGrey = 128

  return average > mediumGrey ? darkText : lightText
}

const findNearestCrossedThreshold = (colors, lastValue) => {
  const sortedColors = _.sortBy(colors, color => Number(color.value))
  const nearestCrossedThreshold = sortedColors
    .filter(color => lastValue >= color.value)
    .pop()

  return nearestCrossedThreshold
}

export const stringifyColorValues = colors => {
  return colors.map(color => ({...color, value: `${color.value}`}))
}

export const generateThresholdsListHexs = ({
  colors,
  lastValue,
  cellType = 'line',
}) => {
  const defaultColoring = {
    bgColor: null,
    textColor:
      cellType === CellType.Table ? '#BEC2CC' : THRESHOLD_COLORS[11].hex,
  }
  const lastValueNumber = Number(lastValue) || 0
  let bgColor
  let textColor

  if (!colors.length) {
    return defaultColoring
  }

  // baseColor is expected in all cases
  const baseColor = colors.find(color => (color.id = THRESHOLD_TYPE_BASE)) || {
    hex: defaultColoring.textColor,
  }

  if (!lastValue) {
    return {...defaultColoring, textColor: baseColor.hex}
  }

  // If the single stat is above a line graph never have a background color
  if (cellType === CellType.LinePlusSingleStat) {
    return baseColor
      ? {bgColor: null, textColor: baseColor.hex}
      : defaultColoring
  }

  // When there is only a base color and it's applied to the text
  const shouldColorizeText = !!colors.find(
    color => color.type === THRESHOLD_TYPE_TEXT
  )
  if (shouldColorizeText && colors.length === 1) {
    return baseColor
      ? {bgColor: null, textColor: baseColor.hex}
      : defaultColoring
  }

  // When there's multiple colors and they're applied to the text
  if (shouldColorizeText && colors.length > 1) {
    const nearestCrossedThreshold = findNearestCrossedThreshold(
      colors,
      lastValueNumber
    )

    return {bgColor: null, textColor: nearestCrossedThreshold.hex}
  }

  // When there is only a base color and it's applued to the background
  if (colors.length === 1) {
    bgColor = baseColor.hex
    textColor = getLegibleTextColor(bgColor)

    return {bgColor, textColor}
  }

  // When there are multiple colors and they're applied to the background
  if (colors.length > 1) {
    const nearestCrossedThreshold = findNearestCrossedThreshold(
      colors,
      lastValueNumber
    )

    bgColor = nearestCrossedThreshold
      ? nearestCrossedThreshold.hex
      : baseColor.hex
    textColor = getLegibleTextColor(bgColor)

    return {bgColor, textColor}
  }

  // If all else fails, use safe default
  bgColor = null
  textColor = baseColor.hex
  return {bgColor, textColor}
}
