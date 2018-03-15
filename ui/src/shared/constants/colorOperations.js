import _ from 'lodash'
import {
  GAUGE_COLORS,
  THRESHOLD_TYPE_BASE,
  THRESHOLD_TYPE_TEXT,
} from 'shared/constants/thresholds'

const hexToRgb = hex => {
  const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex)
  return result
    ? {
        r: parseInt(result[1], 16),
        g: parseInt(result[2], 16),
        b: parseInt(result[3], 16),
      }
    : null
}

const averageRgbValues = valuesObject => {
  const {r, g, b} = valuesObject
  return (r + g + b) / 3
}

const trueNeutralGrey = 128

const getLegibleTextColor = bgColorHex => {
  const averageBackground = averageRgbValues(hexToRgb(bgColorHex))
  const isBackgroundLight = averageBackground > trueNeutralGrey

  const darkText = '#292933'
  const lightText = '#ffffff'

  return isBackgroundLight ? darkText : lightText
}

const findNearestCrossedThreshold = (colors, lastValue) => {
  const sortedColors = _.sortBy(colors, color => Number(color.value))
  const nearestCrossedThreshold = sortedColors
    .filter(color => lastValue > color.value)
    .pop()

  return nearestCrossedThreshold
}

export const stringifyColorValues = colors => {
  return colors.map(color => ({...color, value: `${color.value}`}))
}

export const generateThresholdsListHexs = (
  colors,
  lastValue,
  containsLineGraph
) => {
  const defaultColoring = {bgColor: null, textColor: GAUGE_COLORS[11].hex}
  const lastValueNumber = Number(lastValue) || 0

  if (!colors.length || !lastValue) {
    return defaultColoring
  }

  // baseColor is expected in all cases
  const baseColor = colors.find(color => (color.id = THRESHOLD_TYPE_BASE)) || {
    hex: defaultColoring.textColor,
  }

  // If the single stat is above a line graph never have a background color
  if (containsLineGraph) {
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
    const bgColor = null
    const textColor = nearestCrossedThreshold.hex

    return {bgColor, textColor}
  }

  // When there is only a base color and it's applued to the background
  if (colors.length === 1) {
    const bgColor = baseColor.hex
    const textColor = getLegibleTextColor(bgColor)

    return {bgColor, textColor}
  }

  // When there are multiple colors and they're applied to the background
  if (colors.length > 1) {
    const nearestCrossedThreshold = findNearestCrossedThreshold(
      colors,
      lastValueNumber
    )

    const bgColor = nearestCrossedThreshold
      ? nearestCrossedThreshold.hex
      : baseColor.hex
    const textColor = getLegibleTextColor(bgColor)

    return {bgColor, textColor}
  }

  // If all else fails, use safe default
  const bgColor = null
  const textColor = baseColor.hex
  return {bgColor, textColor}
}
