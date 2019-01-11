import _ from 'lodash'
import chroma from 'chroma-js'

import {
  THRESHOLD_COLORS,
  THRESHOLD_TYPE_BASE,
  THRESHOLD_TYPE_TEXT,
} from 'src/shared/constants/thresholds'

import {ViewType} from 'src/types/v2/dashboards'
import {Color} from 'src/types/colors'

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
}: {
  colors: Color[]
  lastValue: string | number | null
  cellType: string
}) => {
  const defaultColoring = {
    bgColor: null,
    textColor:
      cellType === ViewType.Table ? '#BEC2CC' : THRESHOLD_COLORS[11].hex,
  }

  const lastValueNumber = Number(lastValue) || 0

  if (!colors.length) {
    return defaultColoring
  }

  // baseColor is expected in all cases
  const baseColor = colors.find(color => color.id === THRESHOLD_TYPE_BASE) || {
    hex: defaultColoring.textColor,
  }

  if (!lastValue) {
    return {...defaultColoring, textColor: baseColor.hex}
  }

  // If the single stat is above a line graph never have a background color
  if (cellType === ViewType.LinePlusSingleStat) {
    return baseColor
      ? {bgColor: null, textColor: baseColor.hex}
      : defaultColoring
  }

  // When there is only a base color and it's applied to the text
  const shouldColorizeText = !!colors.find(
    color => color.type === THRESHOLD_TYPE_TEXT
  )

  if (shouldColorizeText && colors.length === 1 && baseColor) {
    return {bgColor: null, textColor: baseColor.hex}
  }

  if (shouldColorizeText && colors.length === 1) {
    return defaultColoring
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
    return {
      bgColor: baseColor.hex,
      textColor: getLegibleTextColor(baseColor.hex),
    }
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

    return {bgColor, textColor: getLegibleTextColor(bgColor)}
  }

  return {bgColor: null, textColor: baseColor.hex}
}
