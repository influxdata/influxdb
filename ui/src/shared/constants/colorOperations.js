import _ from 'lodash'
import {
  THRESHOLD_COLORS,
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

export const generateThresholdsListHexs = ({
  colors,
  lastValue,
  cellType = 'line',
}) => {
  const defaultColoring = {
    bgColor: null,
    textColor: cellType === 'table' ? '#BEC2CC' : THRESHOLD_COLORS[11].hex,
  }
  const lastValueNumber = Number(lastValue) || 0

  if (!colors.length || !lastValue) {
    return defaultColoring
  }

  // baseColor is expected in all cases
  const baseColor = colors.find(color => (color.id = THRESHOLD_TYPE_BASE)) || {
    hex: defaultColoring.textColor,
  }

  // If the single stat is above a line graph never have a background color
  if (cellType === 'line-plus-single-stat') {
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

// Handy tool for converting Hexcodes to HSL values
export const HexcodeToHSL = hex => {
  const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex)

  const red = parseInt(result[1], 16) / 255
  const green = parseInt(result[2], 16) / 255
  const blue = parseInt(result[3], 16) / 255
  const max = Math.max(red, green, blue)
  const min = Math.min(red, green, blue)

  let hue,
    saturation,
    lightness = (max + min) / 2

  if (max === min) {
    hue = saturation = 0 // achromatic
  } else {
    const d = max - min
    saturation = lightness > 0.5 ? d / (2 - max - min) : d / (max + min)
    switch (max) {
      case red:
        hue = (green - blue) / d + (green < blue ? 6 : 0)
        break
      case green:
        hue = (blue - red) / d + 2
        break
      case blue:
        hue = (red - green) / d + 4
        break
    }
    hue /= 6
  }

  hue = Math.round(360 * hue)
  saturation = Math.round(saturation * 100)
  lightness = Math.round(lightness * 100)

  return {hue, saturation, lightness}
}

// Handy tool for converting HSL values to Hexcode
export const HSLToHexcode = (hue, saturation, lightness) => {
  hue /= 360
  saturation /= 100
  lightness /= 100
  let red, green, blue
  if (saturation === 0) {
    red = green = blue = lightness // achromatic
  } else {
    const hue2rgb = (p, q, t) => {
      if (t < 0) {
        t += 1
      }
      if (t > 1) {
        t -= 1
      }
      if (t < 1 / 6) {
        return p + (q - p) * 6 * t
      }
      if (t < 1 / 2) {
        return q
      }
      if (t < 2 / 3) {
        return p + (q - p) * (2 / 3 - t) * 6
      }
      return p
    }
    const q =
      lightness < 0.5
        ? lightness * (1 + saturation)
        : lightness + saturation - lightness * saturation
    const p = 2 * lightness - q
    red = hue2rgb(p, q, hue + 1 / 3)
    green = hue2rgb(p, q, hue)
    blue = hue2rgb(p, q, hue - 1 / 3)
  }
  const toHex = x => {
    const hex = Math.round(x * 255).toString(16)
    return hex.length === 1 ? `0${hex}` : hex
  }
  return `#${toHex(red)}${toHex(green)}${toHex(blue)}`
}
