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

export const isBackgroundLight = backgroundColor => {
  const averageBackground = averageRgbValues(hexToRgb(backgroundColor))
  const isLight = averageBackground > trueNeutralGrey

  return isLight
}
