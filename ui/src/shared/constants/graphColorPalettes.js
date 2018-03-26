import {HexcodeToHSL, HSLToHexcode} from 'src/shared/constants/colorOperations'
import _ from 'lodash'

// Tier 5 Colors
const series1 = ['#22ADF6'] // Blue
const series2 = [...series1, '#4ED8A0'] // Green
const series3 = [...series2, '#7A65F2'] // Purple
const series4 = [...series3, '#F95F53'] // Red
const series5 = [...series4, '#FFB94A'] // Yellow
// Tier 4 Colors
const series6 = [...series5, '#00C9FF'] // Blu
const series7 = [...series6, '#7CE490'] // Green
const series8 = [...series7, '#9394FF'] // Purple
const series9 = [...series8, '#FF8564'] // Red
const series10 = [...series9, '#FFD255'] // Yellow
// Tier 6 Colors
const series11 = [...series10, '#4591ED'] // Blu
const series12 = [...series11, '#32B08C'] // Green
const series13 = [...series12, '#513CC6'] // Purple
const series14 = [...series13, '#DB4D4D'] // Red
const series15 = [...series14, '#F48D38'] // Yellow
// Tier 3 Colors
const series16 = [...series15, '#6BDFFF'] // Blu
const series17 = [...series16, '#A5F3B4'] // Green
const series18 = [...series17, '#B1B6FF'] // Purple
const series19 = [...series18, '#FFB6A0'] // Red
const series20 = [...series19, '#FFE480'] // Yellow

// All Colors
const graphColors = [
  series1,
  series2,
  series3,
  series4,
  series5,
  series6,
  series7,
  series8,
  series9,
  series10,
  series11,
  series12,
  series13,
  series14,
  series15,
  series16,
  series17,
  series18,
  series19,
  series20,
]

export const generateLargePalette = numSeries => {
  const start = {hue: 190, saturation: 90, lightness: 50}
  const end = {hue: 360, saturation: 80, lightness: 98}
  const colorsHSL = []

  for (let i = 0; i < numSeries; i++) {
    const hRange = end.hue - start.hue
    const hStep = hRange / (numSeries - 1)
    const h = hStep * i

    const sRange = end.saturation - start.saturation
    const sStep = sRange / (numSeries - 1)
    const s = sStep * i

    const lRange = end.lightness - start.lightness
    const lStep = lRange / (numSeries - 1)
    const l = lStep * i

    colorsHSL[i] = {
      hue: Math.floor(start.hue + h),
      saturation: Math.floor(start.saturation + s),
      lightness: Math.floor(start.lightness + l),
    }
  }

  const colorsHex = colorsHSL.map(color =>
    HSLToHexcode(color.hue, color.saturation, color.lightness)
  )

  return colorsHex
}

// Sort by hue
const sortColorsByHue = colors => {
  return _.sortBy(colors, color => {
    const {hue} = HexcodeToHSL(color)

    return hue
  })
}

// Color Finder
export const getIdealColors = numSeries => {
  if (numSeries > 18) {
    return generateLargePalette(numSeries)
  }
  const colors = graphColors[numSeries - 1]

  return sortColorsByHue(colors)
}
