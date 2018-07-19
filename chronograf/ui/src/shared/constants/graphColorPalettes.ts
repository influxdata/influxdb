import chroma from 'chroma-js'
import uuid from 'uuid'
import {LineColor} from 'src/types/colors'

const COLOR_TYPE_SCALE = 'scale'

// Color Palettes
export const LINE_COLORS_A = [
  {
    type: COLOR_TYPE_SCALE,
    hex: '#31C0F6',
    id: uuid.v4(),
    name: 'Nineteen Eighty Four',
    value: '0',
  },
  {
    type: COLOR_TYPE_SCALE,
    hex: '#A500A5',
    id: uuid.v4(),
    name: 'Nineteen Eighty Four',
    value: '0',
  },
  {
    type: COLOR_TYPE_SCALE,
    hex: '#FF7E27',
    id: uuid.v4(),
    name: 'Nineteen Eighty Four',
    value: '0',
  },
]

export const LINE_COLORS_B = [
  {
    type: COLOR_TYPE_SCALE,
    hex: '#74D495',
    id: uuid.v4(),
    name: 'Atlantis',
    value: '0',
  },
  {
    type: COLOR_TYPE_SCALE,
    hex: '#3F3FBA',
    id: uuid.v4(),
    name: 'Atlantis',
    value: '0',
  },
  {
    type: COLOR_TYPE_SCALE,
    hex: '#FF4D9E',
    id: uuid.v4(),
    name: 'Atlantis',
    value: '0',
  },
]

export const LINE_COLORS_C = [
  {
    type: COLOR_TYPE_SCALE,
    hex: '#8F8AF4',
    id: uuid.v4(),
    name: 'Do Androids Dream of Electric Sheep?',
    value: '0',
  },
  {
    type: COLOR_TYPE_SCALE,
    hex: '#A51414',
    id: uuid.v4(),
    name: 'Do Androids Dream of Electric Sheep?',
    value: '0',
  },
  {
    type: COLOR_TYPE_SCALE,
    hex: '#F4CF31',
    id: uuid.v4(),
    name: 'Do Androids Dream of Electric Sheep?',
    value: '0',
  },
]

export const LINE_COLORS_D = [
  {
    type: COLOR_TYPE_SCALE,
    hex: '#FD7A5D',
    id: uuid.v4(),
    name: 'Delorean',
    value: '0',
  },
  {
    type: COLOR_TYPE_SCALE,
    hex: '#5F1CF2',
    id: uuid.v4(),
    name: 'Delorean',
    value: '0',
  },
  {
    type: COLOR_TYPE_SCALE,
    hex: '#4CE09A',
    id: uuid.v4(),
    name: 'Delorean',
    value: '0',
  },
]

export const LINE_COLORS_E = [
  {
    type: COLOR_TYPE_SCALE,
    hex: '#FDC44F',
    id: uuid.v4(),
    name: 'Cthulhu',
    value: '0',
  },
  {
    type: COLOR_TYPE_SCALE,
    hex: '#007C76',
    id: uuid.v4(),
    name: 'Cthulhu',
    value: '0',
  },
  {
    type: COLOR_TYPE_SCALE,
    hex: '#8983FF',
    id: uuid.v4(),
    name: 'Cthulhu',
    value: '0',
  },
]

export const LINE_COLORS_F = [
  {
    type: COLOR_TYPE_SCALE,
    hex: '#DA6FF1',
    id: uuid.v4(),
    name: 'Ectoplasm',
    value: '0',
  },
  {
    type: COLOR_TYPE_SCALE,
    hex: '#00717A',
    id: uuid.v4(),
    name: 'Ectoplasm',
    value: '0',
  },
  {
    type: COLOR_TYPE_SCALE,
    hex: '#ACFF76',
    id: uuid.v4(),
    name: 'Ectoplasm',
    value: '0',
  },
]

export const LINE_COLORS_G = [
  {
    type: COLOR_TYPE_SCALE,
    hex: '#F6F6F8',
    id: uuid.v4(),
    name: 'T-Max 400 Film',
    value: '0',
  },
  {
    type: COLOR_TYPE_SCALE,
    hex: '#A4A8B6',
    id: uuid.v4(),
    name: 'T-Max 400 Film',
    value: '0',
  },
  {
    type: COLOR_TYPE_SCALE,
    hex: '#545667',
    id: uuid.v4(),
    name: 'T-Max 400 Film',
    value: '0',
  },
]

export const LINE_COLORS_RULE_GRAPH = [
  {
    type: COLOR_TYPE_SCALE,
    hex: '#7CE490',
    id: uuid.v4(),
    name: 'Honeydew',
    value: '0',
  },
  {
    type: COLOR_TYPE_SCALE,
    hex: '#22ADF6',
    id: uuid.v4(),
    name: 'Pool',
    value: '0',
  },
]

export const DEFAULT_LINE_COLORS = LINE_COLORS_A

export const LINE_COLOR_SCALES = [
  LINE_COLORS_A,
  LINE_COLORS_B,
  LINE_COLORS_C,
  LINE_COLORS_D,
  LINE_COLORS_E,
  LINE_COLORS_F,
  LINE_COLORS_G,
].map(colorScale => {
  const name = colorScale[0].name
  const colors = colorScale
  const id = colorScale[0].id

  return {name, colors, id}
})

export const validateLineColors = (
  colors: LineColor[],
  numSeries = 0
): LineColor[] => {
  const multipleSeriesButOneColor = numSeries > 1 && colors.length < 2
  if (!colors || colors.length === 0 || multipleSeriesButOneColor) {
    return DEFAULT_LINE_COLORS
  }

  const testColorsTypes =
    colors.filter(color => color.type === COLOR_TYPE_SCALE).length ===
    colors.length

  return testColorsTypes ? colors : DEFAULT_LINE_COLORS
}

export const getLineColorsHexes = (
  colors: LineColor[],
  numSeries: number
): string[] => {
  const validatedColors = validateLineColors(colors, numSeries) // ensures safe defaults
  const colorsHexArray = validatedColors.map(color => color.hex)

  if (numSeries === 1 || numSeries === 0) {
    return [colorsHexArray[0]]
  }
  if (numSeries === 2) {
    return [colorsHexArray[0], colorsHexArray[1]]
  }
  return chroma
    .scale(colorsHexArray)
    .mode('lch')
    .colors(numSeries)
}
