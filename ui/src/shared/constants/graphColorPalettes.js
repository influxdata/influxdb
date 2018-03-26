import _ from 'lodash'
import chroma from 'chroma-js'

// Color Palettes
export const LINE_COLORS_A = [
  {
    type: 'scale',
    hex: '#31C0F6',
    id: '0',
    name: 'Nineteen Eighty Four',
    value: 0,
  },
  {
    type: 'scale',
    hex: '#A500A5',
    id: '0',
    name: 'Nineteen Eighty Four',
    value: 0,
  },
  {
    type: 'scale',
    hex: '#FF7E27',
    id: '0',
    name: 'Nineteen Eighty Four',
    value: 0,
  },
]

export const LINE_COLORS_B = [
  {
    type: 'scale',
    hex: '#74D495',
    id: '1',
    name: 'Atlantis',
    value: 0,
  },
  {
    type: 'scale',
    hex: '#3F3FBA',
    id: '1',
    name: 'Atlantis',
    value: 0,
  },
  {
    type: 'scale',
    hex: '#EA5994',
    id: '1',
    name: 'Atlantis',
    value: 0,
  },
]

export const LINE_COLORS_C = [
  {
    type: 'scale',
    hex: '#8F8AF4',
    id: '1',
    name: 'Glarbh',
    value: 0,
  },
  {
    type: 'scale',
    hex: '#A51414',
    id: '1',
    name: 'Glarbh',
    value: 0,
  },
  {
    type: 'scale',
    hex: '#F4CF31',
    id: '1',
    name: 'Glarbh',
    value: 0,
  },
]

export const LINE_COLORS_D = [
  {
    type: 'scale',
    hex: '#FD7A5D',
    id: '1',
    name: 'Spoot',
    value: 0,
  },
  {
    type: 'scale',
    hex: '#5F1CF2',
    id: '1',
    name: 'Spoot',
    value: 0,
  },
  {
    type: 'scale',
    hex: '#4CE09A',
    id: '1',
    name: 'Spoot',
    value: 0,
  },
]

export const LINE_COLORS_E = [
  {
    type: 'scale',
    hex: '#FDC44F',
    id: '1',
    name: 'Swump',
    value: 0,
  },
  {
    type: 'scale',
    hex: '#007C76',
    id: '1',
    name: 'Swump',
    value: 0,
  },
  {
    type: 'scale',
    hex: '#8983FF',
    id: '1',
    name: 'Swump',
    value: 0,
  },
]

export const LINE_COLORS_F = [
  {
    type: 'scale',
    hex: '#DA6FF1',
    id: '1',
    name: 'Splort',
    value: 0,
  },
  {
    type: 'scale',
    hex: '#00717A',
    id: '1',
    name: 'Splort',
    value: 0,
  },
  {
    type: 'scale',
    hex: '#05B7E0',
    id: '1',
    name: 'Splort',
    value: 0,
  },
]

export const LINE_COLORS_G = [
  {
    type: 'scale',
    hex: '#F6F6F8',
    id: '1',
    name: 'OldTimey',
    value: 0,
  },
  {
    type: 'scale',
    hex: '#A4A8B6',
    id: '1',
    name: 'OldTimey',
    value: 0,
  },
  {
    type: 'scale',
    hex: '#545667',
    id: '1',
    name: 'OldTimey',
    value: 0,
  },
]

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
  const colors = colorScale.map(color => color.hex)
  const id = colorScale[0].id

  return {name, colors, id}
})

const paletteA = ['#31C0F6', '#A500A5', '#FF7E27']
const paletteB = ['#74D495', '#3F3FBA', '#EA5994']
const paletteC = ['#8F8AF4', '#A51414', '#F4CF31']
const paletteD = ['#FD7A5D', '#5F1CF2', '#4CE09A']
const paletteE = ['#FDC44F', '#007C76', '#8983FF']
const paletteF = ['#DA6FF1', '#00717A', '#05B7E0']
const paletteG = ['#F6F6F8', '#A4A8B6', '#545667']

export const generateColorScale = numSeries => {
  return chroma
    .scale(paletteB)
    .mode('lch')
    .colors(numSeries)
}
