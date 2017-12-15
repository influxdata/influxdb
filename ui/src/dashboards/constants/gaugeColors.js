import _ from 'lodash'

export const MAX_THRESHOLDS = 5
export const MIN_THRESHOLDS = 2

export const COLOR_TYPE_MIN = 'min'
export const DEFAULT_VALUE_MIN = '0'
export const COLOR_TYPE_MAX = 'max'
export const DEFAULT_VALUE_MAX = '100'
export const COLOR_TYPE_THRESHOLD = 'threshold'

export const SINGLE_STAT_TEXT = 'text'
export const SINGLE_STAT_BG = 'background'

export const GAUGE_COLORS = [
  {
    hex: '#BF3D5E',
    name: 'ruby',
  },
  {
    hex: '#DC4E58',
    name: 'fire',
  },
  {
    hex: '#F95F53',
    name: 'curacao',
  },
  {
    hex: '#F48D38',
    name: 'tiger',
  },
  {
    hex: '#FFB94A',
    name: 'pineapple',
  },
  {
    hex: '#FFD255',
    name: 'thunder',
  },
  {
    hex: '#7CE490',
    name: 'honeydew',
  },
  {
    hex: '#4ED8A0',
    name: 'rainforest',
  },
  {
    hex: '#32B08C',
    name: 'viridian',
  },
  {
    hex: '#4591ED',
    name: 'ocean',
  },
  {
    hex: '#22ADF6',
    name: 'pool',
  },
  {
    hex: '#00C9FF',
    name: 'laser',
  },
  {
    hex: '#513CC6',
    name: 'planet',
  },
  {
    hex: '#7A65F2',
    name: 'star',
  },
  {
    hex: '#9394FF',
    name: 'comet',
  },
  {
    hex: '#383846',
    name: 'pepper',
  },
  {
    hex: '#545667',
    name: 'graphite',
  },
]

export const DEFAULT_COLORS = [
  {
    type: COLOR_TYPE_MIN,
    hex: GAUGE_COLORS[11].hex,
    id: '0',
    name: GAUGE_COLORS[11].name,
    value: DEFAULT_VALUE_MIN,
  },
  {
    type: COLOR_TYPE_MAX,
    hex: GAUGE_COLORS[14].hex,
    id: '1',
    name: GAUGE_COLORS[14].name,
    value: DEFAULT_VALUE_MAX,
  },
]

export const validateColors = (colors, type, colorSingleStatText) => {
  if (type === 'single-stat') {
    // Single stat colors should all have type of 'text' or 'background'
    const colorType = colorSingleStatText ? SINGLE_STAT_TEXT : SINGLE_STAT_BG
    return colors ? colors.map(color => ({...color, type: colorType})) : null
  }
  if (!colors) {
    return DEFAULT_COLORS
  }
  if (type === 'gauge') {
    // Gauge colors should have a type of min, any number of thresholds, and a max
    const formatttedColors = _.sortBy(colors, color =>
      Number(color.value)
    ).map(c => ({
      ...c,
      type: COLOR_TYPE_THRESHOLD,
    }))
    formatttedColors[0].type = COLOR_TYPE_MIN
    formatttedColors[formatttedColors.length - 1].type = COLOR_TYPE_MAX
    return formatttedColors
  }

  return colors.length >= MIN_THRESHOLDS ? DEFAULT_COLORS : colors
}
