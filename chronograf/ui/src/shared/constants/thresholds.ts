import _ from 'lodash'

export const MAX_THRESHOLDS = 5
export const MIN_THRESHOLDS = 2

export const COLOR_TYPE_MIN = 'min'
export const DEFAULT_VALUE_MIN = '0'
export const COLOR_TYPE_MAX = 'max'
export const DEFAULT_VALUE_MAX = '100'
export const COLOR_TYPE_THRESHOLD = 'threshold'

export const THRESHOLD_TYPE_TEXT = 'text'
export const THRESHOLD_TYPE_BG = 'background'
export const THRESHOLD_TYPE_BASE = 'base'

export const THRESHOLD_COLORS = [
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
  {
    hex: '#ffffff',
    name: 'white',
  },
  {
    hex: '#292933',
    name: 'castle',
  },
]

export const DEFAULT_GAUGE_COLORS = [
  {
    type: COLOR_TYPE_MIN,
    hex: THRESHOLD_COLORS[11].hex,
    id: '0',
    name: THRESHOLD_COLORS[11].name,
    value: DEFAULT_VALUE_MIN,
  },
  {
    type: COLOR_TYPE_MAX,
    hex: THRESHOLD_COLORS[14].hex,
    id: '1',
    name: THRESHOLD_COLORS[14].name,
    value: DEFAULT_VALUE_MAX,
  },
]

export const DEFAULT_THRESHOLDS_LIST_COLORS = [
  {
    type: THRESHOLD_TYPE_TEXT,
    hex: THRESHOLD_COLORS[11].hex,
    id: THRESHOLD_TYPE_BASE,
    name: THRESHOLD_COLORS[11].name,
    value: '-999999999999999999',
  },
]

export const validateThresholdsListColors = (colors, type) => {
  if (!colors || colors.length === 0) {
    return DEFAULT_THRESHOLDS_LIST_COLORS
  }

  const colorsAreIncorrectType =
    colors.reduce((acc, color) => {
      if (
        color.type === THRESHOLD_TYPE_BASE ||
        color.type === THRESHOLD_TYPE_TEXT ||
        color.type === THRESHOLD_TYPE_BG
      ) {
        return (acc += 1)
      }
    }, 0) !== colors.length

  if (colorsAreIncorrectType) {
    return DEFAULT_THRESHOLDS_LIST_COLORS
  }

  let containsBaseColor = false

  const formattedColors = colors.map(color => {
    if (color.id === THRESHOLD_TYPE_BASE) {
      // Check for existance of base color
      containsBaseColor = true
      return {...color, value: Number(color.value), type}
    }
    // Single stat colors should all have type of 'text' or 'background'
    return {...color, value: Number(color.value), type}
  })

  const formattedColorsWithBase = [
    ...formattedColors,
    DEFAULT_THRESHOLDS_LIST_COLORS[0],
  ]

  return containsBaseColor ? formattedColors : formattedColorsWithBase
}

export const getThresholdsListType = colors => {
  const type = _.get(colors, ['0', 'type'], false)

  if (type) {
    if (_.includes([THRESHOLD_TYPE_TEXT, THRESHOLD_TYPE_BG], type)) {
      return type
    }
  }

  return THRESHOLD_TYPE_TEXT
}

export const validateGaugeColors = colors => {
  if (!colors || colors.length < MIN_THRESHOLDS) {
    return DEFAULT_GAUGE_COLORS
  }

  const colorsAreIncorrectType =
    colors.reduce((acc, color) => {
      if (
        color.type === COLOR_TYPE_MIN ||
        color.type === COLOR_TYPE_MAX ||
        color.type === COLOR_TYPE_THRESHOLD
      ) {
        return (acc += 1)
      }
    }, 0) !== colors.length

  if (colorsAreIncorrectType) {
    return DEFAULT_GAUGE_COLORS
  }

  // Gauge colors should have a type of min, any number of thresholds, and a max
  const formattedColors = _.sortBy(colors, color => Number(color.value)).map(
    color => ({
      ...color,
      value: Number(color.value),
      type: COLOR_TYPE_THRESHOLD,
    })
  )

  formattedColors[0].type = COLOR_TYPE_MIN
  formattedColors[formattedColors.length - 1].type = COLOR_TYPE_MAX

  return formattedColors
}

export const stringifyColorValues = colors => {
  return colors.map(color => ({...color, value: `${color.value}`}))
}
