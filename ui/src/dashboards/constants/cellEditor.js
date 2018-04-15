import {DEFAULT_TABLE_OPTIONS} from 'src/shared/constants/tableGraph'
import {stringifyColorValues} from 'src/shared/constants/colorOperations'

export const initializeOptions = cellType => {
  switch (cellType) {
    case 'table':
      return DEFAULT_TABLE_OPTIONS
    default:
      return {}
  }
}

export const AXES_SCALE_OPTIONS = {
  LINEAR: 'linear',
  LOG: 'log',
  BASE_2: '2',
  BASE_10: '10',
}

export const TOOLTIP_Y_VALUE_FORMAT =
  '<p><strong>K/M/B</strong> = Thousand / Million / Billion<br/><strong>K/M/G</strong> = Kilo / Mega / Giga </p>'

export const getCellTypeColors = ({
  cellType,
  gaugeColors,
  thresholdsListColors,
  lineColors,
}) => {
  let colors = []

  switch (cellType) {
    case 'gauge': {
      colors = stringifyColorValues(gaugeColors)
      break
    }
    case 'single-stat':
    case 'table': {
      colors = stringifyColorValues(thresholdsListColors)
      break
    }
    case 'bar':
    case 'line':
    case 'line-plus-single-stat':
    case 'line-stacked':
    case 'line-stepplot': {
      colors = stringifyColorValues(lineColors)
    }
  }

  return colors
}
