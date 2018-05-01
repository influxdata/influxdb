import {DEFAULT_TABLE_OPTIONS} from 'src/dashboards/constants'
import {stringifyColorValues} from 'src/shared/constants/colorOperations'
import {
  CELL_TYPE_LINE,
  CELL_TYPE_STACKED,
  CELL_TYPE_STEPPLOT,
  CELL_TYPE_BAR,
  CELL_TYPE_LINE_PLUS_SINGLE_STAT,
  CELL_TYPE_SINGLE_STAT,
  CELL_TYPE_GAUGE,
  CELL_TYPE_TABLE,
} from 'src/dashboards/graphics/graph'

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
    case CELL_TYPE_GAUGE: {
      colors = stringifyColorValues(gaugeColors)
      break
    }
    case CELL_TYPE_SINGLE_STAT:
    case CELL_TYPE_TABLE: {
      colors = stringifyColorValues(thresholdsListColors)
      break
    }
    case CELL_TYPE_BAR:
    case CELL_TYPE_LINE:
    case CELL_TYPE_LINE_PLUS_SINGLE_STAT:
    case CELL_TYPE_STACKED:
    case CELL_TYPE_STEPPLOT: {
      colors = stringifyColorValues(lineColors)
    }
  }

  return colors
}
