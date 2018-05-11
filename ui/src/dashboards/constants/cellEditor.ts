import {DEFAULT_TABLE_OPTIONS} from 'src/dashboards/constants'
import {stringifyColorValues} from 'src/shared/constants/colorOperations'
import {CellType} from 'src/dashboards/graphics/graph'

export const initializeOptions = (cellType: CellType) => {
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
}: {
  cellType: CellType
  gaugeColors
  thresholdsListColors
  lineColors
}) => {
  let colors = []

  switch (cellType) {
    case CellType.Gauge: {
      colors = stringifyColorValues(gaugeColors)
      break
    }
    case CellType.SingleStat:
    case CellType.Table: {
      colors = stringifyColorValues(thresholdsListColors)
      break
    }
    case CellType.Bar:
    case CellType.Line:
    case CellType.LinePlusSingleStat:
    case CellType.Stacked:
    case CellType.StepPlot: {
      colors = stringifyColorValues(lineColors)
    }
  }

  return colors
}
