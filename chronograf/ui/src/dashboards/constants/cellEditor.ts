import {DEFAULT_TABLE_OPTIONS} from 'src/dashboards/constants'
import {stringifyColorValues} from 'src/shared/constants/colorOperations'
import {CellType, Axis} from 'src/types/dashboards'
import {ColorString, ColorNumber} from 'src/types/colors'

export const initializeOptions = (cellType: CellType) => {
  switch (cellType) {
    case 'table':
      return DEFAULT_TABLE_OPTIONS
    default:
      return DEFAULT_TABLE_OPTIONS
  }
}

export const AXES_SCALE_OPTIONS = {
  LINEAR: 'linear',
  LOG: 'log',
  BASE_2: '2',
  BASE_10: '10',
}

type DefaultAxis = Pick<Axis, Exclude<keyof Axis, 'bounds'>>

export const DEFAULT_AXIS: DefaultAxis = {
  prefix: '',
  suffix: '',
  base: AXES_SCALE_OPTIONS.BASE_10,
  scale: AXES_SCALE_OPTIONS.LINEAR,
  label: '',
}

export const TOOLTIP_Y_VALUE_FORMAT =
  '<p><strong>K/M/B</strong> = Thousand / Million / Billion<br/><strong>K/M/G</strong> = Kilo / Mega / Giga </p>'

interface Color {
  cellType: CellType
  thresholdsListColors: ColorNumber[]
  gaugeColors: ColorNumber[]
  lineColors: ColorString[]
}

export const getCellTypeColors = ({
  cellType,
  gaugeColors,
  thresholdsListColors,
  lineColors,
}: Color): ColorString[] => {
  let colors: ColorString[] = []

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
