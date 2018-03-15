export const DEFAULT_TABLE_OPTIONS = {
  timeFormat: 'MM/DD/YYYY HH:mm:ss.ss',
  verticalTimeAxis: true,
  sortBy: {internalName: 'time', displayName: ''},
  wrapping: 'single-line',
  columnNames: [{internalName: 'time', displayName: ''}],
}

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
