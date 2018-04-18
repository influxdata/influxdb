import {NEW_DEFAULT_DASHBOARD_CELL} from 'src/dashboards/constants'
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

const getMostCommonValue = values => {
  const results = values.reduce(
    (acc, value) => {
      const {distribution, mostCommonCount} = acc
      distribution[value] = (distribution[value] || 0) + 1
      if (distribution[value] > mostCommonCount) {
        return {
          distribution,
          mostCommonCount: distribution[value],
          mostCommonValue: value,
        }
      }
      return acc
    },
    {distribution: {}, mostCommonCount: 0}
  )

  return results.mostCommonValue
}

export const UNTITLED_CELL_LINE = 'Untitled Line Graph'
export const UNTITLED_CELL_STACKED = 'Untitled Stacked Graph'
export const UNTITLED_CELL_STEPPLOT = 'Untitled Step-Plot Graph'
export const UNTITLED_CELL_BAR = 'Untitled Bar Graph'
export const UNTITLED_CELL_LINE_PLUS_SINGLE_STAT =
  'Untitled Line Graph + Single Stat'
export const UNTITLED_CELL_SINGLE_STAT = 'Untitled Single Stat'
export const UNTITLED_CELL_GAUGE = 'Untitled Gauge'
export const UNTITLED_CELL_TABLE = 'Untitled Table'

const getNewTypedCellName = type => {
  switch (type) {
    case CELL_TYPE_LINE:
      return UNTITLED_CELL_LINE
    case CELL_TYPE_STACKED:
      return UNTITLED_CELL_STACKED
    case CELL_TYPE_STEPPLOT:
      return UNTITLED_CELL_STEPPLOT
    case CELL_TYPE_BAR:
      return UNTITLED_CELL_BAR
    case CELL_TYPE_LINE_PLUS_SINGLE_STAT:
      return UNTITLED_CELL_LINE_PLUS_SINGLE_STAT
    case CELL_TYPE_SINGLE_STAT:
      return UNTITLED_CELL_SINGLE_STAT
    case CELL_TYPE_GAUGE:
      return UNTITLED_CELL_GAUGE
    case CELL_TYPE_TABLE:
      return UNTITLED_CELL_TABLE
  }
}

export const isCellUntitled = cellName => {
  return (
    cellName === UNTITLED_CELL_LINE ||
    cellName === UNTITLED_CELL_STACKED ||
    cellName === UNTITLED_CELL_STEPPLOT ||
    cellName === UNTITLED_CELL_BAR ||
    cellName === UNTITLED_CELL_LINE_PLUS_SINGLE_STAT ||
    cellName === UNTITLED_CELL_SINGLE_STAT ||
    cellName === UNTITLED_CELL_GAUGE ||
    cellName === UNTITLED_CELL_TABLE
  )
}

export const getNewDashboardCell = (dashboard, cellType) => {
  const type = cellType || CELL_TYPE_LINE
  const typedCell = {
    ...NEW_DEFAULT_DASHBOARD_CELL,
    type,
    name: getNewTypedCellName(type),
  }

  if (dashboard.cells.length === 0) {
    return typedCell
  }

  const newCellY = dashboard.cells
    .map(cell => cell.y + cell.h)
    .reduce((a, b) => (a > b ? a : b))

  const existingCellWidths = dashboard.cells.map(cell => cell.w)
  const existingCellHeights = dashboard.cells.map(cell => cell.h)

  const mostCommonCellWidth = getMostCommonValue(existingCellWidths)
  const mostCommonCellHeight = getMostCommonValue(existingCellHeights)

  return {
    ...typedCell,
    y: newCellY,
    w: mostCommonCellWidth,
    h: mostCommonCellHeight,
  }
}

export const getClonedDashboardCell = (dashboard, cloneCell) => {
  const newCellY = dashboard.cells
    .map(cell => cell.y + cell.h)
    .reduce((a, b) => (a > b ? a : b))

  const name = `${cloneCell.name} (Clone)`

  return {...cloneCell, y: newCellY, name}
}
