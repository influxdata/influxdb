import {NEW_DEFAULT_DASHBOARD_CELL} from 'src/dashboards/constants'
import {CellType} from 'src/types/dashboard'
import {UNTITLED_GRAPH} from 'src/dashboards/constants'

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

export const isCellUntitled = cellName => {
  return cellName === UNTITLED_GRAPH
}

const numColumns = 12

const getNextAvailablePosition = (dashboard, newCell) => {
  const farthestY = dashboard.cells
    .map(cell => cell.y)
    .reduce((a, b) => (a > b ? a : b))

  const bottomCells = dashboard.cells.filter(cell => cell.y === farthestY)
  const farthestX = bottomCells
    .map(cell => cell.x)
    .reduce((a, b) => (a > b ? a : b))
  const lastCell = bottomCells.find(cell => cell.x === farthestX)

  const availableSpace = numColumns - (lastCell.x + lastCell.w)
  const newCellFits = availableSpace >= newCell.w

  return newCellFits
    ? {
        x: lastCell.x + lastCell.w,
        y: farthestY,
      }
    : {
        x: 0,
        y: lastCell.y + lastCell.h,
      }
}

export const getNewDashboardCell = (dashboard, cellType) => {
  const type = cellType || CellType.Line
  const typedCell = {
    ...NEW_DEFAULT_DASHBOARD_CELL,
    type,
    name: UNTITLED_GRAPH,
  }

  if (dashboard.cells.length === 0) {
    return typedCell
  }

  const existingCellWidths = dashboard.cells.map(cell => cell.w)
  const existingCellHeights = dashboard.cells.map(cell => cell.h)

  const mostCommonCellWidth = getMostCommonValue(existingCellWidths)
  const mostCommonCellHeight = getMostCommonValue(existingCellHeights)

  const newCell = {
    ...typedCell,
    w: mostCommonCellWidth,
    h: mostCommonCellHeight,
  }

  const {x, y} = getNextAvailablePosition(dashboard, newCell)

  return {
    ...newCell,
    x,
    y,
  }
}

export const getClonedDashboardCell = (dashboard, cloneCell) => {
  const {x, y} = getNextAvailablePosition(dashboard, cloneCell)

  const name = `${cloneCell.name} (clone)`

  return {...cloneCell, x, y, name}
}
