import {Cell, Dashboard, View, ViewShape} from 'src/types/v2/dashboards'
import {UNTITLED_GRAPH} from 'src/dashboards/constants'

const getMostCommonValue = (values: number[]): number => {
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
    {distribution: {}, mostCommonCount: 0, mostCommonValue: null}
  )

  return results.mostCommonValue
}

export const isCellUntitled = (cellName: string): boolean => {
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

export const getNewDashboardCell = (dashboard: Dashboard): Cell => {
  const defaultCell = {
    x: 0,
    y: 0,
    h: 4,
    w: 4,
    id: '',
    viewID: '',
    links: {
      self: '',
      view: '',
      copy: '',
    },
  }

  if (dashboard.cells.length === 0) {
    return defaultCell
  }

  const existingCellWidths = dashboard.cells.map(cell => cell.w)
  const existingCellHeights = dashboard.cells.map(cell => cell.h)

  const mostCommonCellWidth = getMostCommonValue(existingCellWidths)
  const mostCommonCellHeight = getMostCommonValue(existingCellHeights)

  const newCell = {
    ...defaultCell,
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

export const getClonedDashboardCell = (
  dashboard: Dashboard,
  cloneCell: Cell
): Cell => {
  const {x, y} = getNextAvailablePosition(dashboard, cloneCell)

  return {...cloneCell, x, y}
}

export const getNewView = (): View => {
  const newView: View = {
    id: '',
    name: 'Untitled',
    properties: {
      type: ViewShape.Empty,
      shape: ViewShape.Empty,
    },
  }

  return newView
}
