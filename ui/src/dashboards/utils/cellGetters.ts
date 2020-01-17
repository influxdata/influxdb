// Libraries
import uuid from 'uuid'

// Types
import {NewCell, Cell, Dashboard, AppState, RemoteDataState} from 'src/types'

// Constants
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

export const getNewDashboardCell = (
  state: AppState,
  dashboard: Dashboard,
  clonedCell?: Cell
): NewCell => {
  const defaultCell = {
    id: uuid.v4(),
    x: 0,
    y: 0,
    h: 4,
    w: 4,
    links: {
      self: '',
      view: '',
      copy: '',
    },
    status: RemoteDataState.Done,
  }

  const cells = dashboard.cells.map(
    cellID => state.resources.cells.byID[cellID]
  )

  if (!cells.length) {
    return defaultCell
  }

  const existingCellWidths = cells.map(cell => cell.w)
  const existingCellHeights = cells.map(cell => cell.h)

  const mostCommonCellWidth = getMostCommonValue(existingCellWidths)
  const mostCommonCellHeight = getMostCommonValue(existingCellHeights)

  let newCell = {
    ...defaultCell,
    w: mostCommonCellWidth,
    h: mostCommonCellHeight,
  }

  if (clonedCell) {
    newCell = {
      ...defaultCell,
      w: clonedCell.w,
      h: clonedCell.h,
    }
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
