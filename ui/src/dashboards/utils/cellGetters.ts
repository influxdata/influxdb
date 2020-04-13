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
      x: clonedCell.x,
      y: clonedCell.y,
    }
  }

  return newCell
}
