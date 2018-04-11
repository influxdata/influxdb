import {NEW_DEFAULT_DASHBOARD_CELL} from 'src/dashboards/constants'

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

export const getNewDashboardCell = dashboard => {
  if (dashboard.cells.length === 0) {
    return NEW_DEFAULT_DASHBOARD_CELL
  }

  const newCellY = dashboard.cells
    .map(cell => cell.y + cell.h)
    .reduce((a, b) => (a > b ? a : b))

  const existingCellWidths = dashboard.cells.map(cell => cell.w)
  const existingCellHeights = dashboard.cells.map(cell => cell.h)

  const mostCommonCellWidth = getMostCommonValue(existingCellWidths)
  const mostCommonCellHeight = getMostCommonValue(existingCellHeights)

  return {
    ...NEW_DEFAULT_DASHBOARD_CELL,
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
