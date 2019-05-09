import {get} from 'lodash'

import {AppState, View} from 'src/types'

import {
  getValuesForVariable,
  getTypeForVariable,
  getArgumentValuesForVariable,
} from 'src/variables/selectors'

export const getView = (state: AppState, id: string): View => {
  return get(state, `views.views.${id}.view`)
}

export const getViewsForDashboard = (
  state: AppState,
  dashboardID: string
): View[] => {
  const dashboard = state.dashboards.list.find(
    dashboard => dashboard.id === dashboardID
  )

  const cellIDs = new Set(dashboard.cells.map(cell => cell.id))

  const views = Object.values(state.views.views)
    .map(d => d.view)
    .filter(view => cellIDs.has(view.cellID))

  return views
}

interface DropdownValues {
  list: {name: string; value: string}[]
  selectedKey: string
}

export const getVariableValuesForDropdown = (
  state: AppState,
  variableID: string,
  contextID: string
): DropdownValues => {
  const {selectedValue, values} = getValuesForVariable(
    state,
    variableID,
    contextID
  )
  const type = getTypeForVariable(state, variableID)

  switch (type) {
    case 'map': {
      const mapValues = getArgumentValuesForVariable(state, variableID) as {
        [key: string]: string
      }
      const list = Object.entries(mapValues).map(([name, value]) => ({
        name,
        value,
      }))
      const selection = list.find(({value}) => value === selectedValue)

      return {
        selectedKey: get(selection, 'name', ''),
        list,
      }
    }
    default:
      const list = values.map(v => ({name: v, value: v}))
      return {selectedKey: selectedValue, list}
  }
}
