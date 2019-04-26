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
  list: [string, string][]
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
      const mapValues = getArgumentValuesForVariable<'map'>(state, variableID)
      const list = Object.entries(mapValues)
      const selection = list.find(([_, value]) => value === selectedValue)

      return {
        selectedKey: selection[0],
        list,
      }
    }
    default:
      const list = values.map((v): [string, string] => [v, v])
      return {selectedKey: selectedValue, list}
  }
}
