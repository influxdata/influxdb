import {get} from 'lodash'

import {
  AppState,
  View,
  Check,
  ViewType,
  RemoteDataState,
  TimeRange,
} from 'src/types'

import {
  getValuesForVariable,
  getTypeForVariable,
  getArgumentValuesForVariable,
} from 'src/variables/selectors'

// Constants
import {DEFAULT_TIME_RANGE} from 'src/shared/constants/timeRanges'

export const getView = (state: AppState, id: string): View => {
  return get(state, `views.views.${id}.view`)
}

export const getViewStatus = (state: AppState, id: string): RemoteDataState => {
  return get(state, `views.views.${id}.status`, RemoteDataState.Loading)
}

export const getTimeRangeByDashboardID = (
  state: AppState,
  dashboardID: string
): TimeRange => state.ranges[dashboardID] || DEFAULT_TIME_RANGE

export const getCheckForView = (
  state: AppState,
  view: View
): Partial<Check> => {
  const viewType: ViewType = get(view, 'properties.type')
  const checkID = get(view, 'properties.checkID')

  return viewType === 'check'
    ? state.checks.list.find(c => c.id === checkID)
    : null
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
    .filter(view => view && cellIDs.has(view.cellID))

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

  if (!values) {
    return {list: null, selectedKey: null}
  }

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

      return {
        selectedKey: selectedValue,
        list,
      }
    }
    default:
      const list = values.map(v => ({name: v, value: v}))

      return {selectedKey: selectedValue, list}
  }
}
